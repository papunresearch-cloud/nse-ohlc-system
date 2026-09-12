"""
MASTER ORCHESTRATOR
- Dedicated, unkillable background heartbeat daemon (every 300s).
- Event-triggered immediate pulse on HTTP /health, /start, /stop, and /sync.
- SR Flip-Flop Power Latch (Default: ON).
- Non-blocking weekend/holiday sleep (5-second slice loop).
- Watchlist is absolute master; guarded OHLC protection.
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Index 0 live sanitization: Clears index 0 at 09:00 IST and 16:00 IST.
- Per-script live quarantine: Unsynced stocks are isolated from Child-2.
- Parameter Engine: Computes indicators every 15 minutes during LIVE sessions.
"""
import os
import sys
import time
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, date
import pytz

from config import (
    TIMEZONE,
    LIVE_UPDATE_INTERVAL_SEC,
    SYNC_RETRY_INTERVAL_SEC,
    SYNC_WINDOW_START_HOUR,
    SYNC_WINDOW_START_MIN,
    SYNC_WINDOW_DEADLINE_HOUR,
    SYNC_WINDOW_DEADLINE_MIN,
    PULSE_VALIDITY_SEC,
    TARGET_OHLC_COUNT,
    HISTORICAL_START_INDEX,
    logger
)

try:
    from config import PARAM_UPDATE_INTERVAL_SEC
except ImportError:
    PARAM_UPDATE_INTERVAL_SEC = 900  # 15 minutes default

from firebase_admin import db
from firebase_manager import (
    init_firebase,
    reconcile_stocklist_with_watchlist,
    get_stock_ohlc,
    clear_live_candle
)
from market_calendar import MarketCalendar
from sync_child import sync_historical_script
from live_child import update_live_script
from yahoo_manager import get_latest_available_trading_date
from parameter import update_all_parameters

IST = pytz.timezone(TIMEZONE)
_keep_running = True


# =====================================================================
# HARDWARE-STYLE FLIP-FLOP & PULSE BUS
# =====================================================================
class SystemStateBus:
    def __init__(self):
        self.lock = threading.Lock()
        self.power_latched_on = True  # Default: ON
        self.pulse_sync_time = 0.0
        self.heartbeat_trigger_event = threading.Event()

    def trigger_pulse(self, command: str):
        now = time.time()
        with self.lock:
            if command == "START":
                self.power_latched_on = True
                logger.info("[SIGNAL] START pulse captured -> Flip-flop latched ON.")
            elif command == "STOP":
                self.power_latched_on = False
                logger.warning("[SIGNAL] STOP pulse captured -> Flip-flop latched OFF. Core loop idle.")
            elif command == "SYNC":
                self.pulse_sync_time = now
                logger.info("[SIGNAL] MANUAL SYNC pulse captured -> Immediate sync scheduled.")
        
        # Signal heartbeat daemon to publish telemetry state immediately
        self.heartbeat_trigger_event.set()

    def check_and_clear_manual_sync(self) -> bool:
        now = time.time()
        with self.lock:
            if (now - self.pulse_sync_time) <= PULSE_VALIDITY_SEC:
                self.pulse_sync_time = 0.0
                return True
        return False

    def is_power_on(self) -> bool:
        with self.lock:
            return self.power_latched_on


STATE_BUS = SystemStateBus()


# =====================================================================
# HTTP PULSE RECEIVER & HEALTH SERVER (CRON-JOB / RENDER COMPATIBLE)
# =====================================================================
class PulseCommandServer(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = self.path.lower().strip()
        state_str = "ON" if STATE_BUS.is_power_on() else "OFF"

        if path in ("/health", "/ping"):
            # Trigger immediate heartbeat update on external wake-up ping
            STATE_BUS.heartbeat_trigger_event.set()
            self._send_resp(200, f"OK - STATUS:{state_str}\n")
        elif path in ("/start", "/api/start"):
            STATE_BUS.trigger_pulse("START")
            self._send_resp(200, "START latched ON.\n")
        elif path in ("/stop", "/api/stop"):
            STATE_BUS.trigger_pulse("STOP")
            self._send_resp(200, "STOP latched OFF.\n")
        elif path in ("/sync", "/api/sync"):
            STATE_BUS.trigger_pulse("SYNC")
            self._send_resp(200, "MANUAL SYNC triggered.\n")
        else:
            self._send_resp(200, f"STATUS:{state_str}\n")

    def _send_resp(self, code: int, message: str):
        payload = message.encode("utf-8")
        self.send_response(code)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


def start_http_listener():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), PulseCommandServer)
    logger.info(f"[HTTP] Command server listening on 0.0.0.0:{port}")
    threading.Thread(target=server.serve_forever, daemon=True).start()


# =====================================================================
# SIGNAL HANDLING
# =====================================================================
def handle_shutdown(signum, frame):
    global _keep_running
    logger.info(f"Signal ({signum}) caught. Terminating Master gracefully...")
    _keep_running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# =====================================================================
# MASTER ORCHESTRATOR
# =====================================================================
class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.calendar = MarketCalendar()

        self.last_planned_date = None
        self.is_today_trading_day = False
        self.sync_audit_reported_today = False
        self.preopen_cleared_today = False
        self.postclose_cleared_today = False

        self.last_sync_attempt_time = 0.0
        self.last_live_update_time = 0.0
        self.last_param_calc_time = 0.0
        self.last_heartbeat_time = 0.0

        self.sync_lock = threading.Lock()
        self.script_status = {}

    def _get_reconciled_stock_map(self) -> dict:
        """Unpacks reconcile_stocklist_with_watchlist safely whether it returns dict or tuple."""
        result = reconcile_stocklist_with_watchlist()
        if isinstance(result, tuple):
            return result[0] if len(result) > 0 and isinstance(result[0], dict) else (result[1] if len(result) > 1 and isinstance(result[1], dict) else {})
        elif isinstance(result, dict):
            return result
        return {}

    # =================================================================
    # DEDICATED INDEPENDENT HEARTBEAT DAEMON
    # =================================================================
    def _heartbeat_daemon(self):
        """Continuously broadcasts heartbeat telemetry every 300s or on signal events."""
        logger.info("[HEARTBEAT-DAEMON] Dedicated thread initiated. Interval: 300s.")
        
        while _keep_running:
            try:
                now = time.time()
                is_triggered = STATE_BUS.heartbeat_trigger_event.is_set()
                
                # Fire if 300 seconds elapsed OR if an immediate event was captured
                if (now - self.last_heartbeat_time >= 300) or is_triggered:
                    if is_triggered:
                        STATE_BUS.heartbeat_trigger_event.clear()

                    now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
                    power_status = "RUNNING" if STATE_BUS.is_power_on() else "STOPPED"

                    payload = {
                        "heartbeat_epoch": now,
                        "last_heartbeat": now_ist_str,
                        "backend_power": power_status
                    }

                    db.reference("system_status").update(payload)
                    self.last_heartbeat_time = now
                    logger.info(f"[HEARTBEAT] Telemetry published -> {now_ist_str} (Power: {power_status})")

            except Exception as e:
                logger.error(f"[HEARTBEAT] Telemetry write failed: {e}")

            # Sleep in short 1-second chunks so triggers respond instantly
            time.sleep(1)

    # =================================================================
    # CORE DISPATCHER LOOP
    # =================================================================
    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE")
        logger.info("==================================================")

        # 1. Start dedicated heartbeat background daemon thread
        threading.Thread(target=self._heartbeat_daemon, daemon=True).start()

        # 2. Plan initial routine
        self.replan_daily_routine()

        # 3. Non-blocking initial sync in background thread
        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True).start()

        # 4. Main Event & Scheduling Loop
        while _keep_running:
            try:
                now_epoch = time.time()
                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # Power latch check (flip-flop state)
                if not STATE_BUS.is_power_on():
                    time.sleep(5)
                    continue

                # Priority 0: Manual External Sync Override
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[OVERRIDE PULSE] Immediate sync commanded. Running CHILD-1...")
                    self.execute_historical_sync(is_manual=True)
                    continue

                # Priority 1: State-Driven Date Catch-Up (Midnight rollover)
                if self.last_planned_date != today_date:
                    self.replan_daily_routine()

                # Non-trading days (Weekends / NSE Holidays): Sleep in short 5s intervals
                if not self.is_today_trading_day:
                    time.sleep(5)
                    continue

                # Pre-Market Index 0 Sanitation (09:00 AM IST)
                if now_time.hour == 9 and now_time.minute >= 0 and not self.preopen_cleared_today:
                    self.sanitize_all_indices_zero("Pre-Market (09:00 AM)")
                    self.preopen_cleared_today = True

                # Post-Market Index 0 Sanitation (16:00 PM IST)
                if now_time.hour >= 16 and not self.postclose_cleared_today:
                    self.sanitize_all_indices_zero("Post-Market (16:00 PM)")
                    self.postclose_cleared_today = True

                # Priority 2: Pre-Market Historical Sync Window (08:00 – 08:30 IST)
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s["synced"] for s in self.script_status.values())
                    if has_unsynced and (now_epoch - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync window active. Retrying unsynced scripts...")
                        self.execute_historical_sync(is_manual=False)
                        self.last_sync_attempt_time = now_epoch

                # Audit Report Check at or after 08:30 IST
                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # Priority 3: Live Market Window (09:15 – 15:30 IST)
                self.calendar.refresh_calendar()
                status, _ = self.calendar.get_market_status()

                if status == "LIVE":
                    # 5-minute live update pass (Index 0)
                    if (now_epoch - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = now_epoch

                    # 15-minute parameter calculation pass
                    if (now_epoch - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        active_synced_scripts = [name for name, meta in self.script_status.items() if meta["synced"]]
                        if active_synced_scripts:
                            update_all_parameters(active_synced_scripts)
                        self.last_param_calc_time = now_epoch

                time.sleep(5)

            except Exception as e:
                logger.critical(f"Unhandled exception in master loop: {e}", exc_info=True)
                time.sleep(5)

        logger.info("Master orchestrator stopped cleanly.")

    def replan_daily_routine(self):
        """Generates today's schedule and preserves active stocks."""
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today
        self.sync_audit_reported_today = False
        self.preopen_cleared_today = False
        self.postclose_cleared_today = False

        stock_map = self._get_reconciled_stock_map()
        new_status = {}
        for name, ticker in stock_map.items():
            if name in self.script_status:
                new_status[name] = self.script_status[name]
                new_status[name]["ticker"] = ticker
            else:
                new_status[name] = {
                    "synced": False,
                    "last_attempt_at": None,
                    "error": "Awaiting initial sync",
                    "ticker": ticker
                }
        self.script_status = new_status
        status_label = "TRADING SESSION" if self.is_today_trading_day else "NON-TRADING DAY (Closed)"
        logger.info(f"[PLANNER] Day plan for {today} IST refreshed: {status_label} ({len(self.script_status)} stocks)")

    def execute_historical_sync(self, is_manual: bool = False):
        """Runs CHILD-1 historical sync under concurrency lock."""
        if not self.sync_lock.acquire(blocking=False):
            logger.info("[CHILD-1] Sync already in progress. Skipping duplicate pass.")
            return

        try:
            # Broadcast SYNCING state to Firebase
            try:
                db.reference("system_status/sync_data").update({"status": "SYNCING"})
            except Exception:
                pass

            now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

            for name, meta in list(self.script_status.items()):
                if not _keep_running:
                    break
                if not STATE_BUS.is_power_on() and not is_manual:
                    break

                if meta["synced"] and not is_manual:
                    continue

                meta["last_attempt_at"] = now_str
                ticker = meta["ticker"]

                try:
                    gap = self._calculate_script_gap(name, ticker)
                    if gap == 0:
                        meta["synced"] = True
                        meta["error"] = None
                        logger.info(f"[{name}] Index 1 matches latest exchange session. Synced.")
                        continue

                    success, msg = sync_historical_script(name, ticker, gap_trading_days=gap, calendar=self.calendar)
                    if success:
                        meta["synced"] = True
                        meta["error"] = None
                        logger.info(f"[{name}] Sync successful: {msg}")
                    else:
                        meta["synced"] = False
                        meta["error"] = msg
                        logger.warning(f"[{name}] Sync incomplete: {msg}")

                except Exception as e:
                    meta["synced"] = False
                    meta["error"] = f"Exception: {str(e)}"
                    logger.error(f"Sync fault caught for [{name}]: {e}", exc_info=True)

            # Record final sync summary to Firebase
            synced_count = len([k for k, v in self.script_status.items() if v["synced"]])
            total_count = len(self.script_status)
            try:
                db.reference("system_status/sync_data").update({
                    "last_sync_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
                    "total_registered": total_count,
                    "synced_count": synced_count,
                    "status": "VERIFIED" if (total_count > 0 and synced_count == total_count) else "IDLE"
                })
            except Exception:
                pass

        finally:
            self.sync_lock.release()

    def _calculate_script_gap(self, display_name: str, ticker: str) -> int:
        existing_ohlc = get_stock_ohlc(display_name)
        if not existing_ohlc or not isinstance(existing_ohlc, dict):
            return TARGET_OHLC_COUNT

        idx1 = existing_ohlc.get("1")
        if not idx1 or "date" not in idx1:
            return TARGET_OHLC_COUNT

        try:
            latest_fb_date = datetime.strptime(str(idx1["date"]), "%Y-%m-%d").date()
        except ValueError:
            return TARGET_OHLC_COUNT

        latest_yahoo_date = get_latest_available_trading_date(ticker)
        if not latest_yahoo_date:
            return 0

        return self.calendar.get_trading_day_gap(latest_fb_date, latest_yahoo_date)

    def execute_live_updates(self):
        """Runs CHILD-2 live updates on Index 0."""
        logger.info("[CHILD-2] Starting 5-minute live update cycle...")
        updated_count = 0

        for name, meta in self.script_status.items():
            if not _keep_running or not STATE_BUS.is_power_on():
                break

            if not meta["synced"]:
                logger.warning(f"[{name}] EXCLUDED FROM LIVE UPDATE | Reason: {meta['error']}")
                continue

            try:
                update_live_script(name, meta["ticker"])
                updated_count += 1
            except Exception as e:
                logger.error(f"[{name}] Live update failed: {e}", exc_info=True)

        try:
            db.reference("system_status/live_data").update({
                "last_update_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
                "tickers_updated": updated_count,
                "status": "STREAMING"
            })
        except Exception:
            pass

    def sanitize_all_indices_zero(self, label: str):
        """Clears Index 0 across all stocks to prevent stale quotes."""
        logger.info(f"[MAINTENANCE] Clearing Index 0 for all stocks ({label})...")
        for name in self.script_status.keys():
            clear_live_candle(name)

    def log_detailed_sync_audit(self):
        synced = [k for k, v in self.script_status.items() if v["synced"]]
        unsynced = [k for k, v in self.script_status.items() if not v["synced"]]

        logger.info("=" * 85)
        logger.info("                   SCRIPT-WISE SYNCHRONIZATION AUDIT REPORT")
        logger.info("=" * 85)
        logger.info(f"Total: {len(self.script_status)} | Synced: {len(synced)} | Failed/Unsynced: {len(unsynced)}")

        if synced:
            logger.info("[ACTIVE / SYNCHRONIZED SCRIPTS]")
            for s in synced:
                logger.info(f"  ✓ {s:<24} | Synced At: {self.script_status[s]['last_attempt_at']}")

        if unsynced:
            logger.error("[ISOLATED / UNSYNCHRONIZED SCRIPTS - DATABASE SYNCH ERROR]")
            for u in unsynced:
                info = self.script_status[u]
                logger.error(f"  ✗ {u:<24} | Last Attempt: {info['last_attempt_at']} | Error: {info['error']}")
        else:
            logger.info("All registered stocks successfully synchronized. Live updater ready.")
        logger.info("=" * 85)


# =====================================================================
# PROGRAM ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    start_http_listener()
    orchestrator = MasterOrchestrator()
    orchestrator.run()