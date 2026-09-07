"""
MASTER ORCHESTRATOR
- Priority 0: Watchlist is absolute master. Stocklist is continuously synchronized.
- Never mutates or deletes from /watchlist or /watchlist/detailedDb.
- Guarded OHLC protection: Stocks currently present in /watchlist are never purged.
- SR Flip-Flop Power Latch (Default: ON).
- Handles external 30-second pulse commands: /start, /stop, /sync.
- Embedded HTTP Server with minimal /health, GET, and HEAD handling for cron-job.org.
- State-driven date planning (auto-adjusts if restarted or offline at midnight).
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Index 0 live sanitization at 09:00 IST and 16:00 IST.
- Per-script live quarantine: Unsynced stocks are isolated by Child-2.
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
    HEARTBEAT_TICK_SEC,
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

    def check_and_clear_manual_sync(self) -> bool:
        now = time.time()
        with self.lock:
            if (now - self.pulse_sync_time) <= PULSE_VALIDITY_SEC:
                self.pulse_sync_time = 0.0  # Clear momentary pulse
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
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = self.path.lower().strip()
        if path in ("/health", "/ping"):
            self._send_resp(200, "OK")
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
            state_str = "ON" if STATE_BUS.is_power_on() else "OFF"
            self._send_resp(200, f"State: {state_str}\n")

    def _send_resp(self, code: int, message: str):
        payload = message.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


def start_http_listener():
    port_str = os.environ.get("PORT", "10000")
    try:
        port = int(port_str)
        server = HTTPServer(("0.0.0.0", port), PulseCommandServer)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logger.info(f"Command listener & Render health check bound to 0.0.0.0:{port}")
    except Exception as e:
        logger.error(f"Failed to start pulse listener on port {port_str}: {e}")


# =====================================================================
# SIGNAL HANDLING
# =====================================================================
def handle_shutdown(signum, frame):
    global _keep_running
    logger.info(f"Signal ({signum}) caught. Terminating gracefully...")
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

        self.sync_lock = threading.Lock()
        self.script_status = {}

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE")
        logger.info("==================================================")

        # 1. Startup Routine Planning & Initial Stocklist Sync
        try:
            self.replan_daily_routine()
        except Exception as e:
            logger.error(f"[STARTUP] Initial plan failed, will retry in loop: {e}", exc_info=True)

        # 2. Autonomous Boot Worker
        def boot_worker():
            logger.info("[STARTUP] Running initial historical sync...")
            try:
                self.execute_historical_sync(is_manual=False)
                synced = [name for name, meta in self.script_status.items() if meta.get("synced")]
                if synced:
                    logger.info(f"[STARTUP] Calculating technical parameters for {len(synced)} synced stocks...")
                    update_all_parameters(synced)
                    logger.info("[STARTUP] Initial parameters calculated successfully.")
            except Exception as e:
                logger.error(f"[STARTUP] Boot worker exception: {e}", exc_info=True)

        threading.Thread(target=boot_worker, daemon=True).start()

        # 3. Main Operational Loop
        while _keep_running:
            try:
                # Flip-Flop Power Check
                if not STATE_BUS.is_power_on():
                    time.sleep(HEARTBEAT_TICK_SEC)
                    continue

                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # Priority 0: Heartbeat Difference Detection (Watchlist vs Stocklist)
                try:
                    was_updated, active_map = reconcile_stocklist_with_watchlist()
                    if was_updated:
                        logger.info("[HEARTBEAT] Change detected in watchlist. Reconciling stocklist and syncing OHLC...")
                        self.replan_daily_routine()
                        self.execute_historical_sync(is_manual=False)
                except Exception as e:
                    logger.error(f"[HEARTBEAT] Error during watchlist reconciliation: {e}")

                # Priority 1: External Manual Sync Pulse Override
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[OVERRIDE PULSE] Immediate sync commanded. Running CHILD-1...")
                    self.execute_historical_sync(is_manual=True)
                    continue

                # Priority 2: Midnight / State-Driven Date Catch-Up
                if self.last_planned_date != today_date:
                    self.replan_daily_routine()

                if not self.is_today_trading_day:
                    time.sleep(HEARTBEAT_TICK_SEC * 5)
                    continue

                # Pre-Market Index 0 Sanitation (09:00 AM IST)
                if now_time.hour == 9 and now_time.minute >= 0 and not self.preopen_cleared_today:
                    self.sanitize_all_indices_zero("Pre-Market (09:00 AM)")
                    self.preopen_cleared_today = True

                # Post-Market Index 0 Sanitation (16:00 PM IST)
                if now_time.hour >= 16 and not self.postclose_cleared_today:
                    self.sanitize_all_indices_zero("Post-Market (16:00 PM)")
                    self.postclose_cleared_today = True

                # Priority 3: Pre-Market Historical Sync Window (08:00 – 08:30 IST)
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s["synced"] for s in self.script_status.values())
                    if has_unsynced and (time.time() - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync active. Retrying unsynced scripts...")
                        self.execute_historical_sync(is_manual=False)
                        self.last_sync_attempt_time = time.time()

                # Audit Report at 08:30 IST
                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # Priority 4: Live Market Window (09:15 – 15:30 IST)
                self.calendar.refresh_calendar()
                status, _ = self.calendar.get_market_status()

                if status == "LIVE":
                    # 5-minute live update pass (Index 0)
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = time.time()

                    # 15-minute parameter calculation pass
                    if (time.time() - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        active_synced_scripts = [name for name, meta in self.script_status.items() if meta["synced"]]
                        if active_synced_scripts:
                            update_all_parameters(active_synced_scripts)
                        self.last_param_calc_time = time.time()

                time.sleep(HEARTBEAT_TICK_SEC)

            except Exception as e:
                logger.critical(f"Unhandled exception in master loop: {e}", exc_info=True)
                time.sleep(5)

        logger.info("Master orchestrator stopped cleanly.")

    def replan_daily_routine(self):
        """Refreshes market calendar and builds script statuses with strict OHLC preservation."""
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today

        # Reconcile /stocklist with /watchlist
        _, stock_map = reconcile_stocklist_with_watchlist()

        # STRICT DATA SAFETY GUARD:
        # Never purge any stock if it exists in the active stock_map (derived directly from watchlist).
        # Only purge an orphaned stock if it is completely absent from watchlist.
        if stock_map and len(stock_map) > 0:
            try:
                stocks_node = db.reference("stocks").get() or {}
                if isinstance(stocks_node, dict):
                    for db_script in list(stocks_node.keys()):
                        if db_script not in stock_map:
                            logger.warning(f"[PURGE-SAFEGUARD] '{db_script}' was removed from master watchlist. Deleting /stocks/{db_script}...")
                            db.reference(f"stocks/{db_script}").delete()
            except Exception as e:
                logger.error(f"[PURGE-SAFEGUARD] Error during orphaned cleanup: {e}")

        # Update in-memory tracking dictionary
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
        logger.info(f"[PLANNER] Plan for {today} IST refreshed: {status_label} ({len(self.script_status)} stocks)")

    def execute_historical_sync(self, is_manual: bool = False):
        """Runs CHILD-1 historical sync under mutex lock."""
        if not self.sync_lock.acquire(blocking=False):
            logger.info("[CHILD-1] Historical sync already running. Skipping duplicate call.")
            return

        try:
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
                        logger.info(f"[{name}] Index 1 matches exchange session (Gap=0). Synced.")
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
                    logger.error(f"Fault isolation caught exception for [{name}]: {e}", exc_info=True)
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
        """Runs CHILD-2 for synchronized stocks only."""
        logger.info("[CHILD-2] Starting 5-minute live update cycle...")

        for name, meta in self.script_status.items():
            if not _keep_running or not STATE_BUS.is_power_on():
                break

            if not meta["synced"]:
                logger.warning(f"[{name}] EXCLUDED FROM LIVE UPDATE | Reason: {meta['error']}")
                continue

            try:
                update_live_script(name, meta["ticker"])
            except Exception as e:
                logger.error(f"[{name}] Live update failed: {e}", exc_info=True)

    def sanitize_all_indices_zero(self, label: str):
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


if __name__ == "__main__":
    # CRITICAL: Start HTTP port listener first so Render detects port immediately
    start_http_listener()
    orchestrator = MasterOrchestrator()
    orchestrator.run()