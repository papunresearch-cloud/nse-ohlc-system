"""
MASTER ORCHESTRATOR (STABLE PRE-PURGE ARCHITECTURE)
- Embedded HTTP server on port 10000 (/health, /start, /stop, /sync) with robust URL parsing.
- URL Query parameter sanitization via urllib.parse.urlparse.
- Full CORS preflight support (OPTIONS, HEAD, GET).
- SR Flip-Flop Power Latch (Default: ON).
- Real-time /system_status heartbeat telemetry every 300s.
- Non-blocking 5-second main loop idle tick on weekends and holidays.
- Dynamic stock discovery using get_stocklist_mapping().
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Index 0 preserved (no 09:00 AM or 16:00 PM wipes).
- 5-minute live updates (CHILD-2) and 15-minute parameter engine calculations.
"""
import os
import sys
import time
import signal
import threading
from urllib.parse import urlparse
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
    get_stocklist_mapping,
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
                self.pulse_sync_time = 0.0
                return True
        return False

    def is_power_on(self) -> bool:
        with self.lock:
            return self.power_latched_on


STATE_BUS = SystemStateBus()


# =====================================================================
# HTTP PULSE RECEIVER & HEALTH SERVER
# =====================================================================
class PulseCommandServer(BaseHTTPRequestHandler):
    def _apply_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")

    def do_OPTIONS(self):
        self.send_response(204)
        self._apply_cors_headers()
        self.end_headers()

    def do_HEAD(self):
        self.send_response(200)
        self._apply_cors_headers()
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        # Extract clean URL path, stripping off query parameters (?_t=...)
        parsed_url = urlparse(self.path)
        path = parsed_url.path.lower().strip()
        state_str = "ON" if STATE_BUS.is_power_on() else "OFF"

        if path in ("/health", "/ping"):
            self._send_resp(200, f"OK - State: {state_str}\n")
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
            self._send_resp(200, f"State: {state_str}\n")

    def _send_resp(self, code: int, message: str):
        payload = message.encode("utf-8")
        self.send_response(code)
        self._apply_cors_headers()
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

        self.last_sync_attempt_time = 0.0
        self.last_live_update_time = 0.0
        self.last_param_calc_time = 0.0
        self.last_heartbeat_time = 0.0

        self.script_status = {}

    def record_heartbeat(self):
        """Writes heartbeat status to Firebase /system_status."""
        try:
            now_epoch = time.time()
            now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            power_status = "RUNNING" if STATE_BUS.is_power_on() else "STOPPED"

            db.reference("system_status").update({
                "backend_power": power_status,
                "last_heartbeat": now_ist_str,
                "heartbeat_epoch": now_epoch
            })
            self.last_heartbeat_time = now_epoch
            logger.info(f"[HEARTBEAT] Telemetry updated -> {now_ist_str}")
        except Exception as e:
            logger.error(f"[HEARTBEAT] Failed to update telemetry: {e}")

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE")
        logger.info("==================================================")

        # 1. Startup routine and initial heartbeat
        self.replan_daily_routine()
        self.record_heartbeat()

        # 2. Non-blocking initial sync in background
        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True).start()

        # 3. Main Dispatch Loop
        while _keep_running:
            try:
                now_epoch = time.time()
                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # --- Periodic Heartbeat (Every 300s) ---
                if (now_epoch - self.last_heartbeat_time) >= 300:
                    self.record_heartbeat()

                # --- Priority 0: Manual Pulse Interruption (Independent of power state) ---
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[OVERRIDE PULSE] Immediate sync commanded. Running CHILD-1 in background...")
                    threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": True}, daemon=True).start()
                    continue

                # --- Power Latch Check ---
                if not STATE_BUS.is_power_on():
                    time.sleep(5)
                    continue

                # --- Priority 1: State-Driven Date Catch-Up ---
                if self.last_planned_date != today_date:
                    self.replan_daily_routine()

                # Non-trading days: Sleep in short 5s increments to keep the heartbeat ticking
                if not self.is_today_trading_day:
                    time.sleep(5)
                    continue

                # --- Priority 2: Pre-Market Historical Sync Window (08:00 – 08:30 IST) ---
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s["synced"] for s in self.script_status.values())
                    if has_unsynced and (now_epoch - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync window active. Retrying unsynced scripts...")
                        self.execute_historical_sync(is_manual=False)
                        self.last_sync_attempt_time = now_epoch

                # Audit report at or after 08:30 IST
                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # --- Priority 3: Live Market Window (09:15 – 15:30 IST) ---
                self.calendar.refresh_calendar()
                status, _ = self.calendar.get_market_status()

                if status == "LIVE":
                    # 5-minute live update pass
                    if (now_epoch - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = now_epoch

                    # 15-minute parameter recalculation pass
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
        """Initializes day plan using get_stocklist_mapping."""
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today
        self.sync_audit_reported_today = False

        stock_map = get_stocklist_mapping()
        self.script_status = {
            name: {
                "synced": False,
                "last_attempt_at": None,
                "error": "Awaiting daily sync",
                "ticker": ticker
            }
            for name, ticker in stock_map.items()
        }
        status_label = "TRADING SESSION" if self.is_today_trading_day else "NON-TRADING DAY (Closed)"
        logger.info(f"[PLANNER] Plan for {today} IST refreshed: {status_label} ({len(self.script_status)} stocks)")

    def execute_historical_sync(self, is_manual: bool = False):
        """Runs CHILD-1 historical sync."""
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

        # Update sync telemetry in Firebase
        try:
            synced_count = len([k for k, v in self.script_status.items() if v["synced"]])
            total_count = len(self.script_status)
            db.reference("system_status/sync_data").update({
                "last_sync_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
                "total_registered": total_count,
                "synced_count": synced_count,
                "status": "VERIFIED" if (total_count > 0 and synced_count == total_count) else "IDLE"
            })
        except Exception:
            pass

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
        """Runs CHILD-2 live updates."""
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