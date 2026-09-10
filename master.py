"""
MASTER / MOTHER PROGRAM (nse_ohlc_system)
- Priority 0: Watchlist is absolute master. Stocklist is continuously synchronized.
- Never mutates or deletes from /watchlist or /watchlist/detailedDb.
- Guarded OHLC protection: Stocks present in /watchlist are never purged.
- SR Flip-Flop Power Latch (Default: ON).
- External pulse commands: /start, /stop, /sync.
- Embedded HTTP Server with full CORS, /health status reporting, and HEAD handling.
- Pre-market sync window (08:00–08:30 IST) with retry intervals.
- Index 0 remains safe overnight (Never deleted).
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
# HTTP PULSE RECEIVER & HEALTH SERVER (WITH CORS & STATUS MONITORING)
# =====================================================================
class PulseCommandServer(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        """Handles CORS preflight requests from the React browser frontend."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_HEAD(self):
        """Satisfies HEAD requests with 0 body bytes for uptime monitors."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        """Handles incoming pulse commands and keep-alive health pings."""
        path = self.path.lower().strip()
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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return  # Suppress HTTP access logging in stdout to prevent log flooding


def start_http_listener():
    """Starts the HTTP server on Render's designated port in a daemon thread."""
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), PulseCommandServer)
    logger.info(f"[HTTP] Command server listening on 0.0.0.0:{port}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


# =====================================================================
# SIGNAL HANDLING
# =====================================================================
def signal_handler(signum, frame):
    global _keep_running
    logger.info(f"Shutdown signal ({signum}) received. Stopping Master gracefully...")
    _keep_running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


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
        self.post_market_calc_done = False

        self.last_sync_attempt_time = 0.0
        self.last_live_update_time = 0.0
        self.last_param_calc_time = 0.0

        # Per-script dictionary holding operational state
        self.script_status = {}

    def _get_reconciled_stock_map(self) -> dict:
        """Helper to unpack reconcile_stocklist_with_watchlist safely whether it returns dict or tuple."""
        result = reconcile_stocklist_with_watchlist()
        if isinstance(result, tuple):
            return result[0] if len(result) > 0 and isinstance(result[0], dict) else {}
        elif isinstance(result, dict):
            return result
        return {}

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE")
        logger.info("==================================================")

        # 1. Startup initialization and schedule planning
        self.replan_daily_routine()

        # 2. Run initial historical sync in background thread so HTTP is responsive immediately
        threading.Thread(
            target=self.execute_historical_sync, 
            kwargs={"is_manual": False}, 
            daemon=True
        ).start()

        while _keep_running:
            try:
                # 1. Flip-Flop Power Check
                if not STATE_BUS.is_power_on():
                    time.sleep(HEARTBEAT_TICK_SEC)
                    continue

                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # Priority 0: Manual Sync Trigger (Instant Interruption)
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[MANUAL OVERRIDE] Immediate resync commanded. Processing all scripts...")
                    self.execute_historical_sync(is_manual=True)
                    continue

                # Priority 1: State-Driven Date Catch-Up (crossing midnight)
                if self.last_planned_date != today_date:
                    self.replan_daily_routine()

                # If today is a weekend or NSE holiday, sleep and wait for next calendar date
                if not self.is_today_trading_day:
                    time.sleep(HEARTBEAT_TICK_SEC * 5)
                    continue

                # Priority 2: Pre-Market Historical Sync Window (08:00 – 08:30 IST)
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s.get("synced", False) for s in self.script_status.values())
                    if has_unsynced and (time.time() - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync window active. Retrying unsynced scripts...")
                        self.execute_historical_sync(is_manual=False)
                        self.last_sync_attempt_time = time.time()

                # Audit Report Check at or after 08:30 IST
                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # Priority 3: Live Market Hours Execution (09:15 – 15:30 IST)
                self.calendar.refresh_calendar()
                market_status, _ = self.calendar.get_market_status()

                if market_status == "LIVE":
                    # 5-minute live intraday candle updates (Index 0)
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = time.time()

                    # 15-minute parameter engine calculations
                    if (time.time() - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        self.execute_parameter_calculations()
                        self.last_param_calc_time = time.time()

                # Priority 4: Post-Market Indicator Computation (At 15:30 IST)
                if (now_time.hour == 15 and now_time.minute >= 30) or now_time.hour >= 16:
                    if not self.post_market_calc_done:
                        logger.info("[POST-MARKET] Market session concluded. Running final daily parameter computation...")
                        self.execute_parameter_calculations()
                        self.post_market_calc_done = True

                # Fast heartbeat rest
                time.sleep(HEARTBEAT_TICK_SEC)

            except Exception as e:
                logger.critical(f"Unhandled exception in Master loop: {e}", exc_info=True)
                time.sleep(5)

        logger.info("Master orchestrator stopped safely.")

    def replan_daily_routine(self):
        """Generates or updates today's plan, rebuilds script status, and reconciles stocklist."""
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today
        self.sync_audit_reported_today = False
        self.post_market_calc_done = False

        # Safe unpack of reconciled stock dictionary
        stock_map = self._get_reconciled_stock_map()
        if not stock_map:
            logger.warning("[SAFETY] Watchlist reconciliation returned 0 stocks. Historical data preserved.")
            return

        # Rebuild script status dictionary
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
        """Executes CHILD-1 historical sync across registered stocks."""
        logger.info(f"[SYNC] Starting historical sync cycle (Manual={is_manual})...")
        stock_map = self._get_reconciled_stock_map()

        for name, ticker in stock_map.items():
            if not _keep_running:
                break
            try:
                existing_ohlc = get_stock_ohlc(name)
                gap = TARGET_OHLC_COUNT

                if existing_ohlc:
                    idx1 = existing_ohlc.get("1") if isinstance(existing_ohlc, dict) else None
                    if idx1 and "date" in idx1:
                        fb_date = datetime.strptime(str(idx1["date"]), "%Y-%m-%d").date()
                        latest_yahoo_date = get_latest_available_trading_date(ticker)
                        if latest_yahoo_date:
                            gap = self.calendar.get_trading_day_gap(fb_date, latest_yahoo_date)

                if not existing_ohlc or gap > 0:
                    logger.info(f"[{name}] Sync required. Missing gap: {gap} trading day(s).")
                    success = sync_historical_script(name, ticker, gap_trading_days=gap, calendar=self.calendar)
                else:
                    success = True

                self.script_status[name] = {
                    "synced": success,
                    "last_attempt_at": datetime.now(IST).strftime("%H:%M:%S"),
                    "error": None if success else "Vendor fetch failure",
                    "ticker": ticker
                }
            except Exception as e:
                logger.error(f"[SYNC] Error synchronizing {name}: {e}", exc_info=True)
                self.script_status[name] = {
                    "synced": False,
                    "last_attempt_at": datetime.now(IST).strftime("%H:%M:%S"),
                    "error": str(e),
                    "ticker": ticker
                }

    def execute_live_updates(self):
        """Executes CHILD-2 live intraday candle updates on Index 0."""
        for name, meta in list(self.script_status.items()):
            if not _keep_running:
                break
            ticker = meta.get("ticker", name)
            try:
                update_live_script(name, ticker)
            except Exception as e:
                logger.error(f"[LIVE] Error updating live candle for {name}: {e}")

    def execute_parameter_calculations(self):
        """Executes parameter calculation across all active stocks."""
        active_symbols = list(self.script_status.keys())
        if active_symbols:
            logger.info(f"[PARAM] Running indicator recalculation for {len(active_symbols)} stocks...")
            try:
                update_all_parameters(active_symbols)
            except Exception as e:
                logger.error(f"[PARAM] Error during parameter execution: {e}", exc_info=True)

    def log_detailed_sync_audit(self):
        """Outputs a clean audit report of pre-market sync."""
        synced = [k for k, v in self.script_status.items() if v.get("synced")]
        unsynced = [k for k, v in self.script_status.items() if not v.get("synced")]

        logger.info("=" * 85)
        logger.info("                   SCRIPT-WISE SYNCHRONIZATION AUDIT REPORT")
        logger.info("=" * 85)
        logger.info(f"Total: {len(self.script_status)} | Synced: {len(synced)} | Unsynced: {len(unsynced)}")

        if synced:
            logger.info("[SYNCHRONIZED STOCKS]")
            for s in synced:
                logger.info(f"  ✓ {s:<24} | Synced At: {self.script_status[s]['last_attempt_at']}")
        if unsynced:
            logger.error("[UNSYNCHRONIZED STOCKS - AWAITING RETRY]")
            for u in unsynced:
                info = self.script_status[u]
                logger.error(f"  ✗ {u:<24} | Last Attempt: {info['last_attempt_at']} | Error: {info['error']}")
        logger.info("=" * 85)


# =====================================================================
# PROGRAM ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    # 1. Bind port immediately so Render web service health check passes
    start_http_listener()

    # 2. Run master orchestrator
    orchestrator = MasterOrchestrator()
    orchestrator.run()