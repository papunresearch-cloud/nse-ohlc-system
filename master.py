"""
MASTER ORCHESTRATOR
- Priority 0: Watchlist is absolute master. Stocklist is continuously synchronized.
- Never mutates or deletes from /watchlist or /watchlist/detailedDb.
- SR Flip-Flop Power Latch (Default: ON).
- Handles external 30-second pulse commands: /start, /stop, /sync, /param.
- Embedded HTTP Server with minimal /health, GET, and HEAD handling for cron-job.org and Render.
- State-driven date planning (auto-adjusts if restarted or offline at midnight).
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Date-locked 15:30 IST closing sweep and guaranteed /param calculation engine.
- Per-script live quarantine: Unsynced stocks are isolated by Child-2 during regular trading.
- Parameter Engine: Computes indicators every 15 minutes during LIVE sessions.
"""

import os
import sys
import time
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, date, time as dt_time, timedelta
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
    clear_live_candle,
    get_stocklist_mapping
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
        self.pulse_param_time = 0.0

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
            elif command == "PARAM":
                self.pulse_param_time = now
                logger.info("[SIGNAL] MANUAL PARAM pulse captured -> Immediate parameter run scheduled.")

    def check_and_clear_manual_sync(self) -> bool:
        now = time.time()
        with self.lock:
            if (now - self.pulse_sync_time) <= PULSE_VALIDITY_SEC:
                self.pulse_sync_time = 0.0
                return True
        return False

    def check_and_clear_manual_param(self) -> bool:
        now = time.time()
        with self.lock:
            if (now - self.pulse_param_time) <= PULSE_VALIDITY_SEC:
                self.pulse_param_time = 0.0
                return True
        return False

    def is_power_on(self) -> bool:
        with self.lock:
            return self.power_latched_on


STATE_BUS = SystemStateBus()


# =====================================================================
# HTTP COMMAND & HEALTH SERVER (RENDER COMPATIBLE)
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
        elif path in ("/param", "/api/param"):
            STATE_BUS.trigger_pulse("PARAM")
            self._send_resp(200, "MANUAL PARAMETER CALCULATION triggered.\n")
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
        return  # Suppress request spam in stdout logs


def start_http_listener():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), PulseCommandServer)
    logger.info(f"[HTTP] Command server listening on port {port}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


# =====================================================================
# SIGNAL HANDLING
# =====================================================================
def handle_shutdown(signum, frame):
    global _keep_running
    logger.info(f"[SHUTDOWN] Received signal ({signum}). Terminating gracefully...")
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
        self.last_hist_sync_time = 0.0
        self.last_live_update_time = 0.0
        self.last_param_calc_time = 0.0
        
        # Closing routine latches
        self.last_closing_sweep_date = None
        self.last_close_sweep_attempt_time = 0.0

        self.current_plan_date = None
        self.is_today_trading_day = False
        self.script_status = {}
        self.sync_lock = threading.Lock()
        self.audit_logged_today = False

    def replan_daily_routine(self, now_ist: datetime):
        """Initializes state when the calendar date rolls over at midnight."""
        today = now_ist.date()
        self.current_plan_date = today
        self.calendar.refresh_calendar()
        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.audit_logged_today = False

        # Load active stocks and align stocklist with watchlist
        try:
            reconcile_stocklist_with_watchlist()
        except Exception as e:
            logger.error(f"[MAINTENANCE] Reconcile failed on daily plan: {e}")

        stock_map = get_stocklist_mapping()
        self.script_status = {
            name: {
                "ticker": ticker,
                "synced": False,
                "last_attempt_at": None,
                "error": "Awaiting initial sync"
            }
            for name, ticker in stock_map.items()
        }

        status_type = "TRADING SESSION" if self.is_today_trading_day else "NON-TRADING DAY / HOLIDAY"
        logger.info(f"[PLANNER] Date: {today} | Status: {status_type} | Scripts Loaded: {len(self.script_status)}")

    def _calculate_script_gap(self, display_name: str, ticker: str) -> int:
        """Determines gap strictly by comparing Index 1 date vs Yahoo's latest finalized session."""
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
            return 0  # Vendor unreachable; hold current state

        return self.calendar.get_trading_day_gap(latest_fb_date, latest_yahoo_date)

    def execute_historical_sync(self, now_ist: datetime):
        """Executes pre-market gap verification across all equities."""
        if not self.sync_lock.acquire(blocking=False):
            return

        now_str = now_ist.strftime("%H:%M:%S")
        try:
            for name, meta in self.script_status.items():
                if not _keep_running or not STATE_BUS.is_power_on():
                    break

                if meta["synced"]:
                    continue

                meta["last_attempt_at"] = now_str
                ticker = meta["ticker"]

                try:
                    gap = self._calculate_script_gap(name, ticker)
                    if gap == 0:
                        meta["synced"] = True
                        meta["error"] = None
                        logger.info(f"[{name}] Index 1 matches latest exchange session (Gap=0). Synced.")
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
                    logger.error(f"Fault isolation caught exception during sync for [{name}]: {e}", exc_info=True)
        finally:
            self.sync_lock.release()

    def execute_live_updates(self):
        """Runs CHILD-2 live updates with execution throttling."""
        now = time.time()
        # Guardrail: Prevent execution if called within 30 seconds of the previous call
        if (now - self.last_live_update_time) < 30.0:
            logger.warning("[CHILD-2] Execution throttled; called too quickly.")
            return

        self.last_live_update_time = now
        logger.info("[CHILD-2] Starting live update cycle...")

        for name, meta in self.script_status.items():
            if not _keep_running or not STATE_BUS.is_power_on():
                break

            if not meta.get("synced", False):
                logger.warning(f"[{name}] EXCLUDED FROM LIVE UPDATE | Reason: {meta.get('error')}")
                continue

            try:
                update_live_script(name, meta["ticker"])
            except Exception as e:
                logger.error(f"[{name}] Live update failed: {e}", exc_info=True)

    def log_detailed_sync_audit(self):
        """Emits a comprehensive script-by-script diagnostic audit."""
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

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE MASTER START")
        logger.info("==================================================")

        # Startup initialization
        now_ist = datetime.now(IST)
        self.replan_daily_routine(now_ist)

        # Initial bootstrap sync on boot
        logger.info("[STARTUP] Running initial pre-flight gap check across all equities...")
        self.execute_historical_sync(now_ist)
        self.last_hist_sync_time = time.time()

        # Initial parameter computation if stocks are available
        initial_stocks = list(self.script_status.keys())
        if initial_stocks:
            try:
                logger.info(f"[STARTUP] Computing baseline parameters for {len(initial_stocks)} stocks...")
                update_all_parameters(initial_stocks)
                self.last_param_calc_time = time.time()
                logger.info("[STARTUP] Initial parameters calculated successfully.")
            except Exception as e:
                logger.error(f"[STARTUP] Initial parameter calculation failed: {e}", exc_info=True)

        while _keep_running:
            try:
                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # 1. Midnight / Day Rollover Check
                if today_date != self.current_plan_date:
                    logger.info("[PLANNER] Midnight transition detected. Replanning day...")
                    self.replan_daily_routine(now_ist)

                # 2. Check Power Flip-Flop
                if not STATE_BUS.is_power_on():
                    time.sleep(HEARTBEAT_TICK_SEC)
                    continue

                # 3. Check Manual Sync Command
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[OVERRIDE] Manual SYNC triggered via API.")
                    for meta in self.script_status.values():
                        meta["synced"] = False
                    self.execute_historical_sync(now_ist)
                    self.last_hist_sync_time = time.time()

                # 4. Check Manual Param Command
                if STATE_BUS.check_and_clear_manual_param():
                    logger.info("[OVERRIDE] Manual PARAM triggered via API.")
                    all_stocks = list(self.script_status.keys())
                    if all_stocks:
                        update_all_parameters(all_stocks)
                    self.last_param_calc_time = time.time()

                # 5. Pre-Market Historical Sync Window (08:00 - 08:30 IST)
                sync_win_start = dt_time(SYNC_WINDOW_START_HOUR, SYNC_WINDOW_START_MIN)
                sync_win_end = dt_time(SYNC_WINDOW_DEADLINE_HOUR, SYNC_WINDOW_DEADLINE_MIN)

                if self.is_today_trading_day and (sync_win_start <= now_time <= sync_win_end):
                    if (time.time() - self.last_hist_sync_time) >= SYNC_RETRY_INTERVAL_SEC:
                        self.execute_historical_sync(now_ist)
                        self.last_hist_sync_time = time.time()

                # 6. Pre-Market Audit Report (08:30 IST)
                if self.is_today_trading_day and not self.audit_logged_today:
                    if now_time >= sync_win_end:
                        self.log_detailed_sync_audit()
                        self.audit_logged_today = True

                # 7. Evaluate Market Status
                market_status, market_reason = self.calendar.get_market_status(now_ist)

                # 8. Active Trading Hours: Live Updates (5m) & Parameter Calculations (15m)
                if market_status == "LIVE":
                    # Live 5-minute ticks
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()

                    # 15-minute parameter calculations
                    if (time.time() - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        active_stocks = [name for name, meta in self.script_status.items() if meta.get("synced", False)]
                        target_stocks = active_stocks if active_stocks else list(self.script_status.keys())
                        if target_stocks:
                            try:
                                update_all_parameters(target_stocks)
                                self.last_param_calc_time = time.time()
                            except Exception as e:
                                logger.error(f"[PARAM ENGINE] Calculation failed: {e}", exc_info=True)

                # -----------------------------------------------------------------
                # 9. POST-MARKET CLOSE (15:30 IST) GUARANTEED SWEEP & PARAMETER UPDATE
                # -----------------------------------------------------------------
                if (
                    self.is_today_trading_day
                    and now_time >= dt_time(15, 30)
                    and now_time < dt_time(16, 0)
                    and self.last_closing_sweep_date != today_date
                ):
                    # IMMEDIATE DATE LATCH: Prevents any possibility of an infinite loop
                    self.last_closing_sweep_date = today_date
                    self.last_close_sweep_attempt_time = time.time()
                    logger.info("[MARKET CLOSE] 15:30 IST detected. Initiating final closing sequence...")

                    # STEP 1: Capture final closing candles for /stocks (Isolated)
                    try:
                        logger.info("[MARKET CLOSE] Step 1: Sweeping final 15:30 candle for /stocks...")
                        self.execute_live_updates()
                        logger.info("[MARKET CLOSE] Step 1 complete: /stocks finalized.")
                    except Exception as e:
                        logger.error(f"[MARKET CLOSE] Step 1 live update encountered an issue: {e}", exc_info=True)

                    # STEP 2: Compute indicators and write to /param (Guaranteed)
                    try:
                        all_active = list(self.script_status.keys())
                        if all_active:
                            logger.info(f"[MARKET CLOSE] Step 2: Computing closing parameters for {len(all_active)} stocks...")
                            update_all_parameters(all_active)
                            self.last_param_calc_time = time.time()
                            logger.info("[MARKET CLOSE] Step 2 complete: /param successfully updated.")
                        else:
                            logger.warning("[MARKET CLOSE] No registered stocks found for parameter calculation.")
                    except Exception as e:
                        logger.error(f"[MARKET CLOSE] Step 2 parameter calculation failed: {e}", exc_info=True)

                    logger.info("[MARKET CLOSE] Closing sequence completed. System returning to idle monitor.")

                time.sleep(HEARTBEAT_TICK_SEC)

            except Exception as e:
                logger.critical(f"Unhandled exception in master loop: {e}", exc_info=True)
                time.sleep(5)

        logger.info("Master orchestrator stopped safely.")


# =====================================================================
# PROGRAM ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    start_http_listener()
    orchestrator = MasterOrchestrator()
    orchestrator.run()