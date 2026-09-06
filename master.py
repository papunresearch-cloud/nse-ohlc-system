"""
MASTER ORCHESTRATOR
- SR Flip-Flop Power Latch (Default: ON).
- Handles 30-second pulse commands: /start, /stop, /sync.
- Embedded HTTP Server with minimal /health, GET, and HEAD handling for cron-job.org.
- State-driven date planning (auto-adjusts if restarted or offline at midnight).
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- 08:30 IST synchronization audit log with script-by-script diagnostic reporting.
- Index 0 live sanitization: Clears index 0 at 09:00 IST and 16:00 IST.
- Per-script live quarantine: Unsynced stocks are skipped by Child-2.
- Index 1 vs Index 0 alignment: Sync reads Index 1; Live updates Index 0.
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

# Defensive fallback in case PARAM_UPDATE_INTERVAL_SEC is not defined in config.py
try:
    from config import PARAM_UPDATE_INTERVAL_SEC
except ImportError:
    PARAM_UPDATE_INTERVAL_SEC = 900  # 15 minutes default

from firebase_manager import init_firebase, get_stocklist_mapping, get_stock_ohlc, clear_live_candle
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
        self.power_latched_on = True  # Default state: ON
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
# HTTP PULSE RECEIVER & HEALTH SERVER (CRON-JOB COMPATIBLE)
# =====================================================================
class PulseCommandServer(BaseHTTPRequestHandler):
    def do_HEAD(self):
        """Satisfies HEAD requests with 0 body bytes to pass keep-alive checks."""
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        """Handles incoming pulse commands and keep-alive health pings."""
        path = self.path.lower().strip()
        
        # Dedicated keep-alive route for cron-job.org
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
        return  # Suppress HTTP access logging in stdout


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

        # Per-script dictionary holding independent operational state
        self.script_status = {}

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE")
        logger.info("==================================================")

        # Startup initialization
        self.replan_daily_routine()
        
        # Run startup sync in a background daemon thread so HTTP listener remains immediately responsive
        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True).start()

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

                # Priority 1: State-Driven Date Catch-Up (e.g. crossing midnight or waking late)
                if self.last_planned_date != today_date:
                    self.replan_daily_routine()

                # If today is a weekend or NSE holiday, sleep and wait for the next calendar date
                if not self.is_today_trading_day:
                    time.sleep(HEARTBEAT_TICK_SEC * 5)
                    continue

                # 2. Pre-Market Index 0 Sanitation (09:00 AM IST)
                if now_time.hour == 9 and now_time.minute >= 0 and not self.preopen_cleared_today:
                    self.sanitize_all_indices_zero("Pre-Market (09:00 AM)")
                    self.preopen_cleared_today = True

                # 3. Post-Market Index 0 Sanitation (16:00 PM IST)
                if now_time.hour >= 16 and not self.postclose_cleared_today:
                    self.sanitize_all_indices_zero("Post-Market (16:00 PM)")
                    self.postclose_cleared_today = True

                # Priority 2: Pre-Market Historical Sync Window (08:00 – 08:30 IST)
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s["synced"] for s in self.script_status.values())
                    if has_unsynced and (time.time() - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync window active. Retrying unsynced scripts...")
                        self.execute_historical_sync(is_manual=False)
                        self.last_sync_attempt_time = time.time()

                # Audit Report Check at or after 08:30 IST
                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # Priority 3: Live Market Window (09:15 – 15:30 IST)
                self.calendar.refresh_calendar()
                status, _ = self.calendar.get_market_status()

                if status == "LIVE":
                    # 5-minute live update pass
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = time.time()

                    # 15-minute parameter recalculation pass
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
        """Generates or updates today's calendar plan and resets tracking flags."""
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today
        self.sync_audit_reported_today = False
        self.preopen_cleared_today = False
        self.postclose_cleared_today = False

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
        logger.info(f"[PLANNER] Day plan for {today} IST initialized: {status_label} ({len(self.script_status)} stocks)")

    def execute_historical_sync(self, is_manual: bool = False):
        """Runs CHILD-1 historical sync for scripts with strict fault isolation."""
        now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

        for name, meta in self.script_status.items():
            if not _keep_running:
                break
            if not STATE_BUS.is_power_on() and not is_manual:
                break

            # Skip stocks already synced today unless manually forced
            if meta["synced"] and not is_manual:
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

                # Run historical sync worker
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

    def _calculate_script_gap(self, display_name: str, ticker: str) -> int:
        """Determines gap strictly by comparing Index 1 date vs Yahoo's latest date."""
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
            return 0  # Vendor unreachable; keep existing status

        return self.calendar.get_trading_day_gap(latest_fb_date, latest_yahoo_date)

    def execute_live_updates(self):
        """Runs CHILD-2 for synchronized stocks only, isolating unsynced ones."""
        logger.info("[CHILD-2] Starting 5-minute live update cycle...")

        for name, meta in self.script_status.items():
            if not _keep_running or not STATE_BUS.is_power_on():
                break

            # If not synced, isolate and skip live update
            if not meta["synced"]:
                logger.warning(
                    f"[{name}] EXCLUDED FROM LIVE UPDATE | Reason: {meta['error']} | Last Attempt: {meta['last_attempt_at']}"
                )
                continue

            try:
                update_live_script(name, meta["ticker"])
            except Exception as e:
                logger.error(f"[{name}] Live update failed: {e}", exc_info=True)

    def sanitize_all_indices_zero(self, label: str):
        """Clears Index 0 across all equities to prevent stale quotes."""
        logger.info(f"[MAINTENANCE] Clearing Index 0 for all stocks ({label})...")
        for name in self.script_status.keys():
            clear_live_candle(name)

    def log_detailed_sync_audit(self):
        """Prints a comprehensive script-by-script diagnostic audit at 08:30 IST."""
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
            logger.critical(f"Database Synch Error: {len(unsynced)} stock(s) failed pre-market validation.")
        else:
            logger.info("All registered stocks successfully synchronized. Live updater ready.")
        logger.info("=" * 85)


if __name__ == "__main__":
    start_http_listener()
    orchestrator = MasterOrchestrator()
    orchestrator.run()