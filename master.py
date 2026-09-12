"""
MASTER ORCHESTRATOR
- Watchlist is master. Stocklist is continuously synchronized.
- Never mutates or deletes from /watchlist or /watchlist/detailedDb.
- Guarded OHLC protection: Stocks currently present in /watchlist are never purged.
- SR Flip-Flop Power Latch (Default: ON).
- Handles external 30-second pulse commands: /start, /stop, /sync.
- Embedded HTTP Server on port 10000 with /health handling for Render/cron-job.org.
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Index 0 live sanitization at 09:00 IST and 16:00 IST.
- Dispute-based database auditing (Ideal vs. Lagging sessions).
- Parameter Engine: Computes indicators every 15 minutes during LIVE sessions.
"""
import os
import sys
import time
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
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
    logger
)

try:
    from config import PARAM_UPDATE_INTERVAL_SEC
except ImportError:
    PARAM_UPDATE_INTERVAL_SEC = 900

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
from parameter import update_all_parameters
from health import HEALTH_MONITOR

IST = pytz.timezone(TIMEZONE)
_keep_running = True


class SystemStateBus:
    def __init__(self):
        self.lock = threading.Lock()
        self.power_latched_on = True
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


class PulseCommandServer(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

    def do_GET(self):
        path = self.path.strip().lower()
        if path == "/start":
            STATE_BUS.trigger_pulse("START")
            msg = b"OK - POWER ON LATCHED"
        elif path == "/stop":
            STATE_BUS.trigger_pulse("STOP")
            msg = b"OK - POWER OFF LATCHED"
        elif path == "/sync":
            STATE_BUS.trigger_pulse("SYNC")
            msg = b"OK - SYNC PULSE RECEIVED"
        elif path == "/health":
            msg = b"OK - HEALTHY"
        else:
            msg = b"DAEMON RUNNING"

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        self.wfile.write(msg)

    def log_message(self, format, *args):
        pass


def start_http_listener():
    port_str = os.environ.get("PORT", "10000")
    try:
        port = int(port_str)
        server = HTTPServer(("0.0.0.0", port), PulseCommandServer)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        logger.info(f"Pulse Command Listener active on port {port}")
    except Exception as e:
        logger.error(f"Failed to start HTTP listener on port {port_str}: {e}")


class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.calendar = MarketCalendar()
        self.script_status = {}
        self.active_tickers = {}
        self.today_plan = None
        self.last_planned_date = None
        self.last_live_update_time = 0.0
        self.last_sync_retry_time = 0.0
        self.last_param_calc_time = 0.0
        self.last_closing_sweep_date = None
        self.initial_boot_sync_done = False
        self.sync_lock = threading.Lock()

    def replan_daily_routine(self, now_ist: datetime):
        today = now_ist.date()
        if self.last_planned_date == today:
            return

        is_trading = self.calendar.is_trading_day(today)
        self.today_plan = {
            "date": today,
            "is_trading_day": is_trading,
            "sanitized_0900": False,
            "sanitized_1600": False
        }
        self.last_planned_date = today

        status_str = "TRADING SESSION" if is_trading else "MARKET HOLIDAY / WEEKEND"
        logger.info(f"[PLANNER] Plan for {today} refreshed: {status_str}")

        self.reconcile_and_load_scripts()

    def reconcile_and_load_scripts(self):
        try:
            recon_result = reconcile_stocklist_with_watchlist()
            if isinstance(recon_result, tuple):
                active_stocks = recon_result[1] if len(recon_result) > 1 and isinstance(recon_result[1], dict) else recon_result[0]
            elif isinstance(recon_result, dict):
                active_stocks = recon_result
            else:
                active_stocks = {}

            if not active_stocks:
                logger.warning("[SAFETY] Watchlist returned 0 stocks. Retaining active memory.")
                return

            self.active_tickers = active_stocks
            for name in active_stocks.keys():
                if name not in self.script_status:
                    self.script_status[name] = {
                        "synced": False,
                        "last_attempt_at": "Never",
                        "error": "Awaiting initial sync"
                    }

            removed = [s for s in list(self.script_status.keys()) if s not in active_stocks]
            for s in removed:
                del self.script_status[s]

        except Exception as e:
            logger.error(f"[RECONCILE] Failed to load scripts: {e}")

    def audit_database_disputes(self):
        try:
            target_date = self.calendar.get_latest_completed_trading_date(datetime.now(IST))
            target_date_str = target_date.strftime("%Y-%m-%d")
        except Exception:
            target_date = datetime.now(IST).date()
            target_date_str = target_date.strftime("%Y-%m-%d")

        audit_results = []
        for name in self.active_tickers.keys():
            ohlc = get_stock_ohlc(name)
            if not ohlc or not isinstance(ohlc, dict) or "1" not in ohlc:
                audit_results.append({
                    "name": name,
                    "stored_date": "None",
                    "gap": TARGET_OHLC_COUNT,
                    "broken": True
                })
                continue

            idx1_date_str = str(ohlc["1"].get("date", ""))
            try:
                idx1_date = datetime.strptime(idx1_date_str, "%Y-%m-%d").date()
                if hasattr(self.calendar, "get_trading_day_gap"):
                    gap = self.calendar.get_trading_day_gap(idx1_date, target_date)
                else:
                    gap = (target_date - idx1_date).days
                audit_results.append({
                    "name": name,
                    "stored_date": idx1_date_str,
                    "gap": max(0, gap),
                    "broken": False
                })
            except Exception:
                audit_results.append({
                    "name": name,
                    "stored_date": idx1_date_str,
                    "gap": 1,
                    "broken": True
                })

        HEALTH_MONITOR.record_dispute_audit(target_date_str, audit_results)

    def execute_historical_sync(self, is_manual: bool = False):
        if not self.sync_lock.acquire(blocking=False):
            return

        try:
            HEALTH_MONITOR.record_audit_start()
            self.reconcile_and_load_scripts()
            scripts_to_sync = list(self.active_tickers.items())

            for name, ticker in scripts_to_sync:
                try:
                    success, msg = sync_historical_script(name, ticker)
                    self.script_status[name] = {
                        "synced": success,
                        "last_attempt_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                        "error": "" if success else msg
                    }
                except Exception as e:
                    self.script_status[name] = {
                        "synced": False,
                        "last_attempt_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                        "error": str(e)
                    }

            self.audit_database_disputes()
            self.initial_boot_sync_done = True
        finally:
            self.sync_lock.release()

    def execute_live_updates(self):
        now_ist = datetime.now(IST)
        count = 0
        for name, ticker in self.active_tickers.items():
            meta = self.script_status.get(name, {})
            if not meta.get("synced", False):
                continue
            try:
                if update_live_script(name, ticker):
                    count += 1
            except Exception as e:
                logger.error(f"[{name}] Live update error: {e}")

        # Check market session state
        is_live = False
        if hasattr(self.calendar, "is_market_live"):
            is_live = self.calendar.is_market_live(now_ist)
        elif hasattr(self.calendar, "is_market_open"):
            is_live = self.calendar.is_market_open(now_ist)

        HEALTH_MONITOR.record_live_update(count, is_market_open=is_live)

    def run(self):
        global _keep_running
        logger.info("[DAEMON] Master orchestrator operational.")

        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True).start()

        while _keep_running:
            try:
                now_ist = datetime.now(IST)
                HEALTH_MONITOR.record_heartbeat(STATE_BUS.is_power_on())

                # 1. Pulse Commands Check
                if STATE_BUS.check_and_clear_manual_sync():
                    threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": True}, daemon=True).start()

                if not STATE_BUS.is_power_on():
                    time.sleep(HEARTBEAT_TICK_SEC)
                    continue

                # 2. Plan Check
                self.replan_daily_routine(now_ist)

                # 3. Pre-Market Sync Window (08:00 - 08:30 IST)
                if (now_ist.hour == SYNC_WINDOW_START_HOUR and 
                    SYNC_WINDOW_START_MIN <= now_ist.minute < SYNC_WINDOW_DEADLINE_MIN):
                    if (time.time() - self.last_sync_retry_time) >= SYNC_RETRY_INTERVAL_SEC:
                        threading.Thread(target=self.execute_historical_sync, kwargs={"is_manual": False}, daemon=True).start()
                        self.last_sync_retry_time = time.time()

                # 4. Live Session Handling (09:15 - 15:30 IST)
                market_is_live = False
                if hasattr(self.calendar, "is_market_live"):
                    market_is_live = self.calendar.is_market_live(now_ist)
                elif hasattr(self.calendar, "is_market_open"):
                    market_is_live = self.calendar.is_market_open(now_ist)

                if market_is_live:
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = time.time()

                    if (time.time() - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        try:
                            update_all_parameters()
                        except Exception as e:
                            logger.error(f"[PARAM] Error calculating parameters: {e}")
                        self.last_param_calc_time = time.time()

                time.sleep(HEARTBEAT_TICK_SEC)

            except Exception as e:
                logger.critical(f"[FATAL LOOP EXCEPTION]: {e}", exc_info=True)
                time.sleep(HEARTBEAT_TICK_SEC)


def handle_shutdown(signum, frame):
    global _keep_running
    logger.info("Termination signal received. Shutting down gracefully...")
    _keep_running = False
    sys.exit(0)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

if __name__ == "__main__":
    start_http_listener()
    orchestrator = MasterOrchestrator()
    orchestrator.run()