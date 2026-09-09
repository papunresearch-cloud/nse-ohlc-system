"""
MASTER ORCHESTRATOR
- Priority 0: Watchlist is absolute master. Stocklist is continuously synchronized.
- Resolves authentic exchange tickers dynamically from /watchlist/detailedDb/<stock_name>.
- SR Flip-Flop Power Latch (Default: ON).
- Handles external 30-second pulse commands: /start, /stop, /sync.
- Embedded HTTP Server on $PORT with minimal /health, GET, and HEAD handling for Render & cron-job.org.
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Index-0 Persistence: Index 0 retains its final closing value overnight.
- Per-script live quarantine: Unsynced stocks are isolated by Child-2.
- 15:30 IST Market Close Engine: Forces final closing live tick to Index 0, 
  followed immediately by closing parameter calculation.
- Reactive Watchlist Ingestion: When a new script is detected, /stocklist updates,
  OHLC data (/stocks) is fetched, and technical parameters (/param) are calculated immediately.
"""
import os
import sys
import time
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, date, time as dt_time
import pytz

PORT = int(os.environ.get("PORT", 10000))

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
    get_stock_ohlc
)
from market_calendar import MarketCalendar
from sync_child import sync_historical_script
from live_child import update_live_script
from yahoo_manager import get_latest_available_trading_date
from parameter import update_all_parameters

IST = pytz.timezone(TIMEZONE)
_keep_running = True

FALLBACK_TICKER_MAP = {
    "Bharat Electron": "BEL.NS",
    "Caplin Point Lab": "CAPLIPOINT.NS",
    "Coal India": "COALINDIA.NS",
    "Gokul Agro": "GOKULAGRO.NS",
    "Gravita India": "GRAVITA.NS",
    "HCL Technologies": "HCLTECH.NS",
    "REC Ltd": "RECLTD.NS",
    "Va Tech Wabag": "WABAG.NS",
    "Abbott India": "ABBOTINDIA.NS",
    "Dixon Technolog_": "DIXON.NS",
    "Infosys": "INFY.NS",
    "Kaynes Tech": "KAYNES.NS",
    "NMDC": "NMDC.NS",
    "Reliance Industries": "RELIANCE.NS",
    "TCS": "TCS.NS",
    "Vedanta": "VEDL.NS",
    "Wipro": "WIPRO.NS"
}


# =====================================================================
# HARDWARE-STYLE FLIP-FLOP & PULSE BUS
# =====================================================================
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


# =====================================================================
# HTTP PULSE RECEIVER & HEALTH SERVER (RENDER / CRON-JOB COMPATIBLE)
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
        pass


def start_http_listener():
    try:
        server = HTTPServer(("0.0.0.0", PORT), PulseCommandServer)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        logger.info(f"[HTTP] Command server listening on port {PORT}")
    except Exception as e:
        logger.error(f"[HTTP] Failed to start pulse listener on port {PORT}: {e}")


# =====================================================================
# SIGNAL HANDLING
# =====================================================================
def handle_shutdown(signum, frame):
    global _keep_running
    logger.info(f"Received termination signal ({signum}). Shutting down gracefully...")
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
        self.final_param_calculated_today = False

        self.last_sync_attempt_time = 0.0
        self.last_live_update_time = 0.0
        self.last_param_calc_time = 0.0

        self.sync_lock = threading.Lock()
        self.script_status = {}

    def resolve_ticker_for_stock(self, stock_name: str) -> str:
        """
        Dynamically extracts authentic exchange ticker from /watchlist/detailedDb/<stock_name>
        with fallback to /param/<stock_name>/TICKER and local map.
        """
        try:
            stock_info = db.reference(f"watchlist/detailedDb/{stock_name}").get() or {}

            ticker = stock_info.get("TICKER") or stock_info.get("ticker")
            if ticker:
                return str(ticker).strip()

            nse_code = stock_info.get("NSE") or stock_info.get("CODE") or stock_info.get("code")
            if nse_code:
                clean_code = str(nse_code).strip().upper()
                return f"{clean_code}.NS" if not clean_code.endswith(".NS") else clean_code

        except Exception as e:
            logger.warning(f"[{stock_name}] detailedDb ticker lookup warning: {e}")

        try:
            stored_ticker = db.reference(f"param/{stock_name}/TICKER").get()
            if stored_ticker:
                return str(stored_ticker).strip()
        except Exception:
            pass

        if stock_name in FALLBACK_TICKER_MAP:
            return FALLBACK_TICKER_MAP[stock_name]

        return stock_name if ("." in stock_name or "^" in stock_name) else f"{stock_name.replace(' ', '')}.NS"

    def _purge_stocks_garbage(self, active_stock_set: set):
        try:
            stocks_node = db.reference("stocks").get() or {}
            if isinstance(stocks_node, dict):
                for db_script in list(stocks_node.keys()):
                    if db_script not in active_stock_set:
                        logger.warning(f"[PURGE-STOCKS] '{db_script}' removed from watchlist. Purging /stocks/{db_script}...")
                        db.reference(f"stocks/{db_script}").delete()
        except Exception as e:
            logger.error(f"[PURGE-STOCKS] Error clearing /stocks node: {e}", exc_info=True)

    def _purge_param_garbage(self, active_stock_set: set):
        try:
            param_node = db.reference("param").get() or {}
            if isinstance(param_node, dict):
                for key in list(param_node.keys()):
                    clean_key = key.replace("_", ".")
                    if key not in active_stock_set and clean_key not in active_stock_set:
                        logger.warning(f"[PURGE-PARAM] '{key}' removed from watchlist. Purging /param/{key}...")
                        db.reference("param").child(key).delete()
        except Exception as e:
            logger.error(f"[PURGE-PARAM] Error clearing /param node: {e}", exc_info=True)

    def purge_all_garbage(self, active_stock_set: set):
        if not active_stock_set:
            logger.warning("[SAFETY LOCK] Active stock set is empty. Skipping purge.")
            return
        self._purge_stocks_garbage(active_stock_set)
        self._purge_param_garbage(active_stock_set)

    def replan_daily_routine(self):
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today
        self.sync_audit_reported_today = False
        self.final_param_calculated_today = False

        _, active_map = reconcile_stocklist_with_watchlist()
        if not active_map:
            logger.warning("[PLANNER] Watchlist reconciliation returned 0 active stocks.")
        else:
            self.purge_all_garbage(set(active_map.keys()))

        new_status = {}
        for name in active_map.keys():
            ticker = self.resolve_ticker_for_stock(name)
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
        logger.info(f"[PLANNER] Day plan for {today} IST initialized: {status_label} ({len(self.script_status)} stocks)")

    def _calculate_script_gap(self, name: str, ticker: str) -> int:
        existing = get_stock_ohlc(name)
        if not existing:
            return TARGET_OHLC_COUNT

        idx1 = existing.get("1") if isinstance(existing, dict) else None
        if not idx1 or "date" not in idx1:
            return TARGET_OHLC_COUNT

        try:
            fb_date = datetime.strptime(str(idx1["date"]), "%Y-%m-%d").date()
        except ValueError:
            return TARGET_OHLC_COUNT

        latest_yahoo = get_latest_available_trading_date(ticker)
        if not latest_yahoo:
            return 0

        return self.calendar.get_trading_day_gap(fb_date, latest_yahoo)

    def execute_historical_sync(self, is_manual: bool = False, target_scripts: list[str] = None) -> list[str]:
        """
        Runs CHILD-1 historical sync under mutex lock.
        Returns a list of script names that successfully synchronized during this run.
        """
        if not self.sync_lock.acquire(blocking=False):
            logger.info("[CHILD-1] Historical sync already in progress. Skipping duplicate.")
            return []

        synced_in_this_pass = []
        try:
            now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
            candidates = target_scripts if target_scripts is not None else list(self.script_status.keys())

            for name in candidates:
                if not _keep_running:
                    break
                if not STATE_BUS.is_power_on() and not is_manual:
                    break

                meta = self.script_status.get(name)
                if not meta:
                    continue

                if meta["synced"] and not is_manual and target_scripts is None:
                    continue

                meta["last_attempt_at"] = now_str
                ticker = meta["ticker"]

                try:
                    gap = self._calculate_script_gap(name, ticker)
                    if gap == 0:
                        meta["synced"] = True
                        meta["error"] = None
                        logger.info(f"[{name}] Index 1 matches latest exchange session (Gap=0). Synced.")
                        synced_in_this_pass.append(name)
                        continue

                    success, msg = sync_historical_script(name, ticker, gap_trading_days=gap, calendar=self.calendar)
                    if success:
                        meta["synced"] = True
                        meta["error"] = None
                        logger.info(f"[{name}] Sync successful: {msg}")
                        synced_in_this_pass.append(name)
                    else:
                        meta["synced"] = False
                        meta["error"] = msg
                        logger.warning(f"[{name}] Sync incomplete: {msg}")

                except Exception as e:
                    meta["synced"] = False
                    meta["error"] = f"Exception: {str(e)}"
                    logger.error(f"Fault isolation caught error for [{name}]: {e}", exc_info=True)
        finally:
            self.sync_lock.release()

        return synced_in_this_pass

    def execute_live_updates(self):
        """Runs CHILD-2 intraday candle updater for synchronized stocks."""
        for name, meta in self.script_status.items():
            if not _keep_running or not STATE_BUS.is_power_on():
                break

            if not meta["synced"]:
                logger.debug(f"[CHILD-2] Skipping unsynced stock: {name}")
                continue

            try:
                update_live_script(name, meta["ticker"])
            except Exception as e:
                logger.error(f"[CHILD-2] Live tick failed for [{name}]: {e}", exc_info=True)

    def log_detailed_sync_audit(self):
        synced = [k for k, v in self.script_status.items() if v.get("synced")]
        unsynced = [k for k, v in self.script_status.items() if not v.get("synced")]

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

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE")
        logger.info("==================================================")

        try:
            self.replan_daily_routine()
        except Exception as e:
            logger.error(f"[STARTUP] Initial plan error: {e}", exc_info=True)

        def boot_worker():
            logger.info("[STARTUP] Running initial historical sync...")
            try:
                synced_scripts = self.execute_historical_sync(is_manual=False)
                if synced_scripts:
                    logger.info(f"[STARTUP] Calculating technical parameters for {len(synced_scripts)} synced stocks...")
                    update_all_parameters(synced_scripts)
                    self.last_param_calc_time = time.time()
                    logger.info("[STARTUP] Initial parameters calculated successfully.")
            except Exception as e:
                logger.error(f"[STARTUP] Boot worker error: {e}", exc_info=True)

        threading.Thread(target=boot_worker, daemon=True).start()

        while _keep_running:
            try:
                if not STATE_BUS.is_power_on():
                    time.sleep(HEARTBEAT_TICK_SEC)
                    continue

                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # Priority 0: Difference Detection (Watchlist vs Stocklist)
                try:
                    was_updated, active_map = reconcile_stocklist_with_watchlist()
                    if was_updated:
                        logger.info("[HEARTBEAT] Watchlist change detected. Reconciling...")
                        old_scripts = set(self.script_status.keys())
                        current_scripts = set(active_map.keys())
                        newly_added_scripts = list(current_scripts - old_scripts)

                        self.purge_all_garbage(current_scripts)
                        self.replan_daily_routine()

                        # If new scripts were added, sync their OHLC and immediately compute parameters
                        if newly_added_scripts:
                            logger.info(f"[RECONCILE] Discovered {len(newly_added_scripts)} newly added scripts: {newly_added_scripts}")
                            synced_new = self.execute_historical_sync(is_manual=True, target_scripts=newly_added_scripts)
                            
                            if synced_new:
                                logger.info(f"[PARAM ENGINE] Immediately computing technical parameters for newly added scripts: {synced_new}")
                                update_all_parameters(synced_new)
                                logger.info(f"[PARAM ENGINE] Successfully populated /param for {synced_new}.")
                        else:
                            # Standard historical catch-up
                            self.execute_historical_sync(is_manual=False)
                except Exception as e:
                    logger.error(f"[HEARTBEAT] Watchlist reconciliation error: {e}")

                # Priority 1: External Manual Sync Pulse Override
                if STATE_BUS.check_and_clear_manual_sync():
                    logger.info("[OVERRIDE PULSE] Immediate sync commanded. Running CHILD-1...")
                    synced_all = self.execute_historical_sync(is_manual=True)
                    if synced_all:
                        logger.info(f"[PARAM ENGINE] Updating parameters post-manual sync for: {synced_all}")
                        update_all_parameters(synced_all)
                    continue

                # Priority 2: Midnight / State-Driven Date Catch-Up
                if self.last_planned_date != today_date:
                    self.replan_daily_routine()

                if not self.is_today_trading_day:
                    time.sleep(HEARTBEAT_TICK_SEC * 5)
                    continue

                # Priority 3: Pre-Market Historical Sync Window (08:00 – 08:30 IST)
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s["synced"] for s in self.script_status.values())
                    if has_unsynced and (time.time() - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync window active. Retrying unsynced stocks...")
                        synced_retries = self.execute_historical_sync(is_manual=False)
                        if synced_retries:
                            update_all_parameters(synced_retries)
                        self.last_sync_attempt_time = time.time()

                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # Priority 4: Live Market Processing (09:15 – 15:30 IST)
                self.calendar.refresh_calendar()
                status, _ = self.calendar.get_market_status()

                if status == "LIVE":
                    # 5-minute live updates for Index 0
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = time.time()

                    # 15-minute indicator recalculations
                    if (time.time() - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        active_synced = [name for name, meta in self.script_status.items() if meta["synced"]]
                        if active_synced:
                            logger.info(f"[PARAM ENGINE] Executing regular 15-minute calculation cycle across {len(active_synced)} stocks...")
                            update_all_parameters(active_synced)
                        self.last_param_calc_time = time.time()

                # Priority 5: Post-Market Final Closing Sweep (15:30 IST)
                if (
                    self.is_today_trading_day
                    and now_time >= dt_time(15, 30)
                    and now_time < dt_time(15, 45)
                    and not self.final_param_calculated_today
                ):
                    active_synced = [name for name, meta in self.script_status.items() if meta["synced"]]
                    if active_synced:
                        logger.info("[MARKET CLOSE 15:30] Executing final closing live update to seal Index 0...")
                        try:
                            # 1. Update Index 0 with final 15:30 closing tick
                            self.execute_live_updates()
                            self.last_live_update_time = time.time()
                            logger.info("[MARKET CLOSE 15:30] Final Index 0 update complete.")

                            # 2. Immediately run parameters on finalized Index 0
                            logger.info(f"[MARKET CLOSE 15:30] Executing final parameter calculation across {len(active_synced)} stocks...")
                            update_all_parameters(active_synced)
                            
                            # 3. Latch complete for the day
                            self.final_param_calculated_today = True
                            self.last_param_calc_time = time.time()
                            logger.info("[MARKET CLOSE 15:30] Final parameters calculated and saved successfully.")
                        except Exception as e:
                            logger.error(f"[MARKET CLOSE 15:30] Error during closing sweep: {e}", exc_info=True)

                time.sleep(HEARTBEAT_TICK_SEC)

            except Exception as e:
                logger.critical(f"Unhandled exception in master loop: {e}", exc_info=True)
                time.sleep(5)

        logger.info("Master orchestrator stopped cleanly.")


# =====================================================================
# SCRIPT ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    start_http_listener()
    orchestrator = MasterOrchestrator()
    orchestrator.run()