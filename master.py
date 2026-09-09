"""
MASTER ORCHESTRATOR
- Priority 0: Watchlist is absolute master. Stocklist is continuously synchronized.
- Never mutates or deletes from /watchlist or /watchlist/detailedDb.
- Automatic Garbage Purging: Cleans orphaned entries from /stocks and /param immediately
  as soon as stocklist is reconciled with watchlist.
- Guarded OHLC & Param protection: Stocks residing in /watchlist are never touched.
- SR Flip-Flop Power Latch (Default: ON).
- Handles external 30-second pulse commands: /start, /stop, /sync.
- Embedded HTTP Server with minimal /health, GET, and HEAD handling for cron-job.org.
- State-driven date planning (auto-adjusts if restarted or offline at midnight).
- Pre-market sync window (08:00–08:30 IST) with 5-minute retry intervals.
- Index 0 persistent: Cleared during pre/post-market only if specified; never destroyed.
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
# SYSTEM STATE BUS (SR FLIP-FLOP & PULSE COMMANDS)
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
# EMBEDDED HTTP COMMAND LISTENER (RENDER & CRON-JOB COMPATIBLE)
# =====================================================================
class PulseCommandHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.strip().lower()
        if path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK\n")
            return

        if path == "/start":
            STATE_BUS.trigger_pulse("START")
            msg = "System Latched ON\n"
        elif path == "/stop":
            STATE_BUS.trigger_pulse("STOP")
            msg = "System Latched OFF\n"
        elif path == "/sync":
            STATE_BUS.trigger_pulse("SYNC")
            msg = "Manual Sync Triggered\n"
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Endpoint not found. Use /start, /stop, /sync, or /health\n")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(msg.encode("utf-8"))

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

    def log_message(self, format, *args):
        return


def start_http_listener():
    port_str = os.environ.get("PORT", "10000")
    try:
        port = int(port_str)
        server = HTTPServer(("0.0.0.0", port), PulseCommandHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        logger.info(f"[HTTP] Command server listening on port {port}")
    except Exception as e:
        logger.warning(f"[HTTP] Failed to start HTTP server on port {port_str}: {e}")


# =====================================================================
# SHUTDOWN SIGNAL HANDLING
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

        self.last_sync_attempt_time = 0.0
        self.last_live_update_time = 0.0
        self.last_param_calc_time = 0.0

        self.sync_lock = threading.Lock()
        self.script_status = {}

    def _purge_stocks_garbage(self, active_stock_set: set):
        """Purges orphaned keys in /stocks that no longer exist in watchlist."""
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
        """Purges orphaned keys in /param that no longer exist in watchlist."""
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
        """Coordinates instant dual-node cleanup across /stocks and /param."""
        if not active_stock_set:
            logger.warning("[SAFETY LOCK] Active stock set is empty. Skipping purge to prevent data loss.")
            return
        self._purge_stocks_garbage(active_stock_set)
        self._purge_param_garbage(active_stock_set)

    def replan_daily_routine(self):
        """Synchronizes stocklist with watchlist, executes garbage purges, and prepares status."""
        now_ist = datetime.now(IST)
        today = now_ist.date()
        self.calendar.refresh_calendar()

        self.is_today_trading_day = self.calendar.is_trading_day(today)
        self.last_planned_date = today
        self.sync_audit_reported_today = False

        # Reconcile stocklist using watchlist as absolute master
        _, active_map = reconcile_stocklist_with_watchlist()
        if not active_map:
            logger.warning("[PLANNER] Watchlist reconciliation returned 0 active stocks.")
        else:
            # Immediate Garbage Collection for orphaned stocks
            self.purge_all_garbage(set(active_map.keys()))

        # Rebuild per-script tracking status
        new_status = {}
        for name, ticker in active_map.items():
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
        """Determines missing historical trading sessions."""
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

    def execute_historical_sync(self, is_manual: bool = False):
        """Runs CHILD-1 historical sync under mutex lock to avoid duplicate workers."""
        if not self.sync_lock.acquire(blocking=False):
            logger.info("[CHILD-1] Historical sync already in progress. Skipping duplicate execution.")
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
                    logger.error(f"Fault isolation caught error for [{name}]: {e}", exc_info=True)
        finally:
            self.sync_lock.release()

    def execute_live_updates(self):
        """Runs CHILD-2 intraday candle updater for synchronized stocks."""
        for name, meta in self.script_status.items():
            if not _keep_running:
                break
            if not STATE_BUS.is_power_on():
                break

            if not meta["synced"]:
                logger.debug(f"[CHILD-2] Skipping unsynced stock: {name}")
                continue

            try:
                update_live_script(name, meta["ticker"])
            except Exception as e:
                logger.error(f"[CHILD-2] Live tick failed for [{name}]: {e}", exc_info=True)

    def log_detailed_sync_audit(self):
        """Outputs pre-market verification results."""
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

        # 1. Initial boot planning and baseline stocklist reconciliation
        try:
            self.replan_daily_routine()
        except Exception as e:
            logger.error(f"[STARTUP] Initial plan error: {e}", exc_info=True)

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
                logger.error(f"[STARTUP] Boot worker error: {e}", exc_info=True)

        threading.Thread(target=boot_worker, daemon=True).start()

        # 3. Main Operational Dispatch Loop
        while _keep_running:
            try:
                if not STATE_BUS.is_power_on():
                    time.sleep(HEARTBEAT_TICK_SEC)
                    continue

                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # Priority 0: Difference Detection (Watchlist vs Stocklist) & Instant Sweep
                try:
                    was_updated, active_map = reconcile_stocklist_with_watchlist()
                    if was_updated:
                        logger.info("[HEARTBEAT] Watchlist change detected. Purging garbage & reconciling...")
                        # Purge both /stocks and /param immediately
                        self.purge_all_garbage(set(active_map.keys()))
                        self.replan_daily_routine()
                        self.execute_historical_sync(is_manual=False)
                except Exception as e:
                    logger.error(f"[HEARTBEAT] Watchlist reconciliation error: {e}")

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

                # Priority 3: Pre-Market Historical Sync Window (08:00 – 08:30 IST)
                sync_start = datetime.strptime(f"{SYNC_WINDOW_START_HOUR}:{SYNC_WINDOW_START_MIN}", "%H:%M").time()
                sync_cutoff = datetime.strptime(f"{SYNC_WINDOW_DEADLINE_HOUR}:{SYNC_WINDOW_DEADLINE_MIN}", "%H:%M").time()

                if sync_start <= now_time < sync_cutoff:
                    has_unsynced = any(not s["synced"] for s in self.script_status.values())
                    if has_unsynced and (time.time() - self.last_sync_attempt_time >= SYNC_RETRY_INTERVAL_SEC):
                        logger.info("[SCHEDULE] Pre-market sync window active. Retrying unsynced stocks...")
                        self.execute_historical_sync(is_manual=False)
                        self.last_sync_attempt_time = time.time()

                # Audit Report at or after 08:30 IST
                if now_time >= sync_cutoff and not self.sync_audit_reported_today:
                    self.log_detailed_sync_audit()
                    self.sync_audit_reported_today = True

                # Priority 4: Live Market Processing (09:15 – 15:30 IST)
                self.calendar.refresh_calendar()
                status, _ = self.calendar.get_market_status()

                if status == "LIVE":
                    # 5-minute live ticks
                    if (time.time() - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC:
                        self.execute_live_updates()
                        self.last_live_update_time = time.time()

                    # 15-minute indicator recalculation
                    if (time.time() - self.last_param_calc_time) >= PARAM_UPDATE_INTERVAL_SEC:
                        active_synced = [name for name, meta in self.script_status.items() if meta["synced"]]
                        if active_synced:
                            update_all_parameters(active_synced)
                        self.last_param_calc_time = time.time()

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