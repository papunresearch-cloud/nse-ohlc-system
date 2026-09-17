"""
===============================================================================
MASTER ORCHESTRATOR (Primary Key Architecture: CODE)
===============================================================================
* Coordinates live OHLC backfills, indicators, and HTTP control signals.
* Keyed exclusively by stock CODE across /stocks/<CODE> and /param/<CODE>.
* Listens to /system_commands/stock_event dispatched from Watchlist.jsx.
* Embeds HTTP server on port 10000 with CORS and /sync-screener ETL integration.
"""

import os
import sys
import time
import signal
import json
import logging
import threading
from urllib.parse import urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import pytz

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    TIMEZONE,
    PATH_STOCKS,
    PATH_SCRIPTS,
    LIVE_UPDATE_INTERVAL_SEC,
    PULSE_VALIDITY_SEC,
    logger
)

# FIXED_INDICES is defined in firebase_manager.py
try:
    from firebase_manager import FIXED_INDICES
except ImportError:
    FIXED_INDICES = {
        "NIFTY50": "^NSEI",
        "NIFTY100": "^CNX100",
        "NIFTY MIDCAP 150": "^CRSLDX",
        "NIFTY SMALLCAP 250": "^CNXSC"
    }

from firebase_manager import (
    init_firebase,
    sanitize_key,
    reconcile_stocklist_with_watchlist,
    get_stocklist_mapping
)
import firebase_admin
from firebase_admin import db

# Calendar and Child Workers
from market_calendar import MarketCalendar
from sync_child import sync_historical_script
from live_child import update_live_script

# Parameter calculation engine
try:
    from parameter import update_all_parameters, calculate_single_script_parameters
except ImportError:
    try:
        from parameter import update_all_parameters
        calculate_single_script_parameters = None
    except ImportError:
        update_all_parameters = None
        calculate_single_script_parameters = None

# Screener Pipeline ETL runner
try:
    from RUN_PIPELINE import main as run_screener_pipeline
except ImportError:
    try:
        from BASIC import run_pipeline as run_screener_pipeline
    except ImportError:
        run_screener_pipeline = None

IST = pytz.timezone(TIMEZONE)
_keep_running = True
_screener_lock = threading.Lock()

def handle_exit_signal(sig, frame):
    global _keep_running
    logger.info("[SHUTDOWN] Terminating orchestrator cleanly...")
    _keep_running = False

signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

# =====================================================================
# SYSTEM FLIP-FLOP & PULSE BUS
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
                logger.info("[SIGNAL] START pulse captured -> Latch ON.")
            elif command == "STOP":
                self.power_latched_on = False
                logger.warning("[SIGNAL] STOP pulse captured -> Latch OFF.")
            elif command == "SYNC":
                self.pulse_sync_time = now
                logger.info("[SIGNAL] MANUAL SYNC pulse captured.")

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
# HTTP REQUEST HANDLER
# =====================================================================
class HealthAndControlHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower()

        if path in ["/", "/health", "/ping"]:
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            status = {
                "status": "RUNNING" if STATE_BUS.is_power_on() else "IDLE",
                "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            }
            self.wfile.write(json.dumps(status).encode("utf-8"))
        elif path in ["/calc-param", "/api/calc-param"]:
            if update_all_parameters:
                stock_map = get_stocklist_mapping(force_reconcile=True)
                threading.Thread(target=update_all_parameters, args=(list(stock_map.keys()),), daemon=True).start()
                msg = {"status": "started", "message": "Manual parameter calculation started."}
            else:
                msg = {"error": "parameter module not loaded"}
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(msg).encode("utf-8"))
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower()

        if path in ["/start", "/api/start"]:
            STATE_BUS.trigger_pulse("START")
            msg = {"message": "Master engine started"}
        elif path in ["/stop", "/api/stop"]:
            STATE_BUS.trigger_pulse("STOP")
            msg = {"message": "Master engine paused"}
        elif path in ["/sync", "/api/sync"]:
            STATE_BUS.trigger_pulse("SYNC")
            msg = {"message": "Sync pulse triggered"}
        elif path in ["/sync-screener", "/run-pipeline", "/api/sync-screener"]:
            if run_screener_pipeline:
                def run_screener_worker():
                    if _screener_lock.acquire(blocking=False):
                        try:
                            logger.info("[SCREENER] Starting Google Drive -> Firebase ETL...")
                            run_screener_pipeline()
                            logger.info("[SCREENER] Screener ETL completed successfully.")
                        except Exception as ex:
                            logger.error(f"[SCREENER] Error in pipeline: {ex}", exc_info=True)
                        finally:
                            _screener_lock.release()
                    else:
                        logger.warning("[SCREENER] Pipeline sync already in progress.")
                threading.Thread(target=run_screener_worker, daemon=True).start()
                msg = {"message": "Screener ETL pipeline triggered in background"}
            else:
                msg = {"error": "RUN_PIPELINE module not available"}
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()
            return

        self.send_response(200)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(msg).encode("utf-8"))

    def log_message(self, format, *args):
        return

def start_http_listener(port=10000):
    port = int(os.environ.get("PORT", port))
    server = HTTPServer(("0.0.0.0", port), HealthAndControlHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="HttpServer")
    t.start()
    logger.info(f"[HTTP] Control and Health server listening on port {port}")

# =====================================================================
# MASTER ORCHESTRATOR
# =====================================================================
class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.calendar = MarketCalendar()
        self.script_status = {}
        self.last_stock_event_ts = time.time() * 1000
        self.replan_daily_routine()

    def replan_daily_routine(self):
        """Builds target dictionary keyed exclusively by primary key CODE."""
        stock_map = get_stocklist_mapping(force_reconcile=True)
        
        # Merge fixed market indices
        for k, v in FIXED_INDICES.items():
            stock_map.setdefault(sanitize_key(k), v)

        self.script_status = {
            code: {
                "synced": False,
                "last_attempt_at": None,
                "error": None,
                "ticker": ticker
            }
            for code, ticker in stock_map.items()
        }
        logger.info(f"[PLANNER] Reconciled stock targets by CODE. Total targets: {len(self.script_status)}")

    def check_stock_event(self) -> tuple[bool, dict]:
        """Listens for realtime commands from Watchlist.jsx at /system_commands/stock_event."""
        try:
            event_ref = db.reference("system_commands/stock_event")
            payload = event_ref.get()
            if isinstance(payload, dict):
                ts = float(payload.get("timestamp", 0))
                if ts > self.last_stock_event_ts:
                    self.last_stock_event_ts = ts
                    return True, payload
        except Exception as e:
            logger.error(f"[EVENT LISTENER] Error checking stock_event node: {e}")
        return False, {}

    def handle_stock_event(self, event: dict):
        action = event.get("action")
        stock_code = event.get("stock")
        ticker = event.get("ticker")

        if not stock_code:
            return

        safe_code = sanitize_key(stock_code)
        logger.info(f"[STOCK EVENT] Received action '{action}' for CODE '{safe_code}'")

        if action == "ADD":
            self.replan_daily_routine()
            resolved_ticker = ticker or f"{safe_code}.NS"
            threading.Thread(
                target=self.sync_single_stock_addition,
                args=(safe_code, resolved_ticker),
                daemon=True,
                name=f"SyncAdd-{safe_code}"
            ).start()

        elif action == "DELETE":
            # Admin cascade purge using primary key CODE
            try:
                db.reference(f"stocks/{safe_code}").delete()
                db.reference(f"param/{safe_code}").delete()
                logger.info(f"[DELETE EVENT] Purged /stocks/{safe_code} and /param/{safe_code}")
            except Exception as e:
                logger.error(f"[DELETE EVENT] Error purging /stocks or /param: {e}")

            # Prune from /display_list/stocks
            try:
                display_ref = db.reference("display_list/stocks")
                current_display = display_ref.get()

                if isinstance(current_display, list):
                    filtered = [s for s in current_display if sanitize_key(str(s)) != safe_code]
                    display_ref.set(filtered if filtered else None)
                    logger.info(f"[DELETE EVENT] Removed {safe_code} from array /display_list/stocks")
                elif isinstance(current_display, dict):
                    db.reference(f"display_list/stocks/{safe_code}").delete()
                    logger.info(f"[DELETE EVENT] Removed {safe_code} from map /display_list/stocks")
            except Exception as e:
                logger.error(f"[DELETE EVENT] Error updating display_list: {e}")

            if safe_code in self.script_status:
                del self.script_status[safe_code]

    def sync_single_stock_addition(self, safe_code: str, ticker: str):
        """Seeds 300 bars, initializes live candle 0, and calculates indicators."""
        logger.info(f"[ON-DEMAND SYNC] Seeding 300 bars for CODE: {safe_code} ({ticker})...")
        try:
            success, msg = sync_historical_script(safe_code, ticker, gap_trading_days=300, calendar=self.calendar)
            if success:
                logger.info(f"[ON-DEMAND SYNC] Historical 300 bars seeded for {safe_code}: {msg}")
                try:
                    update_live_script(safe_code, ticker)
                except Exception as live_err:
                    logger.warning(f"[ON-DEMAND SYNC] Live candle init skipped for {safe_code}: {live_err}")

                if calculate_single_script_parameters:
                    try:
                        calculate_single_script_parameters(safe_code)
                        logger.info(f"[ON-DEMAND SYNC] /param/{safe_code} populated successfully.")
                    except Exception as param_err:
                        logger.error(f"[ON-DEMAND SYNC] Parameter calc error for {safe_code}: {param_err}")
                elif update_all_parameters:
                    update_all_parameters([safe_code])
            else:
                logger.error(f"[ON-DEMAND SYNC] Historical sync failed for {safe_code}: {msg}")
        except Exception as e:
            logger.error(f"[ON-DEMAND SYNC] Failed syncing {safe_code}: {e}", exc_info=True)

    def execute_live_intraday_cycle(self):
        """Updates live candles for all active stocks."""
        for safe_code, info in list(self.script_status.items()):
            ticker = info.get("ticker", f"{safe_code}.NS")
            try:
                update_live_script(safe_code, ticker)
            except Exception as e:
                logger.debug(f"[LIVE CYCLE] Live update skipped for {safe_code}: {e}")

    def run(self):
        logger.info("[ORCHESTRATOR] Master engine started. Primary key: CODE.")
        last_intraday_tick = 0.0

        while _keep_running:
            now = time.time()

            # 1. Listen for realtime actions from Watchlist.jsx
            has_event, event_payload = self.check_stock_event()
            if has_event:
                self.handle_stock_event(event_payload)

            # 2. Check for manual sync pulse via HTTP (/sync)
            if STATE_BUS.check_and_clear_manual_sync():
                self.replan_daily_routine()

            # 3. Intraday tick cycle (every LIVE_UPDATE_INTERVAL_SEC if power latched ON)
            if STATE_BUS.is_power_on() and (now - last_intraday_tick >= LIVE_UPDATE_INTERVAL_SEC):
                self.execute_live_intraday_cycle()
                last_intraday_tick = now

            time.sleep(1.0)

        logger.info("[ORCHESTRATOR] Master engine stopped cleanly.")

if __name__ == "__main__":
    start_http_listener(port=10000)
    orchestrator = MasterOrchestrator()
    orchestrator.run()