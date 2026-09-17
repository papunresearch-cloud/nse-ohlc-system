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
    FIXED_INDICES,
    PATH_STOCKS,
    PATH_SCRIPTS,
    LIVE_UPDATE_INTERVAL_SEC,
    PULSE_VALIDITY_SEC,
    logger
)
from firebase_manager import (
    init_firebase,
    sanitize_key,
    reconcile_stocklist_with_watchlist,
    get_stocklist_mapping,
    get_historical_stock_data,
    update_live_candle,
    write_historical_stock_data
)
import firebase_admin
from firebase_admin import db
from yahoo_manager import (
    download_historical_data,
    download_intraday_data,
    download_all_indices_intraday
)

try:
    from parameter import update_all_parameters
except ImportError:
    update_all_parameters = None

try:
    from RUN_PIPELINE import main as run_screener_pipeline
except ImportError:
    run_screener_pipeline = None

IST = pytz.timezone(TIMEZONE)
_keep_running = True

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower()

        if path in ["/", "/health"]:
            self.send_response(200)
            self._send_cors_headers()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            status = {
                "status": "RUNNING" if STATE_BUS.is_power_on() else "IDLE",
                "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            }
            self.wfile.write(json.dumps(status).encode("utf-8"))
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.lower()

        if path == "/start":
            STATE_BUS.trigger_pulse("START")
            msg = {"message": "Master engine started"}
        elif path == "/stop":
            STATE_BUS.trigger_pulse("STOP")
            msg = {"message": "Master engine paused"}
        elif path == "/sync":
            STATE_BUS.trigger_pulse("SYNC")
            msg = {"message": "Sync pulse triggered"}
        elif path == "/sync-screener":
            if run_screener_pipeline:
                threading.Thread(target=run_screener_pipeline, daemon=True).start()
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

def start_http_listener(port=10000):
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
        self.script_status = {}
        self.last_stock_event_ts = 0.0
        self.replan_daily_routine()

    def replan_daily_routine(self):
        """Builds target dictionary keyed exclusively by primary key CODE."""
        _, stock_map = reconcile_stocklist_with_watchlist()
        
        # Include fixed master market indices
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
            # Replan targets to discover the new CODE
            self.replan_daily_routine()
            # Immediately download historical bars and seed live row
            threading.Thread(
                target=self.sync_single_stock_history,
                args=(safe_code, ticker or f"{safe_code}.NS"),
                daemon=True,
                name=f"SyncAdd-{safe_code}"
            ).start()

        elif action == "DELETE":
            # Admin cascade purge using primary key CODE
            try:
                db.reference(f"{PATH_STOCKS}/{safe_code}").delete()
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

    def sync_single_stock_history(self, safe_code: str, ticker: str):
        """Fetches 300 daily bars for a newly enrolled stock and writes under /stocks/<CODE>."""
        logger.info(f"[ON-DEMAND SYNC] Starting historical sync for CODE: {safe_code} ({ticker})...")
        try:
            df = download_historical_data(ticker, period="2y")
            if df is not None and not df.empty:
                write_historical_stock_data(safe_code, df)
                logger.info(f"[ON-DEMAND SYNC] Successfully seeded 300 bars under /stocks/{safe_code}")
                if update_all_parameters:
                    update_all_parameters()
            else:
                logger.warning(f"[ON-DEMAND SYNC] No historical bars returned for {ticker}")
        except Exception as e:
            logger.error(f"[ON-DEMAND SYNC] Failed syncing {safe_code}: {e}", exc_info=True)

    def execute_live_intraday_cycle(self):
        """Updates live candles and technical indicators for all active stocks."""
        for safe_code, info in list(self.script_status.items()):
            ticker = info.get("ticker", f"{safe_code}.NS")
            try:
                live_candle = download_intraday_data(ticker)
                if live_candle:
                    update_live_candle(safe_code, live_candle)
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

            # 3. Intraday tick cycle (every 60 seconds if power latched ON)
            if STATE_BUS.is_power_on() and (now - last_intraday_tick >= LIVE_UPDATE_INTERVAL_SEC):
                self.execute_live_intraday_cycle()
                last_intraday_tick = now

            time.sleep(1.0)

        logger.info("[ORCHESTRATOR] Master engine stopped cleanly.")

if __name__ == "__main__":
    start_http_listener(port=10000)
    orchestrator = MasterOrchestrator()
    orchestrator.run()