"""
MASTER / MOTHER PROGRAM
Orchestrates the continuous maintenance of the 250-candle OHLC database.

Key Responsibilities:
- Binds to Render's $PORT using a lightweight background HTTP server.
- Interrogates the market status (LIVE / CLOSED / HOLIDAY / MUHURAT).
- Polls the script list dynamically from Firebase (/scripts/).
- Enforces Child-1 (Sync) priority over Child-2 (Live Updates).
- Isolates errors per script so one failure does not halt the system.
- Operates statelessly with immediate recovery on server restarts.
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
    HIST_SYNC_INTERVAL_SEC,
    MARKET_CHECK_INTERVAL_SEC,
    TARGET_OHLC_COUNT,
    logger
)
from firebase_manager import init_firebase, get_scripts_list, get_stock_ohlc
from market_calendar import MarketCalendar
from sync_child import sync_historical_script
from live_child import update_live_script
from yahoo_manager import get_latest_available_trading_date

IST = pytz.timezone(TIMEZONE)
_keep_running = True


# =====================================================================
# RENDER CLOUD COMPATIBILITY: BACKGROUND HTTP HEALTH SERVER
# =====================================================================
class RenderHealthCheckHandler(BaseHTTPRequestHandler):
    """Responds with 200 OK to satisfy Render's Web Service port binder & health monitors."""
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"NSE Equity OHLC Maintenance Service is Active.\n")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()

    def log_message(self, format, *args):
        # Suppress HTTP access lines from cluttering orchestrator logs
        return


def start_render_http_server():
    """Starts an HTTP server on the PORT assigned by Render in a background daemon thread."""
    port_str = os.environ.get("PORT", "10000")
    try:
        port = int(port_str)
        server = HTTPServer(("0.0.0.0", port), RenderHealthCheckHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        logger.info(f"Render cloud health check bound to 0.0.0.0:{port}")
    except Exception as e:
        logger.warning(f"Could not start Render health server on port {port_str}: {e}")


# =====================================================================
# SIGNAL HANDLING
# =====================================================================
def handle_shutdown(signum, frame):
    global _keep_running
    logger.info(f"Received termination signal ({signum}). Gracefully shutting down...")
    _keep_running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# =====================================================================
# ORCHESTRATION ENGINE
# =====================================================================
class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.calendar = MarketCalendar()
        self.last_hist_sync_time = 0.0
        self.last_live_update_time = 0.0

    def run(self):
        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE MASTER START")
        logger.info("==================================================")

        # Execute an initial historical sync check immediately upon boot/restart
        logger.info("Executing startup check on all registered scripts...")
        self.orchestrate_cycle(force_hist_check=True, market_is_live=False)
        self.last_hist_sync_time = time.time()

        while _keep_running:
            try:
                now_ts = time.time()
                
                # Dynamically re-read calendar configurations/overrides from Firebase
                self.calendar.refresh_calendar()
                status, reason = self.calendar.get_market_status()
                is_live = (status == "LIVE")

                logger.info(f"Market Status: [{status}] - {reason}")

                # Determine which operations are due
                hist_due = (now_ts - self.last_hist_sync_time) >= HIST_SYNC_INTERVAL_SEC
                live_due = is_live and ((now_ts - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC)

                if hist_due or live_due:
                    self.orchestrate_cycle(force_hist_check=hist_due, market_is_live=is_live)
                    
                    if hist_due:
                        self.last_hist_sync_time = now_ts
                    if live_due:
                        self.last_live_update_time = now_ts

                # Responsive sleeping: Wake up every 1 sec to capture SIGINT/SIGTERM quickly
                for _ in range(MARKET_CHECK_INTERVAL_SEC):
                    if not _keep_running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.critical(f"Unhandled exception in Master loop: {e}", exc_info=True)
                time.sleep(10)

        logger.info("Master orchestrator stopped safely.")

    def orchestrate_cycle(self, force_hist_check: bool = False, market_is_live: bool = False):
        """Fetches the external script list and processes each stock sequentially."""
        scripts = get_scripts_list()
        logger.info(f"SCRIPT LIST READ: {len(scripts)} scripts found in Firebase /scripts/")

        if not scripts:
            logger.warning("Firebase /scripts/ list is currently empty. Standing by...")
            return

        for script in scripts:
            if not _keep_running:
                break
            try:
                self.process_script(script, force_hist_check, market_is_live)
            except Exception as e:
                # Failure Isolation: An error on one script must never crash the loop
                logger.error(f"Fault isolation caught exception for script [{script}]: {e}", exc_info=True)

    def process_script(self, script: str, check_history: bool, market_is_live: bool):
        """
        State logic and synchronization routing for an individual script.
        Enforces: CHILD-1 (Historical Sync) > CHILD-2 (Live Update)
        """
        existing_ohlc = get_stock_ohlc(script)

        # 1. State: NO_DATABASE (New script detected or missing database)
        if not existing_ohlc:
            logger.info(f"[{script}] State: NO_DATABASE. Invoking CHILD-1 (Historical Sync)...")
            sync_historical_script(script, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
            return

        # 2. Check for missing historical trading days if periodic sync check is due
        sync_required = False
        gap = 0

        if check_history:
            # Extract Index 0
            idx0 = None
            if isinstance(existing_ohlc, dict):
                idx0 = existing_ohlc.get("0")
            elif isinstance(existing_ohlc, list) and len(existing_ohlc) > 0:
                idx0 = existing_ohlc[0]

            if not idx0 or "date" not in idx0:
                logger.warning(f"[{script}] Malformed or missing index 0. Invoking CHILD-1...")
                sync_historical_script(script, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
                return

            try:
                latest_fb_date = datetime.strptime(str(idx0["date"]), "%Y-%m-%d").date()
            except ValueError:
                logger.error(f"[{script}] Invalid date format in index 0 ('{idx0.get('date')}'). Invoking CHILD-1...")
                sync_historical_script(script, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
                return

            # Determine Yahoo Finance's latest available trading date
            latest_yahoo_date = get_latest_available_trading_date(script)

            if latest_yahoo_date:
                gap = self.calendar.get_trading_day_gap(latest_fb_date, latest_yahoo_date)
                if gap > 0:
                    logger.info(
                        f"[{script}] SYNC_REQUIRED: Firebase Date={latest_fb_date}, "
                        f"Yahoo Date={latest_yahoo_date}, Missing Trading Days={gap}"
                    )
                    sync_required = True
                else:
                    logger.debug(f"[{script}] SYNCHRONIZED (Latest Date: {latest_fb_date})")

        # 3. Priority Step: CHILD-1 takes precedence over live updating
        if sync_required:
            sync_historical_script(script, gap_trading_days=gap, calendar=self.calendar)
            return

        # 4. Secondary Step: CHILD-2 (Only if synchronized and market is LIVE)
        if market_is_live:
            update_live_script(script)


# =====================================================================
# PROGRAM ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    # Start the HTTP server to satisfy Render's port detection
    start_render_http_server()

    # Launch orchestrator
    orchestrator = MasterOrchestrator()
    orchestrator.run()