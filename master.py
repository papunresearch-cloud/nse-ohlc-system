"""
MASTER / MOTHER PROGRAM
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
from firebase_manager import init_firebase, get_stocklist_mapping, get_stock_ohlc
from market_calendar import MarketCalendar
from sync_child import sync_historical_script
from live_child import update_live_script
from yahoo_manager import get_latest_available_trading_date

IST = pytz.timezone(TIMEZONE)
_keep_running = True


# =====================================================================
# RENDER CLOUD COMPATIBILITY
# =====================================================================
class RenderHealthCheckHandler(BaseHTTPRequestHandler):
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
        return


def start_render_http_server():
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
    logger.info(f"Received termination signal ({signum}). Shutting down...")
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

        # Run startup historical sync check
        logger.info("Executing startup check on all registered stocks...")
        self.orchestrate_cycle(force_hist_check=True, market_is_live=False)
        self.last_hist_sync_time = time.time()

        while _keep_running:
            try:
                now_ts = time.time()
                self.calendar.refresh_calendar()
                status, reason = self.calendar.get_market_status()
                is_live = (status == "LIVE")

                logger.info(f"Market Status: [{status}] - {reason}")

                hist_due = (now_ts - self.last_hist_sync_time) >= HIST_SYNC_INTERVAL_SEC
                live_due = is_live and ((now_ts - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC)

                if hist_due or live_due:
                    self.orchestrate_cycle(force_hist_check=hist_due, market_is_live=is_live)
                    if hist_due:
                        self.last_hist_sync_time = now_ts
                    if live_due:
                        self.last_live_update_time = now_ts

                for _ in range(MARKET_CHECK_INTERVAL_SEC):
                    if not _keep_running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.critical(f"Unhandled exception in Master loop: {e}", exc_info=True)
                time.sleep(10)

        logger.info("Master orchestrator stopped safely.")

    def orchestrate_cycle(self, force_hist_check: bool = False, market_is_live: bool = False):
        stock_map = get_stocklist_mapping()
        logger.info(f"STOCKLIST READ: {len(stock_map)} stocks found in Firebase /stocklist/")

        if not stock_map:
            logger.warning("Firebase /stocklist/ is empty. Standing by...")
            return

        for display_name, ticker in stock_map.items():
            if not _keep_running:
                break
            try:
                self.process_stock(display_name, ticker, force_hist_check, market_is_live)
            except Exception as e:
                logger.error(f"Fault isolation caught exception for [{display_name} ({ticker})]: {e}", exc_info=True)

    def process_stock(self, display_name: str, ticker: str, check_history: bool, market_is_live: bool):
        existing_ohlc = get_stock_ohlc(display_name)

        # 1. State: NO_DATABASE
        if not existing_ohlc:
            logger.info(f"[{display_name}] State: NO_DATABASE. Invoking CHILD-1...")
            sync_historical_script(display_name, ticker, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
            return

        # 2. Check for missing trading days
        sync_required = False
        gap = 0

        if check_history:
            idx0 = None
            if isinstance(existing_ohlc, dict):
                idx0 = existing_ohlc.get("0")
            elif isinstance(existing_ohlc, list) and len(existing_ohlc) > 0:
                idx0 = existing_ohlc[0]

            if not idx0 or "date" not in idx0:
                logger.warning(f"[{display_name}] Malformed index 0. Invoking CHILD-1...")
                sync_historical_script(display_name, ticker, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
                return

            try:
                latest_fb_date = datetime.strptime(str(idx0["date"]), "%Y-%m-%d").date()
            except ValueError:
                logger.error(f"[{display_name}] Invalid date format in index 0. Invoking CHILD-1...")
                sync_historical_script(display_name, ticker, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
                return

            latest_yahoo_date = get_latest_available_trading_date(ticker)
            if latest_yahoo_date:
                gap = self.calendar.get_trading_day_gap(latest_fb_date, latest_yahoo_date)
                if gap > 0:
                    logger.info(
                        f"[{display_name}] SYNC_REQUIRED: Firebase={latest_fb_date}, "
                        f"Yahoo={latest_yahoo_date}, Missing={gap} trading days"
                    )
                    sync_required = True
                else:
                    logger.debug(f"[{display_name}] SYNCHRONIZED ({latest_fb_date})")

        # 3. CHILD-1 Priority
        if sync_required:
            sync_historical_script(display_name, ticker, gap_trading_days=gap, calendar=self.calendar)
            return

        # 4. CHILD-2 Live update (only if market LIVE)
        if market_is_live:
            update_live_script(display_name, ticker)


if __name__ == "__main__":
    start_render_http_server()
    orchestrator = MasterOrchestrator()
    orchestrator.run()