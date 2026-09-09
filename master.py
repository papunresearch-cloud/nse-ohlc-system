"""
MASTER ORCHESTRATOR
- Manages pre-market sync, live CHILD-2 updates, and technical parameter routines.
- Includes HTTP health-check server on $PORT for Render deployment.
- Triggers an isolated parameter calculation immediately at 15:30 IST market close.
"""
import os
import sys
import time
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, time as dt_time
import pytz

from config import (
    TIMEZONE,
    PORT,
    PARAM_CALC_INTERVAL_SEC,
    logger
)
from market_calendar import MarketCalendar
from sync_child import sync_historical_script
from parameter import update_all_parameters
from firebase_manager import db

IST = pytz.timezone(TIMEZONE)
_keep_running = True


# =====================================================================
# HTTP HEALTH-CHECK SERVER (FOR CLOUD DEPLOYMENTS)
# =====================================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "healthy", "service": "NSE Master Orchestrator"}')

    def log_message(self, format, *args):
        # Suppress standard ping spam in Render application logs
        pass


def start_health_server(port: int = PORT):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    logger.info(f"[HTTP] Command server listening on port {port}")
    return server


# =====================================================================
# MASTER ORCHESTRATOR CLASS
# =====================================================================

class MasterOrchestrator:
    def __init__(self):
        self.calendar = MarketCalendar()
        self.script_status = {}
        self.active_stocks = []
        self.last_param_calc_time = 0
        self.final_param_calculated_today = False
        self.current_plan_date = None

    def replan_daily_routine(self, now_ist: datetime):
        """Initializes or resets the daily operational plan at midnight IST."""
        today = now_ist.date()
        is_trading = self.calendar.is_trading_day(today)
        self.current_plan_date = today
        self.final_param_calculated_today = False  # Reset daily closing latch

        self.load_watchlist_from_firebase()
        plan_desc = f"TRADING SESSION ({len(self.active_stocks)} stocks)" if is_trading else "MARKET HOLIDAY / WEEKEND"
        logger.info(f"[PLANNER] Day plan for {today} IST initialized: {plan_desc}")

    def load_watchlist_from_firebase(self):
        """Fetches active stocks list from the watchlist node in Firebase."""
        try:
            wl_ref = db.reference("watchlist")
            wl_data = wl_ref.get() or {}

            raw_names = []
            if isinstance(wl_data, dict) and "watchlist" in wl_data:
                raw_names = wl_data.get("watchlist", [])
            elif isinstance(wl_data, dict):
                raw_names = list(wl_data.values())
            elif isinstance(wl_data, list):
                raw_names = wl_data

            self.active_stocks = [str(n).strip() for n in raw_names if n]
            logger.info(f"[WATCHLIST] Loaded {len(self.active_stocks)} target stocks from Firebase.")
        except Exception as e:
            logger.error(f"[WATCHLIST] Error fetching watchlist: {e}")

    def run_sync_cycle(self):
        """Executes historical synchronization across all watchlist equities."""
        logger.info("=====================================================================================")
        logger.info("                   SCRIPT-WISE SYNCHRONIZATION AUDIT REPORT                          ")
        logger.info("=====================================================================================")

        total = len(self.active_stocks)
        synced_count = 0

        for stock_name in self.active_stocks:
            # Suffix mapping: Assumes standard .NS equity unless already formatted
            ticker = stock_name if ("." in stock_name or "^" in stock_name) else f"{stock_name}.NS"

            success, msg = sync_historical_script(stock_name, ticker, calendar=self.calendar)
            self.script_status[stock_name] = {
                "synced": success,
                "msg": msg,
                "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
            }

            if success:
                synced_count += 1
                logger.info(f"[{stock_name}] Sync successful: {msg}")
            else:
                logger.error(f"[{stock_name}] Sync failed: {msg}")

        failed_count = total - synced_count
        logger.info(f"Total: {total} | Synced: {synced_count} | Failed/Unsynced: {failed_count}")

        if failed_count > 0:
            logger.error("[ISOLATED / UNSYNCHRONIZED SCRIPTS - DATABASE SYNCH ERROR]")
            for stock_name, meta in self.script_status.items():
                if not meta["synced"]:
                    logger.error(f"  ✗ {stock_name:<25} | Last Attempt: {meta['timestamp']} | Error: {meta['msg']}")
            logger.critical(f"Database Synch Error: {failed_count} stock(s) failed validation.")
        
        logger.info("=====================================================================================")

    def get_synced_stocks(self) -> list[str]:
        """Returns list of all stocks that have successfully passed historical synchronization."""
        return [name for name, meta in self.script_status.items() if meta.get("synced", False)]

    def run(self):
        """Main operational state loop."""
        global _keep_running

        logger.info("==================================================")
        logger.info("NSE EQUITY OHLC DATABASE MAINTENANCE ACTIVE       ")
        logger.info("==================================================")

        now_ist = datetime.now(IST)
        self.replan_daily_routine(now_ist)

        # Initial Boot Sync
        logger.info("[STARTUP] Running initial historical sync...")
        self.run_sync_cycle()

        synced = self.get_synced_stocks()
        if synced:
            logger.info(f"[STARTUP] Calculating technical parameters for {len(synced)} synced stocks...")
            update_all_parameters(synced)
            self.last_param_calc_time = time.time()
            logger.info("[STARTUP] Initial parameters calculated successfully.")

        while _keep_running:
            try:
                now_ist = datetime.now(IST)
                today_date = now_ist.date()
                now_time = now_ist.time()

                # 1. Midnight Daily Re-plan Check
                if self.current_plan_date != today_date:
                    self.replan_daily_routine(now_ist)
                    # Run pre-market catchup sync
                    self.run_sync_cycle()

                is_trading = self.calendar.is_trading_day(today_date)
                market_status, _ = self.calendar.get_market_status(now_ist)

                # -------------------------------------------------------------
                # 2. INTRADAY PARAMETER CALCULATION (Every 15 mins during LIVE)
                # -------------------------------------------------------------
                if is_trading and market_status == "LIVE":
                    current_ts = time.time()
                    if current_ts - self.last_param_calc_time >= PARAM_CALC_INTERVAL_SEC:
                        synced_stocks = self.get_synced_stocks()
                        if synced_stocks:
                            logger.info(f"[PARAM ENGINE] Executing regular 15-minute calculation cycle across {len(synced_stocks)} stocks...")
                            update_all_parameters(synced_stocks)
                            self.last_param_calc_time = current_ts

                # -------------------------------------------------------------
                # 3. POST-MARKET PARAMETER CALCULATION (15:30 IST CLOSING SNAPSHOT)
                # -------------------------------------------------------------
                if (
                    is_trading
                    and now_time >= dt_time(15, 30)
                    and now_time < dt_time(15, 45)
                    and not self.final_param_calculated_today
                ):
                    synced_stocks = self.get_synced_stocks()
                    if synced_stocks:
                        logger.info(
                            f"[MARKET CLOSE 15:30] Market closed. Executing final post-closing "
                            f"parameter calculation across {len(synced_stocks)} stocks..."
                        )
                        try:
                            update_all_parameters(synced_stocks)
                            self.final_param_calculated_today = True
                            self.last_param_calc_time = time.time()
                            logger.info("[MARKET CLOSE 15:30] Final parameters calculated successfully.")
                        except Exception as e:
                            logger.error(f"[MARKET CLOSE 15:30] Error during final parameter calculation: {e}", exc_info=True)

                # Idle sleep between evaluation cycles
                time.sleep(10)

            except Exception as e:
                logger.error(f"[MASTER LOOP] Unexpected exception: {e}", exc_info=True)
                time.sleep(15)


# =====================================================================
# TERMINATION & ENTRYPOINT
# =====================================================================

def handle_shutdown(signum, frame):
    global _keep_running
    logger.info(f"Received termination signal ({signum}). Shutting down gracefully...")
    _keep_running = False


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # Initialize health server for Render
    server = start_health_server(PORT)

    orchestrator = MasterOrchestrator()
    try:
        orchestrator.run()
    finally:
        logger.info("Master orchestrator stopped cleanly.")
        sys.exit(0)