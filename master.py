"""
MASTER / MOTHER PROGRAM
Coordinates orchestrations, manages synchronization schedules, determines live market states,
and enforces failure isolation across all NSE scripts.
"""
import time
import signal
import sys
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


def signal_handler(signum, frame):
    global _keep_running
    logger.info("Shutdown signal received. Finishing active operations gracefully...")
    _keep_running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.calendar = MarketCalendar()
        self.last_hist_sync_time = 0.0
        self.last_live_update_time = 0.0

    def run(self):
        logger.info("==========================================")
        logger.info("NSE EQUITY OHLC SYSTEM MASTER ACTIVATED")
        logger.info("==========================================")

        # Initial Boot check
        self.orchestrate_cycle(force_hist_check=True)

        while _keep_running:
            try:
                now_ts = time.time()
                self.calendar.refresh_calendar()
                status, reason = self.calendar.get_market_status()
                is_live = (status == "LIVE")

                logger.info(f"Market Status: [{status}] - {reason}")

                # Determine whether historical checks are due
                hist_due = (now_ts - self.last_hist_sync_time) >= HIST_SYNC_INTERVAL_SEC
                live_due = is_live and ((now_ts - self.last_live_update_time) >= LIVE_UPDATE_INTERVAL_SEC)

                if hist_due or live_due:
                    self.orchestrate_cycle(force_hist_check=hist_due, market_is_live=is_live)
                    if hist_due:
                        self.last_hist_sync_time = now_ts
                    if live_due:
                        self.last_live_update_time = now_ts

                # Interval sleep with responsiveness
                for _ in range(MARKET_CHECK_INTERVAL_SEC):
                    if not _keep_running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.critical(f"Unexpected error in Master loop: {e}", exc_info=True)
                time.sleep(10)

        logger.info("Master service safely stopped.")

    def orchestrate_cycle(self, force_hist_check: bool = False, market_is_live: bool = False):
        scripts = get_scripts_list()
        logger.info(f"SCRIPT LIST READ: Found {len(scripts)} active scripts in Firebase")

        if not scripts:
            logger.warning("No scripts located in /scripts/. Standing by...")
            return

        for script in scripts:
            if not _keep_running:
                break
            try:
                self.process_script(script, force_hist_check, market_is_live)
            except Exception as e:
                logger.error(f"Fault isolation caught exception on script {script}: {e}", exc_info=True)

    def process_script(self, script: str, check_history: bool, market_is_live: bool):
        """
        Processes an individual script ensuring priority:
        CHILD-1 (Sync) > CHILD-2 (Live)
        """
        existing_ohlc = get_stock_ohlc(script)

        # 1. State: NO_DATABASE
        if not existing_ohlc:
            logger.info(f"[{script}] State: NO_DATABASE. Invoking CHILD-1...")
            sync_historical_script(script, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
            return

        # 2. Check synchronization gap if periodic check is due
        sync_required = False
        gap = 0
        latest_fb_date = None

        if check_history:
            # Extract Index 0 Date
            idx0 = existing_ohlc.get("0") if isinstance(existing_ohlc, dict) else (existing_ohlc[0] if isinstance(existing_ohlc, list) and len(existing_ohlc) > 0 else None)
            
            if not idx0 or "date" not in idx0:
                logger.warning(f"[{script}] Corrupted index 0 found. Invoking CHILD-1...")
                sync_historical_script(script, gap_trading_days=TARGET_OHLC_COUNT, calendar=self.calendar)
                return

            latest_fb_date = datetime.strptime(idx0["date"], "%Y-%m-%d").date()
            
            # Query Yahoo's actual finalized daily candle
            latest_yahoo_date = get_latest_available_trading_date(script)
            
            if latest_yahoo_date:
                gap = self.calendar.get_trading_day_gap(latest_fb_date, latest_yahoo_date)
                if gap > 0:
                    logger.info(
                        f"[{script}] SYNC_REQUIRED: Firebase={latest_fb_date} | Yahoo={latest_yahoo_date} | Missing={gap} trading days"
                    )
                    sync_required = True
                else:
                    logger.debug(f"[{script}] SYNCHRONIZED: Up to date ({latest_fb_date})")

        # 3. Priority execution: CHILD-1
        if sync_required:
            sync_historical_script(script, gap_trading_days=gap, calendar=self.calendar)
            return

        # 4. Secondary execution: CHILD-2 (Only if synchronized & Market is LIVE)
        if market_is_live:
            update_live_script(script)


if __name__ == "__main__":
    orchestrator = MasterOrchestrator()
    orchestrator.run()