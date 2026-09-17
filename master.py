"""
MASTER ENGINE: Stock Market Data Orchestrator
Coordinates historical sync, intraday candle updates, indicators,
and live command events dispatched from Watchlist.jsx.
"""

import sys
import os
import time
import signal
import logging
import threading
from datetime import datetime
import pytz

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# FIXED_INDICES belongs to firebase_manager, not config
from config import (
    PATH_STOCKS,
    PATH_SCRIPTS,
    TIMEZONE,
    logger
)
from firebase_manager import (
    init_firebase,
    sanitize_key,
    FIXED_INDICES,
    reconcile_stocklist_with_watchlist,
    get_stocklist_mapping,
    get_stock_ohlc,
    update_live_candle
)
import firebase_admin
from firebase_admin import db
from yahoo_manager import (
    download_historical_daily,
    download_intraday_data
)

try:
    from parameter import update_all_parameters
except ImportError:
    update_all_parameters = None

try:
    from RUN_PIPELINE import main as run_screener_pipeline
except ImportError:
    run_screener_pipeline = None

IST = pytz.timezone(TIMEZONE if 'TIMEZONE' in locals() else "Asia/Kolkata")
_keep_running = True

def handle_exit_signal(sig, frame):
    global _keep_running
    logger.info("[SHUTDOWN] Interrupt received. Terminating orchestrator...")
    _keep_running = False

signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

class MasterOrchestrator:
    def __init__(self):
        init_firebase()
        self.script_status = {}
        self.last_stock_event_ts = 0.0
        self.replan_daily_routine()

    def replan_daily_routine(self):
        """Forces reconciliation against /watchlist and builds active target dictionary."""
        _, stock_map = reconcile_stocklist_with_watchlist()
        
        for k, v in FIXED_INDICES.items():
            stock_map.setdefault(sanitize_key(k), v)

        self.script_status = {
            name: {
                "synced": False,
                "last_attempt_at": None,
                "error": None,
                "ticker": ticker
            }
            for name, ticker in stock_map.items()
        }
        logger.info(f"[PLANNER] Reconciled stock targets. Active targets: {len(self.script_status)}")

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
            logger.error(f"[EVENT LISTENER] Error reading stock_event node: {e}")
        return False, {}

    def handle_stock_event(self, event: dict):
        action = event.get("action")
        stock = event.get("stock")
        ticker = event.get("ticker")

        if not stock:
            return

        safe_stock = sanitize_key(stock)
        logger.info(f"[STOCK EVENT] Received action: '{action}' on target: {stock} ({safe_stock})")

        if action == "ADD":
            self.replan_daily_routine()
            threading.Thread(
                target=self.sync_single_stock_history,
                args=(safe_stock, ticker or stock),
                daemon=True,
                name=f"SyncAdd-{safe_stock}"
            ).start()

        elif action == "DELETE":
            # 1. Admin purge from /stocks and /param
            try:
                db.reference(f"{PATH_STOCKS}/{safe_stock}").delete()
                db.reference(f"param/{safe_stock}").delete()
                logger.info(f"[DELETE EVENT] Purged /stocks/{safe_stock} and /param/{safe_stock}")
            except Exception as e:
                logger.error(f"[DELETE EVENT] Failed deleting /stocks or /param: {e}")

            # 2. Admin purge from /display_list/stocks
            try:
                display_ref = db.reference("display_list/stocks")
                current_display = display_ref.get()

                if isinstance(current_display, list):
                    filtered = [s for s in current_display if sanitize_key(str(s)) != safe_stock]
                    display_ref.set(filtered if filtered else None)
                    logger.info(f"[DELETE EVENT] Pruned {safe_stock} from array /display_list/stocks")
                elif isinstance(current_display, dict):
                    db.reference(f"display_list/stocks/{safe_stock}").delete()
                    if stock in current_display:
                        db.reference(f"display_list/stocks/{stock}").delete()
                    logger.info(f"[DELETE EVENT] Pruned {safe_stock} from dict /display_list/stocks")
            except Exception as e:
                logger.error(f"[DELETE EVENT] Failed cleaning display_list: {e}")

            # 3. Prune internal in-memory status
            if safe_stock in self.script_status:
                del self.script_status[safe_stock]

    def sync_single_stock_history(self, safe_stock: str, ticker: str):
        """Fetches historical bars for a newly enrolled stock and updates parameters."""
        logger.info(f"[ON-DEMAND SYNC] Starting historical sync for {safe_stock} ({ticker})...")
        try:
            from sync_child import sync_historical_script
            success, msg = sync_historical_script(safe_stock, ticker)
            if success:
                logger.info(f"[ON-DEMAND SYNC] {safe_stock} synchronized: {msg}")
                if update_all_parameters:
                    update_all_parameters()
            else:
                logger.warning(f"[ON-DEMAND SYNC] Sync failed for {safe_stock}: {msg}")
        except Exception as e:
            logger.error(f"[ON-DEMAND SYNC] Failed syncing {safe_stock}: {e}", exc_info=True)

    def execute_live_intraday_cycle(self):
        """Updates live candles and technical indicators for all active stocks."""
        for safe_stock, info in list(self.script_status.items()):
            ticker = info.get("ticker", safe_stock)
            try:
                live_candle = download_intraday_data(ticker)
                if live_candle:
                    update_live_candle(safe_stock, live_candle)
            except Exception as e:
                logger.debug(f"[LIVE CYCLE] Live update skipped for {safe_stock}: {e}")

    def run(self):
        logger.info("[ORCHESTRATOR] Master engine started. Listening for commands...")
        last_intraday_tick = 0.0

        while _keep_running:
            now = time.time()

            # 1. Listen for realtime actions from Watchlist.jsx
            has_event, event_payload = self.check_stock_event()
            if has_event:
                self.handle_stock_event(event_payload)

            # 2. Intraday tick cycle (every 60 seconds)
            if now - last_intraday_tick >= 60.0:
                self.execute_live_intraday_cycle()
                last_intraday_tick = now

            time.sleep(1.0)

        logger.info("[ORCHESTRATOR] Engine loop halted cleanly.")

if __name__ == "__main__":
    orchestrator = MasterOrchestrator()
    orchestrator.run()