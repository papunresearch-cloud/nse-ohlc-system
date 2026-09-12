"""
HEALTH & DIAGNOSTIC REPORTER MODULE (health.py)
Tracks backend state, live update timestamps, historical sync health, 
and publishes real-time diagnostics to Firebase Realtime Database.
"""
import time
from datetime import datetime
import pytz
from firebase_admin import db
from config import TIMEZONE, logger
from firebase_manager import init_firebase

IST = pytz.timezone(TIMEZONE)


class SystemHealthManager:
    def __init__(self):
        # Ensure Firebase Admin SDK is initialized before requesting database reference
        init_firebase()
        self.status_ref = db.reference("system_status")
        self.state = {
            "backend_power": "RUNNING",
            "last_heartbeat": self._get_timestamp(),
            "heartbeat_epoch": time.time(),
            "live_data": {
                "last_update_time": "Never",
                "tickers_updated": 0,
                "status": "IDLE"
            },
            "sync_data": {
                "last_sync_time": "Never",
                "status": "IDLE",
                "total_registered": 0,
                "synced_count": 0,
                "failed_count": 0,
                "failed_scripts": []
            }
        }
        self.publish()

    def _get_timestamp(self) -> str:
        return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    def record_heartbeat(self, is_power_on: bool):
        self.state["backend_power"] = "RUNNING" if is_power_on else "STOPPED"
        self.state["last_heartbeat"] = self._get_timestamp()
        self.state["heartbeat_epoch"] = time.time()
        self.publish()

    def record_live_update(self, updated_count: int, is_market_open: bool = True):
        self.state["live_data"]["last_update_time"] = self._get_timestamp()
        self.state["live_data"]["tickers_updated"] = updated_count
        self.state["live_data"]["status"] = "STREAMING" if is_market_open else "MARKET_CLOSED"
        self.publish()

    def record_sync_start(self):
        self.state["sync_data"]["status"] = "SYNCING"
        self.publish()

    def record_sync_finish(self, total: int, synced: list, unsynced: list):
        """
        Records the outcome of a synchronization run.
        Filters out queued/pending entries so newly booted or waiting stocks
        do not falsely trigger a SYNC_ERROR state.
        """
        self.state["sync_data"]["last_sync_time"] = self._get_timestamp()
        self.state["sync_data"]["total_registered"] = total
        self.state["sync_data"]["synced_count"] = len(synced)

        # Distinguish genuine vendor/validation errors from unattempted/pending entries
        true_failed_names = []
        for item in unsynced:
            if isinstance(item, dict):
                err = str(item.get("error", ""))
                # Stocks with no error or still awaiting initial sync are not genuine failures
                if err and "Awaiting" not in err:
                    true_failed_names.append(item.get("name", "Unknown"))
            elif isinstance(item, str):
                true_failed_names.append(item)

        self.state["sync_data"]["failed_count"] = len(true_failed_names)
        self.state["sync_data"]["failed_scripts"] = true_failed_names

        # Evaluate engine status accurately
        if total > 0 and len(synced) == total:
            self.state["sync_data"]["status"] = "VERIFIED"
        elif len(true_failed_names) > 0:
            self.state["sync_data"]["status"] = "SYNC_ERROR"
        else:
            self.state["sync_data"]["status"] = "IDLE"

        self.publish()

    def publish(self):
        try:
            if not self.status_ref:
                init_firebase()
                self.status_ref = db.reference("system_status")
            self.status_ref.set(self.state)
        except Exception as e:
            logger.warning(f"[HEALTH] Failed to update /system_status node: {e}")


# Global singleton instance
HEALTH_MONITOR = SystemHealthManager()