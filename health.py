"""
HEALTH & DIAGNOSTIC REPORTER MODULE (health.py)
Tracks backend operational state, live update counts, baseline sync timestamps,
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
        # Guarantee Firebase connection is active before requesting database reference
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
                "target_date": "N/A",
                "total_registered": 0,
                "synced_count": 0,
                "status": "IDLE"
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

    def record_sync_finish(self, total: int, synced_count: int, target_date_str: str = "N/A"):
        """Records telemetry summary of the baseline historical sync without running dispute tracking."""
        self.state["sync_data"]["last_sync_time"] = self._get_timestamp()
        self.state["sync_data"]["target_date"] = target_date_str
        self.state["sync_data"]["total_registered"] = total
        self.state["sync_data"]["synced_count"] = synced_count
        self.state["sync_data"]["status"] = "VERIFIED" if (total > 0 and synced_count == total) else "IDLE"
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