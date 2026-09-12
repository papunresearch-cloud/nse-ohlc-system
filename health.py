"""
DISPUTE-AWARE HEALTH & AUDIT ENGINE (health.py)
Measures database distance from the ideal exchange trading date instead of
raising binary SYNC_ERROR panics.
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
                "status": "IDLE",            # IDLE | AUDITING | HEALTHY | DISPUTED | DEGRADED
                "target_date": "Unknown",
                "total_registered": 0,
                "ideal_count": 0,
                "disputed_count": 0,
                "broken_count": 0,
                "disputes": []              # [{name, stored_date, lag_days, status}]
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

    def record_audit_start(self):
        self.state["sync_data"]["status"] = "AUDITING"
        self.publish()

    def record_dispute_audit(self, target_date_str: str, stock_audits: list):
        """
        Evaluates stocks against the target exchange date.
        stock_audits: list of dicts:
          [{"name": "Coal India", "stored_date": "2026-09-11", "gap": 0, "broken": False}, ...]
        """
        total = len(stock_audits)
        ideal_count = 0
        broken_count = 0
        disputes = []

        for item in stock_audits:
            name = item.get("name", "Unknown")
            gap = item.get("gap", 0)
            is_broken = item.get("broken", False)
            stored_date = item.get("stored_date", "None")

            if is_broken:
                broken_count += 1
                disputes.append({
                    "name": name,
                    "stored_date": stored_date,
                    "lag_days": gap,
                    "status": "CORRUPTED_OR_EMPTY"
                })
            elif gap > 0:
                disputes.append({
                    "name": name,
                    "stored_date": stored_date,
                    "lag_days": gap,
                    "status": f"LAGGING ({gap}d)"
                })
            else:
                ideal_count += 1

        disputed_count = len(disputes) - broken_count

        if total == 0:
            overall_status = "IDLE"
        elif broken_count > 0:
            overall_status = "DEGRADED"
        elif disputed_count > 0:
            overall_status = "DISPUTED"
        else:
            overall_status = "HEALTHY"

        self.state["sync_data"] = {
            "last_sync_time": self._get_timestamp(),
            "status": overall_status,
            "target_date": target_date_str,
            "total_registered": total,
            "ideal_count": ideal_count,
            "disputed_count": disputed_count,
            "broken_count": broken_count,
            "disputes": disputes
        }
        self.publish()

    def publish(self):
        try:
            if not self.status_ref:
                init_firebase()
                self.status_ref = db.reference("system_status")
            self.status_ref.set(self.state)
        except Exception as e:
            logger.warning(f"[HEALTH] Failed to publish /system_status: {e}")


HEALTH_MONITOR = SystemHealthManager()