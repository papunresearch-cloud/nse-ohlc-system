"""
NSE Market Calendar logic.
Evaluates current market status against weekends, holidays, special sessions,
and manual overrides dynamically loaded from Firebase.
"""
from datetime import datetime, date, time as dt_time, timedelta
import pytz
from config import TIMEZONE, TEST_MODE, SIMULATED_TIME, logger
from firebase_manager import get_calendar_config, set_calendar_config

# Baseline 2026 Calendar fallback seed
INITIAL_2026_CALENDAR = {
    "market": "NSE_EQUITY",
    "timezone": "Asia/Kolkata",
    "regular_market": {"start": "09:15", "end": "15:30"},
    "pre_open": {"start": "09:00", "end": "09:08"},
    "closing_auction": {
        "order_entry_start": "15:20",
        "order_entry_end": "15:30",
        "matching_start": "15:30",
        "matching_end": "15:35",
        "transition_start": "15:35",
        "transition_end": "15:50"
    },
    "closing_session": {"start": "15:50", "end": "16:00"},
    "weekend": {"saturday": "HOLIDAY", "sunday": "HOLIDAY"},
    "holidays": {
        "2026-01-15": "Municipal Corporation Election - Maharashtra",
        "2026-01-26": "Republic Day",
        "2026-03-03": "Holi",
        "2026-03-26": "Shri Ram Navami",
        "2026-03-31": "Shri Mahavir Jayanti",
        "2026-04-03": "Good Friday",
        "2026-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
        "2026-05-01": "Maharashtra Day",
        "2026-05-28": "Bakri Id",
        "2026-06-26": "Muharram",
        "2026-09-14": "Ganesh Chaturthi",
        "2026-10-02": "Mahatma Gandhi Jayanti",
        "2026-10-20": "Dussehra",
        "2026-11-08": "Diwali Laxmi Pujan - Trading Holiday / Muhurat Trading",
        "2026-11-10": "Diwali-Balipratipada",
        "2026-11-24": "Prakash Gurpurb Sri Guru Nanak Dev",
        "2026-12-25": "Christmas"
    },
    "weekend_holidays": {
        "2026-02-15": "Mahashivratri",
        "2026-03-21": "Id-Ul-Fitr (Ramadan Eid)",
        "2026-08-15": "Independence Day",
        "2026-11-08": "Diwali Laxmi Pujan"
    },
    "special_trading_days": {
        "2026-11-08": {
            "status": "SPECIAL_TRADING",
            "name": "Muhurat Trading - Diwali Laxmi Pujan",
            "start": "18:00",
            "end": "19:00",
            "timing_pending": False
        }
    },
    "manual_overrides": {}
}


class MarketCalendar:
    def __init__(self):
        self.tz = pytz.timezone(TIMEZONE)
        self.config = {}
        self.refresh_calendar()

    def refresh_calendar(self) -> None:
        """Loads or seeds calendar configurations from Firebase."""
        cfg = get_calendar_config()
        if not cfg or "regular_market" not in cfg:
            logger.warning("NSE Calendar not found in Firebase. Seeding defaults...")
            set_calendar_config(INITIAL_2026_CALENDAR)
            self.config = INITIAL_2026_CALENDAR
        else:
            self.config = cfg

    def get_current_time(self) -> datetime:
        """Returns localized datetime, supporting simulation in test mode."""
        if TEST_MODE and SIMULATED_TIME:
            try:
                dt = datetime.strptime(SIMULATED_TIME, "%Y-%m-%d %H:%M")
                return self.tz.localize(dt)
            except Exception:
                pass
        return datetime.now(self.tz)

    def is_holiday(self, check_date: date) -> tuple[bool, str]:
        date_str = check_date.strftime("%Y-%m-%d")
        holidays = self.config.get("holidays", {})
        if date_str in holidays:
            return True, holidays[date_str]
        return False, ""

    def is_weekend(self, check_date: date) -> bool:
        return check_date.weekday() in (5, 6)  # 5=Saturday, 6=Sunday

    def get_special_trading(self, check_date: date) -> dict | None:
        date_str = check_date.strftime("%Y-%m-%d")
        return self.config.get("special_trading_days", {}).get(date_str)

    def get_manual_override(self, check_date: date) -> dict | None:
        date_str = check_date.strftime("%Y-%m-%d")
        return self.config.get("manual_overrides", {}).get(date_str)

    def is_trading_day(self, check_date: date) -> bool:
        """
        Calculates if date is a valid trading session based on strict priority:
        1. Manual Override
        2. Special Trading Session
        3. Holiday
        4. Weekend
        5. Normal Weekday
        """
        # 1. Manual Override
        override = self.get_manual_override(check_date)
        if override:
            status = override.get("status", "").upper()
            if status in ("TRADING", "SPECIAL_TRADING"):
                return True
            if status in ("HOLIDAY", "CLOSED"):
                return False

        # 2. Special Trading Day
        special = self.get_special_trading(check_date)
        if special and special.get("status") == "SPECIAL_TRADING":
            return True

        # 3. Holiday
        is_hol, _ = self.is_holiday(check_date)
        if is_hol:
            return False

        # 4. Weekend
        if self.is_weekend(check_date):
            return False

        # 5. Regular Trading Day
        return True

    def get_market_status(self, now: datetime | None = None) -> tuple[str, str]:
        """
        Determines current state: LIVE, CLOSED, PRE_OPEN, CLOSING_AUCTION, etc.
        Returns: (STATUS, REASON)
        """
        if now is None:
            now = self.get_current_time()

        curr_date = now.date()
        curr_time = now.time()

        # 1. Manual Override
        override = self.get_manual_override(curr_date)
        if override:
            status = override.get("status", "").upper()
            if status in ("HOLIDAY", "CLOSED"):
                return "CLOSED", f"Manual Override: {override.get('name', 'Closed')}"
            if status == "SPECIAL_TRADING":
                start = self._parse_time(override.get("start", "09:15"))
                end = self._parse_time(override.get("end", "15:30"))
                if start <= curr_time < end:
                    return "LIVE", f"Manual Override Session: {override.get('name')}"
                return "CLOSED", "Outside Manual Override Session"

        # 2. Special Trading Day (e.g. Muhurat)
        special = self.get_special_trading(curr_date)
        if special and special.get("status") == "SPECIAL_TRADING":
            start_str = special.get("start")
            end_str = special.get("end")
            if not start_str or not end_str or special.get("timing_pending", False):
                return "CLOSED", f"Special Trading ({special.get('name')}) timing pending"
            start = self._parse_time(start_str)
            end = self._parse_time(end_str)
            if start <= curr_time < end:
                return "LIVE", f"Special Trading: {special.get('name')}"
            return "CLOSED", f"Outside Special Trading Window ({start_str}-{end_str})"

        # 3. Holiday Check
        is_hol, hol_name = self.is_holiday(curr_date)
        if is_hol:
            return "CLOSED", f"NSE Holiday: {hol_name}"

        # 4. Weekend Check
        if self.is_weekend(curr_date):
            return "CLOSED", "Weekend (Market Closed)"

        # 5. Regular Session Windows
        reg = self.config.get("regular_market", {"start": "09:15", "end": "15:30"})
        reg_start = self._parse_time(reg["start"])
        reg_end = self._parse_time(reg["end"])

        pre = self.config.get("pre_open", {"start": "09:00", "end": "09:08"})
        pre_start = self._parse_time(pre["start"])
        pre_end = self._parse_time(pre["end"])

        if pre_start <= curr_time < pre_end:
            return "PRE_OPEN", "Pre-market opening order matching"

        if reg_start <= curr_time < reg_end:
            return "LIVE", "Regular Trading Hours"

        return "CLOSED", "Outside Regular Market Hours"

    def is_market_live(self, now: datetime | None = None) -> bool:
        status, _ = self.get_market_status(now)
        return status == "LIVE"

    def get_previous_trading_day(self, ref_date: date) -> date:
        """Finds the immediately preceding valid trading date."""
        target = ref_date - timedelta(days=1)
        while not self.is_trading_day(target):
            target -= timedelta(days=1)
        return target

    def get_trading_day_gap(self, start_date: date, end_date: date) -> int:
        """
        Calculates missing trading days between start_date (exclusive) and end_date (inclusive).
        Returns 0 if end_date <= start_date.
        """
        if end_date <= start_date:
            return 0
        gap = 0
        cur = start_date + timedelta(days=1)
        while cur <= end_date:
            if self.is_trading_day(cur):
                gap += 1
            cur += timedelta(days=1)
        return gap

    @staticmethod
    def _parse_time(t_str: str) -> dt_time:
        parts = t_str.split(":")
        return dt_time(int(parts[0]), int(parts[1]))