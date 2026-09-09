"""
CHILD-1: Historical Synchronization & Data Validation Engine.
- Maintains up to 250 historical records strictly under keys '1' through '250'.
- Guarantees Index 1 is the last COMPLETED trading day (never today's active session).
- Strips any bar newer than expected_latest_date (prevents Index 0/1 collisions).
- Sanitizes NaN / Inf floats to prevent Firebase JSON serialization errors.
- Decouples vendor lag from live tracking so CHILD-2 is not blocked.
- Completely leaves Index 0 untouched.
"""

import math
from datetime import datetime, date, time
import pandas as pd
import pytz

from config import (
    TARGET_OHLC_COUNT,
    HISTORICAL_START_INDEX,
    DEFAULT_SAFETY_MARGIN,
    TIMEZONE,
    logger,
)
from firebase_manager import get_stock_ohlc, write_full_ohlc
from yahoo_manager import download_historical_daily
from market_calendar import MarketCalendar

IST = pytz.timezone(TIMEZONE)
MARKET_CLOSE_TIME = time(15, 30)


def _get_expected_latest_date(calendar: MarketCalendar, now_ist: datetime) -> date:
    """
    Determines the exact date of the latest COMPLETED trading session.
    - Before 15:30 IST on a trading day: Previous trading day (today is incomplete).
    - At or after 15:30 IST on a trading day: Today's finalized session.
    - Weekends or NSE Holidays: Previous trading day.
    """
    today = now_ist.date()
    current_time = now_ist.time()

    if calendar.is_trading_day(today):
        if current_time < MARKET_CLOSE_TIME:
            return calendar.get_previous_trading_day(today)
        return today
    return calendar.get_previous_trading_day(today)


def _normalize_to_iso_date(val) -> str:
    """Converts timestamps, dates, or non-ISO strings to YYYY-MM-DD."""
    if val is None or pd.isna(val):
        return ""
    if isinstance(val, (date, datetime)):
        return val.strftime("%Y-%m-%d")

    s = str(val).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return ""

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    try:
        parsed = pd.to_datetime(s, errors="coerce")
        if pd.notnull(parsed):
            return parsed.strftime("%Y-%m-%d")
    except Exception:
        pass

    return ""


def _clean_price(val) -> float | None:
    """Validates numeric price, rejects NaN/Inf/zero/negative values."""
    try:
        if val is None or pd.isna(val):
            return None
        f = float(val)
        if math.isnan(f) or math.isinf(f) or f <= 0:
            return None
        return round(f, 2)
    except (ValueError, TypeError):
        return None


def _clean_volume(val) -> int:
    """Safely parses volume to integer, defaulting to 0 on NaN/corrupt input."""
    try:
        if val is None or pd.isna(val):
            return 0
        f = float(val)
        if math.isnan(f) or math.isinf(f) or f < 0:
            return 0
        return int(f)
    except (ValueError, TypeError):
        return 0


def _parse_firebase_historical_records(raw_data) -> list[dict]:
    """Extracts only keys '1' through '250', ignoring Index 0."""
    if not raw_data or not isinstance(raw_data, dict):
        return []
    records = []
    for i in range(HISTORICAL_START_INDEX, TARGET_OHLC_COUNT + 1):
        k = str(i)
        if k in raw_data and isinstance(raw_data[k], dict) and "date" in raw_data[k]:
            records.append(raw_data[k])
    return records


def _merge_and_sort_records(
    existing_records: list[dict],
    df: pd.DataFrame,
    expected_latest_date: date,
) -> list[dict]:
    """
    Merges existing records with incoming DataFrame, drops NaN/corrupt prices,
    and strips any candle newer than expected_latest_date so Index 1 is
    strictly the last completed market session.
    """
    date_map = {}
    expected_latest_str = expected_latest_date.strftime("%Y-%m-%d")

    # 1. Ingest existing Firebase historical records
    for r in existing_records:
        iso_d = _normalize_to_iso_date(r.get("date"))
        if iso_d and iso_d <= expected_latest_str:
            r_copy = dict(r)
            r_copy["date"] = iso_d
            date_map[iso_d] = r_copy

    # 2. Ingest and sanitize new rows from Yahoo Finance
    if df is not None and not df.empty:
        df_clean = df.copy()
        if isinstance(df_clean.columns, pd.MultiIndex):
            df_clean.columns = df_clean.columns.get_level_values(0)
        df_clean.columns = [str(c).strip().lower() for c in df_clean.columns]

        for _, row in df_clean.iterrows():
            d_str = _normalize_to_iso_date(row.get("date"))
            if not d_str:
                continue

            # Hard filter: Never admit today's in-progress or future sessions into historical DB
            if d_str > expected_latest_str:
                continue

            o = _clean_price(row.get("open"))
            h = _clean_price(row.get("high"))
            l = _clean_price(row.get("low"))
            c = _clean_price(row.get("close"))

            # Discard incomplete or NaN candle bars
            if o is None or h is None or l is None or c is None:
                continue

            # Candlestick boundary check
            if (h < l) or (h < max(o, c) - 0.05) or (l > min(o, c) + 0.05):
                continue

            date_map[d_str] = {
                "date": d_str,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": _clean_volume(row.get("volume")),
            }

    # 3. Sort strictly descending (newest completed date first)
    sorted_dates = sorted(date_map.keys(), reverse=True)
    return [date_map[d] for d in sorted_dates]


def validate_historical_payload(payload: dict[str, dict]) -> tuple[bool, str]:
    """Validates contiguous keys starting at 1, price integrity, and descending dates."""
    count = len(payload)
    if count == 0:
        return False, "Historical payload is completely empty"
    if count > TARGET_OHLC_COUNT:
        return False, f"Payload count {count} exceeds limit {TARGET_OHLC_COUNT}"

    dates_seen = []
    for i in range(1, count + 1):
        k = str(i)
        if k not in payload:
            return False, f"Missing contiguous historical index '{k}'"

        bar = payload[k]
        for field in ("date", "open", "high", "low", "close"):
            if field not in bar or bar[field] is None:
                return False, f"Index {k} missing required field '{field}'"

        try:
            o, h, l, c = float(bar["open"]), float(bar["high"]), float(bar["low"]), float(bar["close"])
        except (ValueError, TypeError):
            return False, f"Index {k} contains non-numeric values"

        if o <= 0 or h <= 0 or l <= 0 or c <= 0 or math.isnan(o) or math.isnan(h) or math.isnan(l) or math.isnan(c):
            return False, f"Index {k} has non-positive or NaN price (O={o}, H={h}, L={l}, C={c})"

        if (h < l) or (h < max(o, c) - 0.05) or (l > min(o, c) + 0.05):
            return False, f"Index {k} OHLC boundary violation (O={o}, H={h}, L={l}, C={c})"

        d_str = str(bar["date"])
        try:
            d_val = datetime.strptime(d_str, "%Y-%m-%d").date()
        except ValueError:
            return False, f"Index {k} has invalid date format: {d_str}"

        if dates_seen and d_val >= dates_seen[-1]:
            return False, f"Index {k} date {d_val} is not strictly older than {dates_seen[-1]}"

        dates_seen.append(d_val)

    return True, "Valid"


def sync_historical_script(
    display_name: str,
    ticker: str = None,
    gap_trading_days: int = 0,
    calendar: MarketCalendar = None,
) -> tuple[bool, str]:
    """
    Coordinates historical catch-up and returns live-readiness status:
    - Guarantees Index 1 matches the latest completed market session.
    - Prevents vendor lag from blocking live updates if a valid baseline exists.
    """
    try:
        # Defensive argument resolution
        if isinstance(ticker, int):
            gap_trading_days = ticker
            ticker = display_name
        elif ticker is None:
            ticker = display_name

        if calendar is None:
            calendar = MarketCalendar()

        now_ist = datetime.now(IST)
        expected_latest_date = _get_expected_latest_date(calendar, now_ist)
        expected_latest_str = expected_latest_date.strftime("%Y-%m-%d")

        existing_ohlc = get_stock_ohlc(display_name)
        existing_records = _parse_firebase_historical_records(existing_ohlc)

        # 1. Quick Bypass: Baseline already current
        if existing_records and existing_records[0].get("date") == expected_latest_str:
            return True, f"VERIFIED: Firebase baseline already current at {expected_latest_str}"

        # 2. Determine Fetch Depth
        is_empty_bootstrap = not existing_records
        if is_empty_bootstrap:
            days_needed = TARGET_OHLC_COUNT + DEFAULT_SAFETY_MARGIN
        else:
            curr_latest_str = existing_records[0].get("date")
            try:
                curr_latest_date = datetime.strptime(curr_latest_str, "%Y-%m-%d").date()
                actual_gap = calendar.get_trading_day_gap(curr_latest_date, expected_latest_date)
            except (ValueError, TypeError):
                actual_gap = 10
            days_needed = min(max(gap_trading_days, actual_gap) + DEFAULT_SAFETY_MARGIN, 30)

        # 3. Query Vendor & Normalize
        df = download_historical_daily(ticker, days_needed=days_needed)
        merged_records = _merge_and_sort_records(existing_records, df, expected_latest_date)

        # 4. Freshness Check & Capped Retry (30 Days)
        if merged_records and merged_records[0]["date"] != expected_latest_str and not is_empty_bootstrap:
            logger.warning(
                f"[{display_name}] Stale data detected (Got: {merged_records[0]['date']}, Expected: {expected_latest_str}). "
                f"Executing capped retry (30 days)..."
            )
            df_retry = download_historical_daily(ticker, days_needed=30)
            merged_records = _merge_and_sort_records(existing_records, df_retry, expected_latest_date)

        # 5. Handle Vendor Lag Without Blocking CHILD-2
        actual_latest = merged_records[0]["date"] if merged_records else ""
        if actual_latest != expected_latest_str:
            if existing_records:
                logger.warning(
                    f"[{display_name}] Vendor lag: Latest available is {actual_latest}, expected {expected_latest_str}. "
                    f"Preserving existing Firebase baseline ({len(existing_records)} bars). Live update permitted."
                )
                return True, f"VENDOR_LAG: Baseline preserved at {existing_records[0].get('date')}; Live tracking allowed"

            if not merged_records:
                msg = f"INITIAL_SYNC_FAILED: Yahoo returned no valid data for {ticker}"
                logger.error(f"[{display_name}] {msg}. Firebase untouched.")
                return False, msg

            # Bootstrapping with available historical records
            logger.warning(
                f"[{display_name}] VENDOR_LAG_BOOTSTRAP: Baseline seeded with {len(merged_records)} bars "
                f"(Latest available: {actual_latest}). Live update permitted."
            )

        # 6. Build Contiguous Payload (Keys '1' to 'N')
        final_candles = merged_records[:TARGET_OHLC_COUNT]
        indexed_db = {}
        for idx, candle in enumerate(final_candles):
            indexed_db[str(idx + HISTORICAL_START_INDEX)] = candle

        # 7. Validate Payload
        valid, err_msg = validate_historical_payload(indexed_db)
        if not valid:
            msg = f"Sanity validation rejected: {err_msg}"
            logger.error(f"[{display_name}] {msg}. Firebase left untouched.")
            return False, msg

        # 8. Write to Firebase
        write_ok = write_full_ohlc(display_name, indexed_db)
        if write_ok:
            rec_count = len(indexed_db)
            latest_dt = indexed_db["1"]["date"]
            return True, f"OK ({rec_count} historical bars, Index 1: {latest_dt})"
        return False, "Firebase Realtime DB rejected write payload"

    except Exception as e:
        logger.error(f"[{display_name}] Internal sync exception: {e}", exc_info=True)
        return False, f"Exception: {str(e)}"