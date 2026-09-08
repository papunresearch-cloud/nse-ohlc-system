"""
CHILD-1: Historical Synchronization & Data Validation Engine.
- Maintains up to 250 historical records under keys '1' through '250'.
- Enforces strict temporal freshness anchored to MarketCalendar.
- Dynamic trading-day gap calculation; auto deep-fetch retry on stale data.
- Strips current session only if the market is actively open.
- Leaves index '0' untouched.
"""
from datetime import datetime, date
import pandas as pd
import pytz

from config import TARGET_OHLC_COUNT, HISTORICAL_START_INDEX, DEFAULT_SAFETY_MARGIN, TIMEZONE, logger
from firebase_manager import get_stock_ohlc, write_full_ohlc
from yahoo_manager import download_historical_daily
from market_calendar import MarketCalendar

IST = pytz.timezone(TIMEZONE)


def _get_expected_latest_date(calendar: MarketCalendar, now_ist: datetime) -> date:
    """
    Determines the single authority date that MUST occupy Index 1:
    - If today is a trading day and market is LIVE/PRE_OPEN: Previous completed session.
    - If today is a trading day and market is CLOSED (post 15:30 IST): Today's finalized session.
    - If today is a non-trading day (weekend/holiday): Previous completed session.
    """
    today = now_ist.date()
    if calendar.is_trading_day(today):
        status, _ = calendar.get_market_status(now_ist)
        if status in ("LIVE", "PRE_OPEN"):
            return calendar.get_previous_trading_day(today)
        return today
    return calendar.get_previous_trading_day(today)


def sync_historical_script(display_name: str, ticker: str, gap_trading_days: int = 0, calendar: MarketCalendar = None) -> tuple[bool, str]:
    """
    Coordinates historical catch-up or full baseline construction for indices 1 to 250.
    Guarantees that Index 1 matches the latest completed market session.
    """
    try:
        if calendar is None:
            calendar = MarketCalendar()

        now_ist = datetime.now(IST)
        expected_latest_date = _get_expected_latest_date(calendar, now_ist)
        expected_latest_str = expected_latest_date.strftime("%Y-%m-%d")

        existing_ohlc = get_stock_ohlc(display_name)
        existing_records = _parse_firebase_historical_records(existing_ohlc)

        # 1. Determine Initial Fetch Depth
        if not existing_records:
            days_needed = TARGET_OHLC_COUNT + DEFAULT_SAFETY_MARGIN
        else:
            curr_latest_str = existing_records[0].get("date")
            try:
                curr_latest_date = datetime.strptime(curr_latest_str, "%Y-%m-%d").date()
                actual_gap = calendar.get_trading_day_gap(curr_latest_date, expected_latest_date)
            except (ValueError, TypeError):
                actual_gap = TARGET_OHLC_COUNT

            gap_to_use = max(gap_trading_days, actual_gap)
            days_needed = max(gap_to_use + DEFAULT_SAFETY_MARGIN, 10)

        # 2. Query Historical Daily Candles
        df = download_historical_daily(ticker, days_needed=days_needed)
        merged_records = _merge_and_sort_records(existing_records, df, calendar, now_ist)

        # 3. Freshness Verification & Adaptive Deep-Fetch Retry
        if merged_records:
            actual_latest_str = merged_records[0]["date"]
            if actual_latest_str != expected_latest_str:
                logger.warning(
                    f"[{display_name}] Stale data detected (Got: {actual_latest_str}, Expected: {expected_latest_str}). "
                    f"Executing adaptive deep-fetch retry..."
                )
                deep_days = TARGET_OHLC_COUNT + DEFAULT_SAFETY_MARGIN
                df_deep = download_historical_daily(ticker, days_needed=deep_days)
                merged_records = _merge_and_sort_records(existing_records, df_deep, calendar, now_ist)

        if not merged_records:
            msg = f"Vendor returned empty data or normalization failed for {ticker}"
            logger.warning(f"[{display_name}] {msg}")
            return False, msg

        # Final Freshness Gate
        actual_latest_str = merged_records[0]["date"]
        if actual_latest_str != expected_latest_str:
            msg = f"Stale vendor data: Latest bar {actual_latest_str} != Expected {expected_latest_str}"
            logger.error(f"[{display_name}] {msg}. Firebase left untouched.")
            return False, msg

        # 4. Slicing to Target Depth
        final_candles = merged_records[:TARGET_OHLC_COUNT]

        # 5. Build Sequential Payload (1 to N)
        indexed_db = {}
        for idx, candle in enumerate(final_candles):
            indexed_db[str(idx + HISTORICAL_START_INDEX)] = candle

        # 6. Structural Validation
        valid, err_msg = validate_historical_payload(indexed_db)
        if not valid:
            msg = f"Sanity validation rejected: {err_msg}"
            logger.error(f"[{display_name}] {msg}. Firebase left untouched.")
            return False, msg

        # 7. Write to Firebase
        write_ok = write_full_ohlc(display_name, indexed_db)
        if write_ok:
            rec_count = len(indexed_db)
            latest_dt = indexed_db["1"]["date"]
            return True, f"OK ({rec_count} historical bars, Index 1: {latest_dt})"
        else:
            return False, "Firebase Realtime DB rejected write payload"

    except Exception as e:
        logger.error(f"[{display_name}] Internal sync exception: {e}", exc_info=True)
        return False, f"Exception: {str(e)}"


def _parse_firebase_historical_records(raw_data) -> list[dict]:
    """Extracts only keys '1' through '250', completely discarding '0'."""
    if not raw_data or not isinstance(raw_data, dict):
        return []
    records = []
    for i in range(HISTORICAL_START_INDEX, TARGET_OHLC_COUNT + 1):
        k = str(i)
        if k in raw_data and isinstance(raw_data[k], dict) and "date" in raw_data[k]:
            records.append(raw_data[k])
    return records


def _merge_and_sort_records(existing_records: list[dict], df: pd.DataFrame, calendar: MarketCalendar, now_ist: datetime) -> list[dict]:
    """
    Combines existing historical rows with downloaded dataframe, deduplicates by date,
    and strips out ongoing session so Index 1 is always the last COMPLETED session.
    """
    date_map = {}
    for r in existing_records:
        d = r.get("date")
        if d:
            date_map[d] = r

    if df is not None and not df.empty:
        df_clean = df.copy()
        if isinstance(df_clean.columns, pd.MultiIndex):
            df_clean.columns = df_clean.columns.get_level_values(0)
        df_clean.columns = [str(c).strip().lower() for c in df_clean.columns]

        for _, row in df_clean.iterrows():
            row_date = row.get("date")
            if isinstance(row_date, (date, datetime)):
                d_str = row_date.strftime("%Y-%m-%d")
            else:
                d_str = str(row_date) if row_date is not None else ""

            if not d_str or d_str.lower() == "nan":
                continue

            vol_raw = row.get("volume", 0)
            try:
                volume_val = int(vol_raw) if pd.notnull(vol_raw) else 0
            except (ValueError, TypeError):
                volume_val = 0

            date_map[d_str] = {
                "date": d_str,
                "open": round(float(row.get("open", 0.0)), 2),
                "high": round(float(row.get("high", 0.0)), 2),
                "low": round(float(row.get("low", 0.0)), 2),
                "close": round(float(row.get("close", 0.0)), 2),
                "volume": volume_val
            }

    # Remove today's candle ONLY if the session is currently active/open
    today_date = now_ist.date()
    if calendar.is_trading_day(today_date):
        status, _ = calendar.get_market_status(now_ist)
        if status in ("LIVE", "PRE_OPEN"):
            today_str = today_date.strftime("%Y-%m-%d")
            if today_str in date_map:
                del date_map[today_str]

    sorted_dates = sorted(date_map.keys(), reverse=True)
    return [date_map[d] for d in sorted_dates]


def validate_historical_payload(payload: dict[str, dict]) -> tuple[bool, str]:
    """Validates sequential keys starting at 1, price integrity, and descending dates."""
    count = len(payload)
    if count == 0:
        return False, "Historical payload is completely empty"
    if count > TARGET_OHLC_COUNT:
        return False, f"Payload count {count} exceeds limit {TARGET_OHLC_COUNT}"

    dates_seen = []
    for i in range(1, count + 1):
        k = str(i)
        if k not in payload:
            return False, f"Missing contiguous historical sequential index '{k}'"

        bar = payload[k]
        for field in ("date", "open", "high", "low", "close"):
            if field not in bar or bar[field] is None:
                return False, f"Index {k} missing required field '{field}'"

        try:
            o, h, l, c = float(bar["open"]), float(bar["high"]), float(bar["low"]), float(bar["close"])
        except (ValueError, TypeError):
            return False, f"Index {k} contains non-numeric values"

        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            return False, f"Index {k} has non-positive price (O={o}, H={h}, L={l}, C={c})"

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