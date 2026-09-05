"""
CHILD-1: Historical Synchronization & Data Validation Engine.
- Maintains up to 250 historical records under keys '1' through '250'.
- Completely excludes and leaves index '0' untouched.
- Slices excess rows to enforce the 250-bar capacity.
- Supports recent listings/IPOs gracefully.
"""
from datetime import datetime, date
import pandas as pd
import pytz

from config import TARGET_OHLC_COUNT, HISTORICAL_START_INDEX, DEFAULT_SAFETY_MARGIN, TIMEZONE, logger
from firebase_manager import get_stock_ohlc, write_full_ohlc
from yahoo_manager import download_historical_daily

IST = pytz.timezone(TIMEZONE)


def sync_historical_script(display_name: str, ticker: str, gap_trading_days: int = 0, calendar=None) -> tuple[bool, str]:
    """
    Coordinates historical catch-up or full baseline construction for indices 1 to 250.
    Returns:
        (True, "Success summary") or (False, "Exact rejection reason")
    """
    try:
        existing_ohlc = get_stock_ohlc(display_name)
        # Parse ONLY historical keys ('1' to '250'); index '0' is completely ignored
        existing_records = _parse_firebase_historical_records(existing_ohlc)

        # Determine fetch depth
        if not existing_records:
            days_needed = TARGET_OHLC_COUNT + DEFAULT_SAFETY_MARGIN
        else:
            days_needed = max(gap_trading_days + DEFAULT_SAFETY_MARGIN, 10)

        # Query historical daily candles from Yahoo Finance
        df = download_historical_daily(ticker, days_needed=days_needed)
        if df is None or df.empty:
            msg = f"Vendor returned empty data or HTTP error for {ticker}"
            logger.warning(f"[{display_name}] {msg}")
            return False, msg

        # Merge, deduplicate, and sort newest to oldest
        merged_records = _merge_and_sort_records(existing_records, df)
        if not merged_records:
            msg = "Merged dataset is empty after date normalization"
            logger.warning(f"[{display_name}] {msg}")
            return False, msg

        # Retain up to TARGET_OHLC_COUNT (250) completed sessions
        final_candles = merged_records[:TARGET_OHLC_COUNT]

        # Key strictly from "1" to "N" (where N <= 250)
        indexed_db = {}
        for idx, candle in enumerate(final_candles):
            indexed_db[str(idx + HISTORICAL_START_INDEX)] = candle

        # Validate strictly keys 1 through N
        valid, err_msg = validate_historical_payload(indexed_db)
        if not valid:
            msg = f"Sanity validation rejected: {err_msg}"
            logger.error(f"[{display_name}] {msg}. Firebase left untouched.")
            return False, msg

        # Write historical payload to Firebase (preserves index '0')
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


def _merge_and_sort_records(existing_records: list[dict], df: pd.DataFrame) -> list[dict]:
    """Combines existing historical rows with downloaded dataframe and deduplicates by date."""
    date_map = {}
    for r in existing_records:
        d = r.get("date")
        if d:
            date_map[d] = r

    # Standardize DataFrame column names to lowercase to avoid KeyErrors
    df_clean = df.copy()
    df_clean.columns = [str(c).lower().strip() for c in df_clean.columns]

    for _, row in df_clean.iterrows():
        # Handle date extraction safely
        row_date = row.get("date")
        if isinstance(row_date, (date, datetime)):
            d_str = row_date.strftime("%Y-%m-%d")
        else:
            d_str = str(row_date) if row_date is not None else ""

        if not d_str or d_str == "nan":
            continue

        # Extract volume safely (returns 0 if not found or NaN)
        vol_val = row.get("volume", 0)
        try:
            volume = int(vol_val) if pd.notnull(vol_val) else 0
        except (ValueError, TypeError):
            volume = 0

        date_map[d_str] = {
            "date": d_str,
            "open": round(float(row.get("open", 0.0)), 2),
            "high": round(float(row.get("high", 0.0)), 2),
            "low": round(float(row.get("low", 0.0)), 2),
            "close": round(float(row.get("close", 0.0)), 2),
            "volume": volume
        }

    # Sort descending: newest completed session first
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

        # Cushion of 0.05 for rounding anomalies on candle extremes
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
