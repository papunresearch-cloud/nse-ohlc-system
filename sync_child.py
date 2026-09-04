"""
CHILD-1: Historical Synchronization.
Responsible for:
- Bootstrapping new databases to exactly 250 records.
- Catching up existing databases behind Yahoo Finance using gap + safety margin.
- Idempotent normalization, duplicate purging, sorting newest to oldest.
- Defensive validation ensuring zero corruption to Firebase Realtime Database.
"""
from datetime import datetime, date
import pandas as pd
from config import TARGET_OHLC_COUNT, DEFAULT_SAFETY_MARGIN, logger
from firebase_manager import get_stock_ohlc, write_full_ohlc
from yahoo_manager import download_historical_daily
from market_calendar import MarketCalendar


def sync_historical_script(script: str, gap_trading_days: int, calendar: MarketCalendar) -> bool:
    """
    Coordinates historical catchup or complete bootstrap.
    Returns True if successfully validated and synchronized to Firebase.
    """
    logger.info(f"[{script}] Synchronizing historical data (Gap: {gap_trading_days} days)...")
    
    # 1. Read existing records from Firebase
    existing_data = get_stock_ohlc(script)
    existing_list = _parse_firebase_records(existing_data)

    # 2. Determine fetch size
    if not existing_list or len(existing_list) < TARGET_OHLC_COUNT:
        # Full bootstrap required
        logger.info(f"[{script}] Insufficient baseline ({len(existing_list)} records). Downloading full ~250 days...")
        days_to_download = TARGET_OHLC_COUNT + DEFAULT_SAFETY_MARGIN
    else:
        # Incremental catch-up
        days_to_download = gap_trading_days + DEFAULT_SAFETY_MARGIN
        logger.info(f"[{script}] Downloading incremental {days_to_download} days (Gap {gap_trading_days} + Margin {DEFAULT_SAFETY_MARGIN})...")

    # 3. Fetch data from Yahoo
    yahoo_df = download_historical_daily(script, days_needed=days_to_download)
    if yahoo_df.empty:
        logger.error(f"[{script}] Failed to obtain historical records from Yahoo. Aborting sync.")
        return False

    yahoo_list = yahoo_df.to_dict(orient="records")

    # 4. Merge, Normalize, Sort, Deduplicate
    merged_records = _merge_and_sort_records(existing_list, yahoo_list)

    # 5. Check if we reached 250 records
    if len(merged_records) < TARGET_OHLC_COUNT:
        logger.warning(
            f"[{script}] Retained record count ({len(merged_records)}) is below target {TARGET_OHLC_COUNT}. "
            f"Stock may not have sufficient trading history."
        )
        # If the stock has less than 250 total days of market existence, we retain what's valid
        if len(merged_records) == 0:
            return False

    final_candles = merged_records[:TARGET_OHLC_COUNT]

    # 6. Rebuild exact index 0 through 249
    indexed_db = {}
    for idx, candle in enumerate(final_candles):
        indexed_db[str(idx)] = {
            "date": str(candle["date"]),
            "open": round(float(candle["open"]), 2),
            "high": round(float(candle["high"]), 2),
            "low": round(float(candle["low"]), 2),
            "close": round(float(candle["close"]), 2)
        }

    # 7. Defensive Validation
    valid, err_msg = validate_ohlc_payload(indexed_db)
    if not valid:
        logger.error(f"[{script}] Validation rejected payload: {err_msg}. Firebase untouched.")
        return False

    # 8. Write to Firebase
    success = write_full_ohlc(script, indexed_db)
    if success:
        logger.info(
            f"[{script}] Successfully synchronized {len(indexed_db)} candles. "
            f"Index 0: {indexed_db['0']['date']} | Index {len(indexed_db)-1}: {indexed_db[str(len(indexed_db)-1)]['date']}"
        )
    return success


def _parse_firebase_records(raw_data: dict | list | None) -> list[dict]:
    """Extracts existing records into standard list of dictionaries."""
    if not raw_data:
        return []
    records = []
    if isinstance(raw_data, list):
        for item in raw_data:
            if isinstance(item, dict) and "date" in item:
                records.append(item)
    elif isinstance(raw_data, dict):
        for k, v in raw_data.items():
            if isinstance(v, dict) and "date" in v:
                records.append(v)
    return records


def _merge_and_sort_records(existing: list[dict], new_records: list[dict]) -> list[dict]:
    """Combines datasets, eliminates duplicate dates, and sorts newest -> oldest."""
    by_date: dict[str, dict] = {}

    for row in existing:
        d = str(row.get("date", "")).strip()
        if d:
            by_date[d] = row

    # Overwrite/enrich with newest Yahoo data
    for row in new_records:
        d = str(row.get("date", "")).strip()
        if d:
            by_date[d] = row

    sorted_dates = sorted(by_date.keys(), reverse=True)  # Newest first
    return [by_date[d] for d in sorted_dates]


def validate_ohlc_payload(payload: dict[str, dict]) -> tuple[bool, str]:
    """Defensively checks integrity, record continuity, and mathematical correctness."""
    count = len(payload)
    if count == 0:
        return False, "Payload is empty"

    if count > TARGET_OHLC_COUNT:
        return False, f"Payload count {count} exceeds maximum {TARGET_OHLC_COUNT}"

    seen_dates = set()
    prev_date = None

    for i in range(count):
        key = str(i)
        if key not in payload:
            return False, f"Missing sequential key '{key}'"

        candle = payload[key]
        for field in ("date", "open", "high", "low", "close"):
            if field not in candle or candle[field] is None:
                return False, f"Key '{key}' missing required field '{field}'"

        d_str = candle["date"]
        if d_str in seen_dates:
            return False, f"Duplicate date '{d_str}' detected at index {key}"
        seen_dates.add(d_str)

        try:
            curr_date = datetime.strptime(d_str, "%Y-%m-%d").date()
        except ValueError:
            return False, f"Invalid date format '{d_str}' at index {key}"

        if prev_date and curr_date >= prev_date:
            return False, f"Date sort violation at index {key}: {curr_date} >= {prev_date}"
        prev_date = curr_date

        try:
            o = float(candle["open"])
            h = float(candle["high"])
            l = float(candle["low"])
            c = float(candle["close"])
        except (ValueError, TypeError):
            return False, f"Non-numeric price encountered at index {key}"

        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            return False, f"Non-positive price at index {key}: O={o}, H={h}, L={l}, C={c}"

        # Logical boundary verification with minor tolerance for floating point representations
        if h < (max(o, c) - 0.05) or l > (min(o, c) + 0.05) or h < l:
            return False, f"Logical OHLC price violation at index {key}: O={o}, H={h}, L={l}, C={c}"

    return True, "Valid"