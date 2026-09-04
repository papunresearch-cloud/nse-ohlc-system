"""
CHILD-1: Historical Synchronization.
"""
from datetime import datetime, date
import pandas as pd
from config import TARGET_OHLC_COUNT, DEFAULT_SAFETY_MARGIN, logger
from firebase_manager import get_stock_ohlc, write_full_ohlc
from yahoo_manager import download_historical_daily
from market_calendar import MarketCalendar


def sync_historical_script(display_name: str, ticker: str, gap_trading_days: int, calendar: MarketCalendar) -> bool:
    """
    Downloads historical data using `ticker` and saves to `/stocks/<display_name>`.
    """
    logger.info(f"[{display_name}] Historical sync (Ticker: {ticker}, Gap: {gap_trading_days} days)...")
    
    # 1. Read existing records from Firebase under display name
    existing_data = get_stock_ohlc(display_name)
    existing_list = _parse_firebase_records(existing_data)

    # 2. Determine fetch volume
    if not existing_list or len(existing_list) < TARGET_OHLC_COUNT:
        logger.info(f"[{display_name}] Initializing full baseline (~250 days)...")
        days_to_download = TARGET_OHLC_COUNT + DEFAULT_SAFETY_MARGIN
    else:
        days_to_download = gap_trading_days + DEFAULT_SAFETY_MARGIN
        logger.info(f"[{display_name}] Fetching {days_to_download} days (Gap {gap_trading_days} + Margin {DEFAULT_SAFETY_MARGIN})...")

    # 3. Download using authentic Yahoo ticker
    yahoo_df = download_historical_daily(ticker, days_needed=days_to_download)
    if yahoo_df.empty:
        logger.error(f"[{display_name}] Failed to obtain historical records for {ticker}. Aborting sync.")
        return False

    yahoo_list = yahoo_df.to_dict(orient="records")

    # 4. Merge, normalize, and sort
    merged_records = _merge_and_sort_records(existing_list, yahoo_list)

    if len(merged_records) == 0:
        logger.warning(f"[{display_name}] No valid merged records created.")
        return False

    final_candles = merged_records[:TARGET_OHLC_COUNT]

    # 5. Build 0-249 schema
    indexed_db = {}
    for idx, candle in enumerate(final_candles):
        indexed_db[str(idx)] = {
            "date": str(candle["date"]),
            "open": round(float(candle["open"]), 2),
            "high": round(float(candle["high"]), 2),
            "low": round(float(candle["low"]), 2),
            "close": round(float(candle["close"]), 2)
        }

    # 6. Validate
    valid, err_msg = validate_ohlc_payload(indexed_db)
    if not valid:
        logger.error(f"[{display_name}] Validation failed: {err_msg}. Firebase untouched.")
        return False

    # 7. Write to Firebase
    success = write_full_ohlc(display_name, indexed_db)
    if success:
        logger.info(
            f"[{display_name}] Synced {len(indexed_db)} candles. "
            f"Index 0: {indexed_db['0']['date']} | Index {len(indexed_db)-1}: {indexed_db[str(len(indexed_db)-1)]['date']}"
        )
    return success


def _parse_firebase_records(raw_data: dict | list | None) -> list[dict]:
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
    by_date = {}
    for row in existing:
        d = str(row.get("date", "")).strip()
        if d:
            by_date[d] = row

    for row in new_records:
        d = str(row.get("date", "")).strip()
        if d:
            by_date[d] = row

    sorted_dates = sorted(by_date.keys(), reverse=True)
    return [by_date[d] for d in sorted_dates]


def validate_ohlc_payload(payload: dict[str, dict]) -> tuple[bool, str]:
    count = len(payload)
    if count == 0:
        return False, "Payload is empty"

    seen_dates = set()
    prev_date = None

    for i in range(count):
        key = str(i)
        if key not in payload:
            return False, f"Missing sequential key '{key}'"

        candle = payload[key]
        for field in ("date", "open", "high", "low", "close"):
            if field not in candle or candle[field] is None:
                return False, f"Key '{key}' missing field '{field}'"

        d_str = candle["date"]
        if d_str in seen_dates:
            return False, f"Duplicate date '{d_str}' at index {key}"
        seen_dates.add(d_str)

        try:
            curr_date = datetime.strptime(d_str, "%Y-%m-%d").date()
        except ValueError:
            return False, f"Invalid date '{d_str}'"

        if prev_date and curr_date >= prev_date:
            return False, f"Date sorting violation at index {key}: {curr_date} >= {prev_date}"
        prev_date = curr_date

        try:
            o, h, l, c = float(candle["open"]), float(candle["high"]), float(candle["low"]), float(candle["close"])
        except (ValueError, TypeError):
            return False, f"Non-numeric price at index {key}"

        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            return False, f"Non-positive price at index {key}"

        if h < (max(o, c) - 0.05) or l > (min(o, c) + 0.05) or h < l:
            return False, f"OHLC price violation at index {key}"

    return True, "Valid"