"""
CHILD-1: Historical Synchronization Engine.
- Maintains up to 250 historical records under keys '1' through '250'.
- Decouples historical verification from live-tracking permission.
- Features a 7-day NSE Bhavcopy fallback bridge to immediately resolve Yahoo vendor lag.
- Sanitizes NaN/Inf floating point prices for Firebase JSON compliance.
- Never mutates Index '0' (reserved exclusively for CHILD-2 live updates).
"""
import io
import math
from datetime import datetime, date, timedelta
import pandas as pd
import pytz
import requests

from config import (
    TARGET_OHLC_COUNT,
    HISTORICAL_START_INDEX,
    DEFAULT_SAFETY_MARGIN,
    TIMEZONE,
    logger
)
from firebase_manager import get_stock_ohlc, write_full_ohlc
from yahoo_manager import download_historical_daily
from market_calendar import MarketCalendar

IST = pytz.timezone(TIMEZONE)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# =====================================================================
# NSE BHAVCOPY DIRECT FALLBACK (7-DAY BRIDGE)
# =====================================================================

def fetch_nse_bhavcopy_candle(ticker: str, trade_date: date) -> dict | None:
    """
    Downloads the official full Bhavcopy CSV from NSE archives for a specific date
    and extracts authentic OHLC metrics for the specified ticker.
    """
    date_str = trade_date.strftime("%d%m%Y")
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"

    try:
        resp = requests.get(url, headers=NSE_HEADERS, timeout=12)
        if resp.status_code != 200:
            return None

        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]

        if "SERIES" in df.columns:
            df = df[df["SERIES"].str.strip() == "EQ"]

        # Clean symbol by stripping .NS extension
        clean_symbol = ticker.replace(".NS", "").strip().upper()
        match = df[df["SYMBOL"].str.strip().str.upper() == clean_symbol]

        if match.empty:
            return None

        row = match.iloc[0]
        o = _clean_price(row.get("OPEN_PRICE"))
        h = _clean_price(row.get("HIGH_PRICE"))
        l = _clean_price(row.get("LOW_PRICE"))
        c = _clean_price(row.get("CLOSE_PRICE"))
        vol_raw = row.get("TTL_TRD_QNTY", 0)

        try:
            volume_val = int(vol_raw) if pd.notnull(vol_raw) else 0
        except (ValueError, TypeError):
            volume_val = 0

        # Validate candle logic
        if o <= 0 or h <= 0 or l <= 0 or c <= 0 or (h < l):
            return None

        return {
            "date": trade_date.strftime("%Y-%m-%d"),
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(l, 2),
            "close": round(c, 2),
            "volume": volume_val
        }
    except Exception as e:
        logger.debug(f"[NSE-BHAVCOPY] Failed query for {ticker} on {trade_date}: {e}")
        return None


def patch_recent_vendor_lag(merged_records: list[dict], ticker: str, calendar: MarketCalendar, expected_latest_date: date) -> list[dict]:
    """
    Walks backward over the 7 most recent trading days.
    If any session is missing from Yahoo's feed, it fetches and injects the candle directly from NSE Bhavcopy.
    """
    if not ticker.upper().endswith(".NS"):
        return merged_records

    existing_dates = {r["date"] for r in merged_records if r.get("date")}
    curr = expected_latest_date
    checked_days = 0
    injected_count = 0

    while checked_days < 7:
        if calendar.is_trading_day(curr):
            checked_days += 1
            curr_str = curr.strftime("%Y-%m-%d")

            if curr_str not in existing_dates:
                bhav_bar = fetch_nse_bhavcopy_candle(ticker, curr)
                if bhav_bar:
                    merged_records.append(bhav_bar)
                    existing_dates.add(curr_str)
                    injected_count += 1
                    logger.info(f"[{ticker}] Successfully bridged missing session {curr_str} via NSE Bhavcopy.")

        curr -= timedelta(days=1)

    if injected_count > 0:
        # Re-sort descending: newest date at index 0
        merged_records = sorted(merged_records, key=lambda x: x["date"], reverse=True)

    return merged_records


# =====================================================================
# SYNCHRONIZATION WORKER CORE
# =====================================================================

def sync_historical_script(display_name: str, ticker: str, gap_trading_days: int = 0, calendar: MarketCalendar = None) -> tuple[bool, str]:
    """
    Coordinates historical catch-up and returns live-readiness status:
    - Returns (True, msg) if stock is safe for live updates (VERIFIED, RECOVERED, or VENDOR_LAG with valid baseline).
    - Returns (False, msg) only if baseline is completely missing or structurally invalid.
    """
    try:
        if calendar is None:
            calendar = MarketCalendar()

        now_ist = datetime.now(IST)
        expected_latest_date = _get_expected_latest_date(calendar, now_ist)
        expected_latest_str = expected_latest_date.strftime("%Y-%m-%d")

        existing_ohlc = get_stock_ohlc(display_name)
        existing_records = _parse_firebase_historical_records(existing_ohlc)

        # 1. Inspect Current Firebase Baseline
        if _audit_vendor_freshness(existing_records, expected_latest_str):
            return True, f"VERIFIED: Firebase baseline already current at {expected_latest_str}"

        # 2. Determine Fetch Depth & Query Vendor
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

        df = download_historical_daily(ticker, days_needed=days_needed)
        merged_records = _merge_and_sort_records(existing_records, df, calendar, now_ist)

        sync_state = "UNKNOWN"

        # 3. Check Freshness & Attempt Capped Deep-Fetch Retry
        if _audit_vendor_freshness(merged_records, expected_latest_str):
            sync_state = "VERIFIED"
        else:
            if not is_empty_bootstrap:
                logger.warning(
                    f"[{display_name}] Vendor lagging (Got: {merged_records[0]['date'] if merged_records else 'None'}, "
                    f"Expected: {expected_latest_str}). Retrying with 30-day fetch..."
                )
                df_deep = download_historical_daily(ticker, days_needed=30)
                merged_records = _merge_and_sort_records(existing_records, df_deep, calendar, now_ist)

                if _audit_vendor_freshness(merged_records, expected_latest_str):
                    sync_state = "RECOVERED"
            
            # 3B. 7-Day Exchange Fallback Bridge
            if sync_state not in ("VERIFIED", "RECOVERED"):
                merged_records = patch_recent_vendor_lag(merged_records, ticker, calendar, expected_latest_date)
                if _audit_vendor_freshness(merged_records, expected_latest_str):
                    sync_state = "RECOVERED_BHAVCOPY"
                else:
                    sync_state = "VENDOR_LAG"

        # 4. Commit Fresh Records (VERIFIED / RECOVERED / RECOVERED_BHAVCOPY)
        if sync_state in ("VERIFIED", "RECOVERED", "RECOVERED_BHAVCOPY"):
            final_candles = merged_records[:TARGET_OHLC_COUNT]
            indexed_db = {str(idx + HISTORICAL_START_INDEX): c for idx, c in enumerate(final_candles)}

            valid, err_msg = validate_historical_payload(indexed_db)
            if not valid:
                logger.error(f"[{display_name}] Sanity validation rejected: {err_msg}. Firebase untouched.")
                return False, f"Validation Rejected: {err_msg}"

            if write_full_ohlc(display_name, indexed_db):
                return True, f"{sync_state}: {len(indexed_db)} bars committed (Index 1: {expected_latest_str})"
            return False, "Firebase write failed"

        # 5. Handle VENDOR_LAG: Decouple Live Permission & Support Initial Bootstrap
        valid_existing, _ = validate_historical_payload(
            {str(idx + HISTORICAL_START_INDEX): c for idx, c in enumerate(existing_records)}
        ) if existing_records else (False, "No records")

        # Condition A: Preserved valid baseline exists in Firebase
        if valid_existing:
            stale_date = existing_records[0].get("date")
            logger.warning(
                f"[{display_name}] VENDOR_LAG: Session {expected_latest_str} omitted. "
                f"Firebase untouched (retaining baseline from {stale_date}). CHILD-2 live tracking permitted."
            )
            return True, f"VENDOR_LAG: Baseline preserved at {stale_date}; Live tracking allowed"

        # Condition B: If Firebase is empty, seed with best available vendor records
        if is_empty_bootstrap and merged_records:
            final_candles = merged_records[:TARGET_OHLC_COUNT]
            indexed_db = {str(idx + HISTORICAL_START_INDEX): c for idx, c in enumerate(final_candles)}
            valid, err_msg = validate_historical_payload(indexed_db)
            if valid and write_full_ohlc(display_name, indexed_db):
                seeded_date = indexed_db[str(HISTORICAL_START_INDEX)]["date"]
                logger.warning(
                    f"[{display_name}] VENDOR_LAG_BOOTSTRAP: Initial baseline seeded with {len(indexed_db)} bars "
                    f"(Latest available: {seeded_date}). CHILD-2 live tracking permitted."
                )
                return True, f"VENDOR_LAG: Bootstrapped at {seeded_date}; Live tracking allowed"

        # Hard failure: vendor returned nothing usable and no baseline exists
        msg = f"INITIAL_SYNC_FAILED: Missing {expected_latest_str} and no usable data returned."
        logger.error(f"[{display_name}] {msg}. Firebase untouched.")
        return False, msg

    except Exception as e:
        logger.error(f"[{display_name}] Internal sync exception: {e}", exc_info=True)
        return False, f"Exception: {str(e)}"


# =====================================================================
# DATA EXTRACTION & MERGE HELPERS
# =====================================================================

def _clean_price(val) -> float:
    """Converts price to clean float, replacing NaN/Inf with 0.0."""
    try:
        f = float(val)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return 0.0


def _get_expected_latest_date(calendar: MarketCalendar, now_ist: datetime) -> date:
    """Calculates the date of the latest fully finalized market session."""
    today = now_ist.date()
    if calendar.is_trading_day(today):
        status, _ = calendar.get_market_status(now_ist)
        if status in ("LIVE", "PRE_OPEN"):
            return calendar.get_previous_trading_day(today)
        return today
    return calendar.get_previous_trading_day(today)


def _audit_vendor_freshness(records: list[dict], expected_date_str: str) -> bool:
    """Checks whether the newest record matches the expected completed date."""
    if not records:
        return False
    return records[0].get("date") == expected_date_str


def _parse_firebase_historical_records(raw_data) -> list[dict]:
    """Extracts only historical keys starting at HISTORICAL_START_INDEX up to TARGET_OHLC_COUNT, discarding '0'."""
    if not raw_data or not isinstance(raw_data, dict):
        return []
    records = []
    for i in range(HISTORICAL_START_INDEX, HISTORICAL_START_INDEX + TARGET_OHLC_COUNT):
        k = str(i)
        if k in raw_data and isinstance(raw_data[k], dict) and "date" in raw_data[k]:
            records.append(raw_data[k])
    return records


def _merge_and_sort_records(existing_records: list[dict], df: pd.DataFrame, calendar: MarketCalendar, now_ist: datetime) -> list[dict]:
    """Combines existing records with downloaded dataframe, deduplicating strictly by date."""
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

            o = _clean_price(row.get("open", 0.0))
            h = _clean_price(row.get("high", 0.0))
            l = _clean_price(row.get("low", 0.0))
            c = _clean_price(row.get("close", 0.0))

            if o <= 0 or h <= 0 or l <= 0 or c <= 0:
                continue

            date_map[d_str] = {
                "date": d_str,
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(l, 2),
                "close": round(c, 2),
                "volume": volume_val
            }

    # CRITICAL: Exclude ongoing session from historical series (1-250) during market hours
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
    """Validates sequential keys starting at HISTORICAL_START_INDEX, price integrity, and descending dates."""
    count = len(payload)
    if count == 0:
        return False, "Historical payload is completely empty"
    if count > TARGET_OHLC_COUNT:
        return False, f"Payload count {count} exceeds limit {TARGET_OHLC_COUNT}"

    dates_seen = []
    for i in range(HISTORICAL_START_INDEX, HISTORICAL_START_INDEX + count):
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