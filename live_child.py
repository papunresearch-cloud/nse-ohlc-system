"""
CHILD-2: Live Intraday OHLC Updater.
- Responsible exclusively for writing to /stocks/<display_name>/0.
- Never modifies or shifts historical records (keys '1' through '250').
- Performs price sanity verification and temporal checks against Index 1.
"""
from datetime import datetime
import pytz

from config import TIMEZONE, logger
from firebase_manager import update_live_candle, get_stock_ohlc, clear_live_candle
from yahoo_manager import download_intraday_today

IST = pytz.timezone(TIMEZONE)


def update_live_script(display_name: str, ticker: str) -> bool:
    """Updates only node /stocks/<display_name>/0 with the latest intraday candle."""
    candle = download_intraday_today(ticker)
    if not candle:
        logger.warning(f"[{display_name}] Unable to fetch current live candle for {ticker}")
        return False

    valid, err_msg = _validate_single_candle(candle)
    if not valid:
        logger.error(f"[{display_name}] Live candle rejected: {err_msg}")
        return False

    # Temporal continuity check: Live date must not be older than Index 1 date
    existing = get_stock_ohlc(display_name)
    if isinstance(existing, dict) and "1" in existing:
        idx1_date = existing["1"].get("date")
        if idx1_date and str(candle["date"]) < str(idx1_date):
            logger.error(
                f"[{display_name}] Date conflict: Live date {candle['date']} is older than Index 1 ({idx1_date})"
            )
            return False

    success = update_live_candle(display_name, candle)
    if success:
        logger.debug(f"[{display_name}] Index 0 updated: CMP={candle['close']} (O={candle['open']} H={candle['high']} L={candle['low']})")
    return success


def reset_index_zero(display_name: str) -> bool:
    """Sets index 0 to None/empty during non-market windows."""
    return clear_live_candle(display_name)


def _validate_single_candle(c: dict) -> tuple[bool, str]:
    """Validates that a single intraday candle has numeric and logically sound values."""
    for field in ("date", "open", "high", "low", "close"):
        if field not in c or c[field] is None:
            return False, f"Missing field '{field}'"

    try:
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    except (ValueError, TypeError):
        return False, "Non-numeric price"

    if o <= 0 or h <= 0 or l <= 0 or cl <= 0:
        return False, "Non-positive price"

    # Candle logic check with 0.05 rounding buffer
    if (h < l) or (h < max(o, cl) - 0.05) or (l > min(o, cl) + 0.05):
        return False, f"Logical boundary violation: O={o}, H={h}, L={l}, C={cl}"

    return True, "Valid"