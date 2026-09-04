"""
CHILD-2: Current-Day Live OHLC Update.
"""
from datetime import datetime
import pytz
from config import TIMEZONE, logger
from firebase_manager import update_live_candle, get_stock_ohlc
from yahoo_manager import download_intraday_today

IST = pytz.timezone(TIMEZONE)


def update_live_script(display_name: str, ticker: str) -> bool:
    """Updates only node /stocks/<display_name>/0 using live data from `ticker`."""
    candle = download_intraday_today(ticker)
    if not candle:
        logger.warning(f"[{display_name}] Live candle not retrieved for {ticker}")
        return False

    valid, err_msg = _validate_single_candle(candle)
    if not valid:
        logger.error(f"[{display_name}] Live candle rejected: {err_msg}")
        return False

    # Check date consistency against Index 1
    existing = get_stock_ohlc(display_name)
    if isinstance(existing, dict) and "1" in existing:
        idx1_date = existing["1"].get("date")
        if idx1_date and idx1_date >= candle["date"]:
            logger.error(
                f"[{display_name}] Date conflict: Live candle date {candle['date']} is not newer than Index 1 date {idx1_date}"
            )
            return False

    success = update_live_candle(display_name, candle)
    if success:
        logger.debug(f"[{display_name}] Live candle updated: CMP={candle['close']}")
    return success


def _validate_single_candle(c: dict) -> tuple[bool, str]:
    for field in ("date", "open", "high", "low", "close"):
        if field not in c or c[field] is None:
            return False, f"Missing field {field}"

    try:
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    except (ValueError, TypeError):
        return False, "Non-numeric price"

    if o <= 0 or h <= 0 or l <= 0 or cl <= 0:
        return False, "Non-positive price"

    if h < l or h < max(o, cl) or l > min(o, cl):
        return False, f"Logical violation: O={o}, H={h}, L={l}, C={cl}"

    return True, "Valid"