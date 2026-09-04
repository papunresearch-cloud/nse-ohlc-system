"""
CHILD-2: Current-Day Live OHLC Update.
Responsible for:
- Fetching active intraday market values during trading hours.
- Overwriting ONLY index 0 without shifting historical records (indexes 1-249 remain intact).
- Price sanity checks to ensure no malformed data reaches Firebase.
"""
from datetime import datetime
import pytz
from config import TIMEZONE, logger
from firebase_manager import update_live_candle, get_stock_ohlc
from yahoo_manager import download_intraday_today

IST = pytz.timezone(TIMEZONE)


def update_live_script(script: str) -> bool:
    """Updates only node /stocks/<script>/0 with current market candle."""
    # 1. Fetch current intraday data
    candle = download_intraday_today(script)
    if not candle:
        logger.warning(f"[{script}] Unable to fetch current live candle from Yahoo")
        return False

    # 2. Validate candle structure
    valid, err_msg = _validate_single_candle(candle)
    if not valid:
        logger.error(f"[{script}] Live candle rejected: {err_msg}")
        return False

    # 3. Verify node 0 date consistency
    existing = get_stock_ohlc(script)
    if isinstance(existing, dict) and "1" in existing:
        idx1_date = existing["1"].get("date")
        if idx1_date and idx1_date >= candle["date"]:
            logger.error(
                f"[{script}] Live date conflict: Candle date {candle['date']} is not newer than Index 1 date {idx1_date}"
            )
            return False

    # 4. Perform localized write to Firebase
    success = update_live_candle(script, candle)
    if success:
        logger.debug(f"[{script}] Live candle updated: CMP={candle['close']} (O={candle['open']} H={candle['high']} L={candle['low']})")
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