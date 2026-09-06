"""
PARAMETER CALCULATION MODULE (parameter.py)
Calculates technical indicators and performance metrics for active scripts.
- Source data: Reads OHLC historical series from Firebase `/stocks/<script>`
- Lookups: Reads 1yr and 3yr values directly from `/watchlist/detailedDb/<script>`
- Output: Writes sanitized calculations to Firebase `/param/<script>`
- Formatting: Handles zero-division and missing data safely with "N/A"
"""

import math
from typing import Any, Dict, List, Optional
from firebase_admin import db
from config import logger


def safe_round(val: Any, decimals: int = 2) -> Any:
    """Rounds a numeric value or returns 'N/A' if NaN, infinite, or invalid."""
    if val is None or val == "N/A":
        return "N/A"
    try:
        f_val = float(val)
        if math.isnan(f_val) or math.isinf(f_val):
            return "N/A"
        return round(f_val, decimals)
    except (ValueError, TypeError):
        return "N/A"


def safe_div(numerator: Any, denominator: Any, factor: float = 100.0) -> Any:
    """Safely calculates (numerator / denominator) * factor; returns 'N/A' on zero-division or errors."""
    try:
        num = float(numerator)
        den = float(denominator)
        if den == 0.0 or math.isnan(den) or math.isnan(num):
            return "N/A"
        result = (num / den) * factor
        if math.isnan(result) or math.isinf(result):
            return "N/A"
        return round(result, 2)
    except (ValueError, TypeError, ZeroDivisionError):
        return "N/A"


def parse_price(candle: Optional[Dict[str, Any]], field: str) -> Optional[float]:
    """Extracts and verifies a floating price from an individual candle dictionary."""
    if not candle or field not in candle:
        return None
    try:
        val = float(candle[field])
        return None if math.isnan(val) or math.isinf(val) else val
    except (ValueError, TypeError):
        return None


def fetch_ordered_candles(script: str) -> List[Dict[str, Any]]:
    """
    Fetches historical candle collection under /stocks/<script>.
    Returns candles ordered chronologically backwards (index 0 is current, index 1 is yesterday, etc.).
    """
    ref = db.reference(f"stocks/{script}")
    stock_data = ref.get()
    if not stock_data:
        return []

    ordered_candles: List[Dict[str, Any]] = []
    if isinstance(stock_data, list):
        ordered_candles = [c for c in stock_data if isinstance(c, dict)]
    elif isinstance(stock_data, dict):
        for i in range(len(stock_data)):
            key = str(i)
            if key in stock_data and isinstance(stock_data[key], dict):
                ordered_candles.append(stock_data[key])
            else:
                break
    return ordered_candles


def calculate_rsi(closes_newest_first: List[float], period: int = 14) -> Any:
    """
    Calculates standard RSI using Exponential Moving Average on Close.
    Requires chronological order (oldest to newest).
    """
    if len(closes_newest_first) < (period + 1):
        return "N/A"

    # Reverse to oldest -> newest for sequential calculation
    chronological = list(reversed(closes_newest_first))
    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, len(chronological)):
        diff = chronological[i] - chronological[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    if len(gains) < period:
        return "N/A"

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    alpha = 1.0 / period
    for i in range(period, len(gains)):
        avg_gain = (alpha * gains[i]) + ((1.0 - alpha) * avg_gain)
        avg_loss = (alpha * losses[i]) + ((1.0 - alpha) * avg_loss)

    if avg_loss == 0.0:
        return 100.0 if avg_gain > 0.0 else 50.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return safe_round(rsi, 2)


def calculate_sma(closes_newest_first: List[float], window: int) -> Any:
    """Calculates Simple Moving Average over the requested window; returns 'N/A' if window is incomplete."""
    if len(closes_newest_first) < window:
        return "N/A"
    sub_slice = closes_newest_first[:window]
    return safe_round(sum(sub_slice) / window, 2)


def fetch_detailed_metrics(script: str) -> Dict[str, Any]:
    """Fetches static 1yr and 3yr metrics from /watchlist/detailedDb/<script> with case-insensitive fallback."""
    ref = db.reference(f"watchlist/detailedDb/{script}")
    data = ref.get() or {}

    ret_1yr = data.get("1yr") or data.get("1YR") or data.get("1Yr")
    ret_3yr = data.get("3yr") or data.get("3YR") or data.get("3Yr")

    return {
        "1yr": safe_round(ret_1yr, 2),
        "3yr": safe_round(ret_3yr, 2)
    }


def compute_script_parameters(script: str) -> Optional[Dict[str, Any]]:
    """Calculates all parameter metrics for a single script and commits them to /param/<script>."""
    candles = fetch_ordered_candles(script)
    if not candles:
        logger.warning(f"[{script}] No candle records found under /stocks/{script}.")
        return None

    c_0 = candles[0] if len(candles) > 0 else None
    close_0 = parse_price(c_0, "close")
    open_0 = parse_price(c_0, "open")

    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []

    for c in candles:
        cl = parse_price(c, "close")
        hi = parse_price(c, "high")
        lo = parse_price(c, "low")
        if cl is not None:
            closes.append(cl)
        if hi is not None:
            highs.append(hi)
        if lo is not None:
            lows.append(lo)

    # 1. Moving Averages
    ma10 = calculate_sma(closes, 10)
    ma25 = calculate_sma(closes, 25)
    ma50 = calculate_sma(closes, 50)
    ma200 = calculate_sma(closes, 200)

    # 2. RSI (14 days)
    rsi = calculate_rsi(closes, 14)

    # 3. 52-Week Extremes (250 sessions)
    h_slice = highs[:250]
    l_slice = lows[:250]
    w52h = safe_round(max(h_slice), 2) if len(h_slice) >= 10 else "N/A"
    w52l = safe_round(min(l_slice), 2) if len(l_slice) >= 10 else "N/A"

    # 4. Short/Medium Term Returns
    c_1 = closes[1] if len(closes) > 1 else None
    c_6 = closes[6] if len(closes) > 6 else None
    c_21 = closes[21] if len(closes) > 21 else None
    c_66 = closes[66] if len(closes) > 66 else None
    c_121 = closes[121] if len(closes) > 121 else None

    chng_2dy = safe_div(close_0 - open_0, open_0) if (close_0 is not None and open_0 is not None) else "N/A"
    chng_ydy = safe_div(close_0 - c_1, c_1) if (close_0 is not None and c_1 is not None) else "N/A"

    ret_1wr = safe_div(c_1 - c_6, c_6) if (c_1 is not None and c_6 is not None) else "N/A"
    ret_1mr = safe_div(c_1 - c_21, c_21) if (c_1 is not None and c_21 is not None) else "N/A"
    ret_3mr = safe_div(c_1 - c_66, c_66) if (c_1 is not None and c_66 is not None) else "N/A"
    ret_6mr = safe_div(c_1 - c_121, c_121) if (c_1 is not None and c_121 is not None) else "N/A"

    # 5. Long-term returns from /watchlist/detailedDb/<script>
    detailed_metrics = fetch_detailed_metrics(script)

    payload = {
        "RSI": rsi,
        "10ma": ma10,
        "25ma": ma25,
        "50ma": ma50,
        "200ma": ma200,
        "52wh": w52h,
        "52wl": w52l,
        "2dy-%chng": chng_2dy,
        "Ydy-%chng": chng_ydy,
        "1wr": ret_1wr,
        "1mr": ret_1mr,
        "3mr": ret_3mr,
        "6mr": ret_6mr,
        "1yr": detailed_metrics["1yr"],
        "3yr": detailed_metrics["3yr"],
        "updated_at": c_0.get("date", "N/A") if c_0 else "N/A"
    }

    db.reference(f"param/{script}").set(payload)
    logger.info(f"[{script}] Parameters saved to /param/{script}")
    return payload


def update_all_parameters(scripts: List[str]) -> None:
    """Iterates through all registered active scripts to calculate and save parameters."""
    logger.info(f"[PARAM ENGINE] Executing 15-minute calculation cycle across {len(scripts)} scripts...")
    for script in scripts:
        try:
            compute_script_parameters(script)
        except Exception as e:
            logger.error(f"[{script}] Failed to process parameters: {e}", exc_info=True)