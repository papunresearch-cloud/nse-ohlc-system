"""
PARAMETER CALCULATION MODULE (parameter.py)
Calculates technical indicators and performance metrics for active scripts.
- Source data: Reads OHLC historical series from Firebase /stocks/<script>
- Lookups: Reads 3yr value directly from /watchlist/detailedDb/<script>
- Output: Writes sanitized calculations to Firebase /param/<script>
- Stamping: Stores both date and time (IST) separately for each script
"""
import math
from datetime import datetime
import pytz
from typing import Any, Dict, List, Optional
from firebase_admin import db
from config import logger, TIMEZONE
from firebase_manager import sanitize_key

IST = pytz.timezone(TIMEZONE)

def safe_round(val: Any, decimals: int = 2) -> Any:
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
    if not candle or field not in candle:
        return None
    try:
        val = float(candle[field])
        return None if math.isnan(val) or math.isinf(val) else val
    except (ValueError, TypeError):
        return None

def fetch_ordered_candles(script: str) -> List[Dict[str, Any]]:
    safe_script = sanitize_key(script)
    ref = db.reference(f"stocks/{safe_script}")
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
    if len(closes_newest_first) < (period + 1):
        return "N/A"

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
    return round(100.0 - (100.0 / (1.0 + rs)), 2)

def calculate_single_script_parameters(script: str) -> bool:
    """Calculates all metrics for a single script and writes directly to /param/<script>."""
    safe_script = sanitize_key(script)
    candles = fetch_ordered_candles(safe_script)
    if not candles or len(candles) < 2:
        logger.warning(f"[PARAM] Insufficient candle history for {safe_script} (Count: {len(candles)})")
        return False

    now_ist = datetime.now(IST)
    current_date_str = now_ist.strftime("%d-%m-%Y")
    current_time_str = now_ist.strftime("%H:%M:%S")

    # Index 0 = Live candle, Index 1 = Previous trading session
    live_c = candles[0]
    cmp_val = parse_price(live_c, "close")
    prev_close = parse_price(candles[1], "close") if len(candles) > 1 else None

    closes = [parse_price(c, "close") for c in candles if parse_price(c, "close") is not None]

    # Moving averages
    ma10 = safe_round(sum(closes[:10]) / 10) if len(closes) >= 10 else "N/A"
    ma25 = safe_round(sum(closes[:25]) / 25) if len(closes) >= 25 else "N/A"
    ma50 = safe_round(sum(closes[:50]) / 50) if len(closes) >= 50 else "N/A"
    ma200 = safe_round(sum(closes[:200]) / 200) if len(closes) >= 200 else "N/A"

    # RSI
    rsi_val = calculate_rsi(closes, period=14)

    # Return percentages
    chg_today = safe_div(cmp_val - prev_close, prev_close) if cmp_val and prev_close else "N/A"

    # 3-year value from detailedDb
    three_yr_val = "N/A"
    try:
        det_data = db.reference(f"watchlist/detailedDb/{safe_script}").get() or {}
        three_yr_val = det_data.get("3yr", "N/A")
    except Exception:
        pass

    param_payload = {
        "Name": safe_script,
        "CMP": cmp_val or "N/A",
        "PREV_CLOSE": prev_close or "N/A",
        "%Chg (T)": chg_today,
        "10MA": ma10,
        "25MA": ma25,
        "50MA": ma50,
        "200MA": ma200,
        "RSI": rsi_val,
        "3yr": three_yr_val,
        "DATE": current_date_str,
        "TIME": current_time_str
    }

    try:
        db.reference(f"param/{safe_script}").set(param_payload)
        logger.info(f"[PARAM] Updated /param/{safe_script} at {current_time_str}")
        return True
    except Exception as e:
        logger.error(f"[PARAM] Failed writing /param/{safe_script}: {e}")
        return False

def update_all_parameters(scripts: List[str]):
    logger.info(f"[PARAM ENGINE] Executing calculation cycle across {len(scripts)} scripts...")
    for s in scripts:
        calculate_single_script_parameters(s)