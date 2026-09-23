"""
PARAMETER CALCULATION MODULE (parameter.py)
Autonomous calculation engine:
1. Discovers every active stock from /stocklist, /watchlist, /display_list, /stocks, and fixed indices.
2. Identifies and prioritizes primary uppercase 'CODE'.
3. Calculates RSI, 10MA, 25MA, 50MA, 200MA, 52W Extremes, 100/50/25-day Highs & Lows,
   Periodic Returns, and 3yr metric.
4. Saves strictly in one place under the primary CODE: /param/<CODE>
"""

import math
import re
from datetime import datetime
import pytz
from typing import Any, Dict, List, Optional, Set, Tuple
from firebase_admin import db
from config import logger, TIMEZONE

IST = pytz.timezone(TIMEZONE)

FIXED_INDICES = {
    "NIFTY50": "^NSEI",
    "NIFTY100": "^CNX100",
    "NIFTYMID150": "NIFTYMIDCAP150.NS",
    "NIFTYSM250": "NIFTYSMLCAP250.NS"
}

_SCREENER_CACHE: Optional[Dict[str, Any]] = None


def sanitize_key(key: Any) -> str:
    """Removes or replaces forbidden Firebase Realtime Database path characters."""
    if not key:
        return ""
    return re.sub(r'[.#$\[\]/]', '', str(key)).strip().upper()


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


def get_screener_map() -> Dict[str, Any]:
    """Caches /SCREENER table indexed by CODE, Name, NSE, and BSE."""
    global _SCREENER_CACHE
    if _SCREENER_CACHE is not None:
        return _SCREENER_CACHE

    screener_map = {}
    try:
        raw_data = db.reference("SCREENER").get()
        records: List[Dict[str, Any]] = []

        if isinstance(raw_data, list):
            records = [r for r in raw_data if isinstance(r, dict)]
        elif isinstance(raw_data, dict):
            records = [v for v in raw_data.values() if isinstance(v, dict)]

        for rec in records:
            keys_to_index = [
                rec.get("CODE"),
                rec.get("NSE"),
                rec.get("BSE"),
                rec.get("Name")
            ]
            for k in keys_to_index:
                if k:
                    k_str = str(k).strip()
                    k_upper = k_str.upper()
                    screener_map[k_str] = rec
                    screener_map[k_upper] = rec
                    screener_map[sanitize_key(k_str)] = rec

        _SCREENER_CACHE = screener_map
    except Exception as e:
        logger.error(f"[PARAM] Failed to fetch /SCREENER table: {e}")
        _SCREENER_CACHE = {}

    return _SCREENER_CACHE


def fetch_detailed_metrics(aliases: List[str]) -> Dict[str, Any]:
    """Fetches static 3yr metric from /SCREENER or fallback to detailedDb safely without path crashes."""
    screener_map = get_screener_map()
    ret_3yr = None

    for alias in aliases:
        up = str(alias).strip().upper()
        clean = up.replace(".NS", "").replace(".BO", "")
        item = screener_map.get(up) or screener_map.get(clean) or screener_map.get(sanitize_key(up))
        if item:
            val = item.get("3yr") or item.get("3YR") or item.get("3Yr")
            if val is not None:
                ret_3yr = val
                break

    if ret_3yr is None:
        for alias in aliases:
            clean_alias = sanitize_key(alias)
            if not clean_alias:
                continue
            try:
                data = (
                    db.reference(f"watchlist/detailedDb/{clean_alias}").get()
                    or db.reference(f"detailedDb/{clean_alias}").get()
                )
                if isinstance(data, dict):
                    val = data.get("3yr") or data.get("3YR") or data.get("3Yr")
                    if val is not None:
                        ret_3yr = val
                        break
            except Exception as e:
                logger.debug(f"[PARAM] Could not fetch detailedDb for {clean_alias}: {e}")

    return {"3yr": safe_round(ret_3yr, 2)}


def fetch_candles_for_aliases(aliases: List[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Tries primary CODE and candidate aliases to retrieve OHLC candles from /stocks."""
    checked: Set[str] = set()
    for key in aliases:
        if not key:
            continue
        candidates = [sanitize_key(key), key]
        for candidate in candidates:
            if not candidate or candidate in checked:
                continue
            checked.add(candidate)

            if re.search(r'[.#$\[\]/]', candidate):
                continue

            stock_data = db.reference(f"stocks/{candidate}").get()
            if not stock_data:
                continue

            ordered: List[Dict[str, Any]] = []
            if isinstance(stock_data, list):
                ordered = [c for c in stock_data if isinstance(c, dict)]
            elif isinstance(stock_data, dict):
                for i in range(len(stock_data)):
                    idx_key = str(i)
                    if idx_key in stock_data and isinstance(stock_data[idx_key], dict):
                        ordered.append(stock_data[idx_key])
                    else:
                        break
            if len(ordered) >= 1:
                return ordered, candidate
    return [], None


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
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return safe_round(rsi, 2)


def calculate_sma(closes_newest_first: List[float], window: int) -> Any:
    if len(closes_newest_first) < window:
        return "N/A"
    sub_slice = closes_newest_first[:window]
    return safe_round(sum(sub_slice) / window, 2)


def process_target_group(aliases: List[str]) -> bool:
    clean_aliases = list(dict.fromkeys([str(a).strip() for a in aliases if a and str(a).strip()]))
    if not clean_aliases:
        return False

    primary_code = sanitize_key(clean_aliases[0])
    if not primary_code:
        return False

    candles, found_source_key = fetch_candles_for_aliases(clean_aliases)
    if not candles:
        logger.warning(f"[{primary_code}] Skipped: No candle history found in /stocks under {clean_aliases}")
        return False

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

    # Moving Averages
    ma10 = calculate_sma(closes, 10)
    ma25 = calculate_sma(closes, 25)
    ma50 = calculate_sma(closes, 50)
    ma200 = calculate_sma(closes, 200)

    # RSI
    rsi = calculate_rsi(closes, 14)

    # Extremes
    h52_slice = highs[:252]
    l52_slice = lows[:252]
    w52h = safe_round(max(h52_slice), 2) if len(h52_slice) >= 10 else "N/A"
    w52l = safe_round(min(l52_slice), 2) if len(l52_slice) >= 10 else "N/A"

    h100_slice = highs[:100]
    l100_slice = lows[:100]
    h100 = safe_round(max(h100_slice), 2) if len(h100_slice) >= 5 else "N/A"
    l100 = safe_round(min(l100_slice), 2) if len(l100_slice) >= 5 else "N/A"

    h50_slice = highs[:50]
    l50_slice = lows[:50]
    h50 = safe_round(max(h50_slice), 2) if len(h50_slice) >= 5 else "N/A"
    l50 = safe_round(min(l50_slice), 2) if len(l50_slice) >= 5 else "N/A"

    h25_slice = highs[:25]
    l25_slice = lows[:25]
    h25 = safe_round(max(h25_slice), 2) if len(h25_slice) >= 5 else "N/A"
    l25 = safe_round(min(l25_slice), 2) if len(l25_slice) >= 5 else "N/A"

    def get_close(idx: int) -> Optional[float]:
        return closes[idx] if len(closes) > idx else None

    c_1 = get_close(1)
    c_6 = get_close(6)
    c_21 = get_close(21)
    c_66 = get_close(66)
    c_121 = get_close(121)
    c_251 = get_close(251) or get_close(250)

    # Single-instance distinct returns
    chng_2dy = safe_div(close_0 - open_0, open_0) if (close_0 is not None and open_0 is not None) else "N/A"
    chng_ydy = safe_div(close_0 - c_1, c_1) if (close_0 is not None and c_1 is not None) else "N/A"

    ret_1wr = safe_div(c_1 - c_6, c_6) if (c_1 is not None and c_6 is not None) else "N/A"
    ret_1mr = safe_div(c_1 - c_21, c_21) if (c_1 is not None and c_21 is not None) else "N/A"
    ret_3mr = safe_div(c_1 - c_66, c_66) if (c_1 is not None and c_66 is not None) else "N/A"
    ret_6mr = safe_div(c_1 - c_121, c_121) if (c_1 is not None and c_121 is not None) else "N/A"
    ret_1yr = safe_div(c_1 - c_251, c_251) if (c_1 is not None and c_251 is not None) else "N/A"

    detailed_metrics = fetch_detailed_metrics(clean_aliases)

    now_ist = datetime.now(IST)
    current_time_str = now_ist.strftime("%H:%M:%S")
    current_date_str = str(c_0.get("date", now_ist.strftime("%Y-%m-%d"))) if c_0 else now_ist.strftime("%Y-%m-%d")

    # DEDUPLICATED CLEAN PAYLOAD
    payload = {
        "CODE": primary_code,
        "Name": primary_code,
        "CMP": close_0 if close_0 is not None else "N/A",
        "PREV_CLOSE": c_1 if c_1 is not None else "N/A",
        "DATE": current_date_str,
        "TIME": current_time_str,
        "updated_at": f"{current_date_str} {current_time_str}",
        "RSI": rsi,
        "10MA": ma10,
        "25MA": ma25,
        "50MA": ma50,
        "200MA": ma200,
        "52WH": w52h,
        "52WL": w52l,
        "100H": h100,
        "100L": l100,
        "50H": h50,
        "50L": l50,
        "25H": h25,
        "25L": l25,
        "CHG_DAY": chng_ydy,
        "CHG_INTRADAY": chng_2dy,
        "1W": ret_1wr,
        "1M": ret_1mr,
        "3M": ret_3mr,
        "6M": ret_6mr,
        "1YR": ret_1yr,
        "3YR": detailed_metrics["3yr"]
    }

    try:
        db.reference(f"param/{primary_code}").set(payload)
        logger.info(f"[{primary_code}] Successfully saved single record to /param/{primary_code}")
        return True
    except Exception as e:
        logger.error(f"[{primary_code}] Failed to save /param/{primary_code}: {e}")
        return False


def discover_all_system_targets() -> List[List[str]]:
    """Discovers targets across nodes and groups them by primary CODE."""
    targets: List[List[str]] = []

    for idx_code, idx_ticker in FIXED_INDICES.items():
        targets.append([idx_code, idx_ticker, sanitize_key(idx_code), sanitize_key(idx_ticker)])

    try:
        sl = db.reference("stocklist").get() or {}
        if isinstance(sl, dict):
            for s_code, s_ticker in sl.items():
                targets.append([sanitize_key(s_code), str(s_code).strip(), str(s_ticker).strip()])
    except Exception:
        pass

    try:
        det_db = db.reference("watchlist/detailedDb").get() or {}
        if isinstance(det_db, dict):
            for k, info in det_db.items():
                if isinstance(info, dict):
                    code_val = sanitize_key(info.get("CODE") or k)
                    aliases = [code_val, k, info.get("Name"), info.get("TICKER"), info.get("NSE")]
                    targets.append([str(a).strip() for a in aliases if a])
                else:
                    targets.append([sanitize_key(k), str(k).strip()])
    except Exception:
        pass

    try:
        raw_wl = db.reference("watchlist/watchlist").get() or db.reference("watchlist").get()
        if isinstance(raw_wl, list):
            for item in raw_wl:
                if isinstance(item, str):
                    clean_c = sanitize_key(item)
                    targets.append([clean_c, item.strip()])
                elif isinstance(item, dict):
                    c_id = sanitize_key(item.get("CODE") or item.get("Name"))
                    aliases = [c_id, item.get("CODE"), item.get("Name"), item.get("TICKER")]
                    targets.append([str(a).strip() for a in aliases if a])
    except Exception:
        pass

    try:
        dl = db.reference("display_list").get() or {}
        stk_list = dl.get("stocks", [])
        if isinstance(stk_list, list):
            for s in stk_list:
                if isinstance(s, str):
                    targets.append([sanitize_key(s), s.strip()])
                elif isinstance(s, dict):
                    c_id = sanitize_key(s.get("CODE") or s.get("Name"))
                    aliases = [c_id, s.get("CODE"), s.get("Name"), s.get("ticker"), s.get("TICKER")]
                    targets.append([str(a).strip() for a in aliases if a])
    except Exception:
        pass

    try:
        stocks_root = db.reference("stocks").shallow().get() or {}
        if isinstance(stocks_root, dict):
            for stock_k in stocks_root.keys():
                targets.append([sanitize_key(stock_k), stock_k])
    except Exception:
        pass

    merged: List[Set[str]] = []
    for grp in targets:
        grp_set = {str(x).strip() for x in grp if x and str(x).strip()}
        if not grp_set:
            continue
        found_idx = -1
        for idx, ex in enumerate(merged):
            if not ex.isdisjoint(grp_set):
                found_idx = idx
                break
        if found_idx >= 0:
            merged[found_idx].update(grp_set)
        else:
            merged.append(grp_set)

    result_groups: List[List[str]] = []
    for s in merged:
        items = list(s)
        items.sort(key=lambda x: ('.' in x, '^' in x, '_' in x, len(x)))
        result_groups.append(items)

    return result_groups


def update_all_parameters(scripts: Optional[List[str]] = None) -> None:
    """Executes parameters calculation and saves strictly to /param/<CODE>."""
    global _SCREENER_CACHE
    _SCREENER_CACHE = None
    get_screener_map()

    all_groups = discover_all_system_targets()
    logger.info(f"[PARAM ENGINE] Discovered {len(all_groups)} asset groups across database nodes.")

    success = 0
    for group in all_groups:
        try:
            if process_target_group(group):
                success += 1
        except Exception as e:
            logger.error(f"[{group[0]}] Parameter calculation error: {e}", exc_info=True)

    logger.info(f"[PARAM ENGINE] Completed: Updated {success}/{len(all_groups)} assets in /param.")


def calculate_single_script_parameters(script: str) -> bool:
    try:
        clean_code = sanitize_key(script)
        return process_target_group([clean_code, script])
    except Exception as e:
        logger.error(f"[PARAM] Error on single script {script}: {e}")
        return False


if __name__ == "__main__":
    from firebase_manager import init_firebase
    init_firebase()
    print("Initiating deduplicated parameter calculation (saving strictly under CODE in /param)...")
    update_all_parameters()
    print("Parameter calculation complete.")