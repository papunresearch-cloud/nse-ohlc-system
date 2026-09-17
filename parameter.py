"""
PARAMETER CALCULATION MODULE (parameter.py)
Fully autonomous calculation engine:
1. Discovers every active stock from /watchlist, /display_list, /stocks, and fixed indices.
2. Calculates RSI, 10MA, 25MA, 50MA, 200MA, 52W Extremes, Periodic Returns, and 3yr from SCREENER.
3. Multi-writes to /param across raw, sanitized, and symbol aliases so frontend fetches never miss.
"""

import math
from datetime import datetime
import pytz
from typing import Any, Dict, List, Optional, Set, Tuple
from firebase_admin import db
from config import logger, TIMEZONE
from firebase_manager import sanitize_key, FIXED_INDICES

IST = pytz.timezone(TIMEZONE)

_SCREENER_CACHE: Optional[Dict[str, Any]] = None


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
    """Caches /SCREENER array to map records by Name, NSE, BSE, and CODE."""
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
                rec.get("Name"),
                rec.get("NSE"),
                rec.get("BSE"),
                rec.get("CODE")
            ]
            for k in keys_to_index:
                if k:
                    k_str = str(k).strip().upper()
                    screener_map[k_str] = rec
                    screener_map[sanitize_key(k_str)] = rec

        _SCREENER_CACHE = screener_map
    except Exception as e:
        logger.error(f"[PARAM] Failed to fetch /SCREENER table: {e}")
        _SCREENER_CACHE = {}

    return _SCREENER_CACHE


def fetch_detailed_metrics(aliases: List[str]) -> Dict[str, Any]:
    """Fetches static 3yr metric primarily from /SCREENER with fallback to detailedDb."""
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
            data = (
                db.reference(f"watchlist/detailedDb/{alias}").get()
                or db.reference(f"detailedDb/{alias}").get()
                or db.reference(f"watchlist/detailedDb/{sanitize_key(alias)}").get()
                or db.reference(f"detailedDb/{sanitize_key(alias)}").get()
            )
            if isinstance(data, dict):
                val = data.get("3yr") or data.get("3YR") or data.get("3Yr")
                if val is not None:
                    ret_3yr = val
                    break

    return {"3yr": safe_round(ret_3yr, 2)}


def fetch_candles_for_aliases(aliases: List[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Tries all alias variations to find candles in /stocks."""
    for key in aliases:
        if not key:
            continue
        for candidate in [key, sanitize_key(key)]:
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

    candles, found_source_key = fetch_candles_for_aliases(clean_aliases)
    primary_name = clean_aliases[0]

    if not candles:
        logger.warning(f"[{primary_name}] Skipped: No candle history found in /stocks under {clean_aliases}")
        return False

    c_0 = candles[0] if len(candles) > 0 else None
    c_1_candle = candles[1] if len(candles) > 1 else None

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

    # 52-Week Extremes
    h_slice = highs[:252]
    l_slice = lows[:252]
    w52h = safe_round(max(h_slice), 2) if len(h_slice) >= 10 else "N/A"
    w52l = safe_round(min(l_slice), 2) if len(l_slice) >= 10 else "N/A"

    def get_close(idx: int) -> Optional[float]:
        return closes[idx] if len(closes) > idx else None

    c_1 = get_close(1)
    c_6 = get_close(6)
    c_21 = get_close(21)
    c_66 = get_close(66)
    c_121 = get_close(121)
    c_251 = get_close(251) or get_close(250)

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

    payload = {
        # Old File Keys (Stock_window.jsx and Index_window.jsx primary)
        "date": current_date_str,
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
        "1yr": ret_1yr,
        "3yr": detailed_metrics["3yr"],
        "updated_at": f"{current_date_str} {current_time_str}",

        # Uppercase Schema Aliases
        "Name": primary_name,
        "CMP": close_0 if close_0 is not None else "N/A",
        "PREV_CLOSE": c_1 if c_1 is not None else "N/A",
        "Tdy-%chng": chng_2dy,
        "%Chg (T)": chng_ydy,
        "10MA": ma10,
        "25MA": ma25,
        "50MA": ma50,
        "200MA": ma200,
        "52WH": w52h,
        "52WL": w52l,
        "1W": ret_1wr,
        "1M": ret_1mr,
        "3M": ret_3mr,
        "1YR": ret_1yr,
        "3YR": detailed_metrics["3yr"],
        "DATE": current_date_str,
        "TIME": current_time_str
    }

    # 1. Mirror payload across all alias keys in /param
    write_keys: Set[str] = set()
    for a in clean_aliases:
        write_keys.add(a)
        write_keys.add(sanitize_key(a))

    for wkey in write_keys:
        try:
            db.reference(f"param/{wkey}").set(payload)
        except Exception:
            pass

    # 2. Mirror indices if applicable
    for a in clean_aliases:
        if a in FIXED_INDICES:
            try:
                db.reference(f"indices/{a}").set(payload)
                db.reference(f"indices/{sanitize_key(a)}").set(payload)
            except Exception:
                pass

    # 3. Ensure live /stocks nodes exist for every alias so c0/c1 fetches resolve
    for alias in clean_aliases:
        if alias != found_source_key:
            try:
                if c_0:
                    ref_idx0 = db.reference(f"stocks/{alias}/0")
                    if not ref_idx0.get():
                        ref_idx0.set(c_0)
                if c_1_candle:
                    ref_idx1 = db.reference(f"stocks/{alias}/1")
                    if not ref_idx1.get():
                        ref_idx1.set(c_1_candle)
            except Exception:
                pass

    logger.info(f"[{primary_name}] Successfully updated /param across: {list(write_keys)}")
    return True


def discover_all_system_targets() -> List[List[str]]:
    """Discovers every target group across /stocks, /watchlist, /display_list, and FIXED_INDICES."""
    targets: List[List[str]] = []

    # 1. Fixed market indices
    for idx_name, idx_ticker in FIXED_INDICES.items():
        targets.append([idx_name, idx_ticker, sanitize_key(idx_name), sanitize_key(idx_ticker)])

    # 2. All stocks currently populated under /stocks
    try:
        stocks_root = db.reference("stocks").shallow().get() or {}
        if isinstance(stocks_root, dict):
            for stock_k in stocks_root.keys():
                targets.append([stock_k, sanitize_key(stock_k)])
    except Exception:
        pass

    # 3. Watchlist detailedDb
    try:
        det_db = db.reference("watchlist/detailedDb").get() or {}
        if isinstance(det_db, dict):
            for k, info in det_db.items():
                if isinstance(info, dict):
                    aliases = [k, info.get("Name"), info.get("TICKER"), info.get("NSE"), info.get("CODE")]
                    targets.append([str(a).strip() for a in aliases if a])
                else:
                    targets.append([str(k).strip()])
    except Exception:
        pass

    # 4. Watchlist array
    try:
        raw_wl = db.reference("watchlist/watchlist").get() or db.reference("watchlist").get()
        if isinstance(raw_wl, list):
            for item in raw_wl:
                if isinstance(item, str):
                    targets.append([item.strip(), f"{item.strip()}.NS"])
                elif isinstance(item, dict):
                    aliases = [item.get("Name"), item.get("TICKER"), item.get("NSE")]
                    targets.append([str(a).strip() for a in aliases if a])
    except Exception:
        pass

    # 5. Front page display_list
    try:
        dl = db.reference("display_list").get() or {}
        stk_list = dl.get("stocks", [])
        if isinstance(stk_list, list):
            for s in stk_list:
                if isinstance(s, str):
                    targets.append([s.strip()])
                elif isinstance(s, dict):
                    aliases = [s.get("Name"), s.get("ticker"), s.get("TICKER")]
                    targets.append([str(a).strip() for a in aliases if a])
    except Exception:
        pass

    # 6. Existing /stocklist
    try:
        sl = db.reference("stocklist").get() or {}
        if isinstance(sl, dict):
            for s_name, s_ticker in sl.items():
                targets.append([str(s_name).strip(), str(s_ticker).strip()])
    except Exception:
        pass

    # Merge overlapping alias sets
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

    return [list(s) for s in merged]


def update_all_parameters(scripts: Optional[List[str]] = None) -> None:
    """Executes parameters cycle across all discovered system assets."""
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

    logger.info(f"[PARAM ENGINE] Completed: Updated {success}/{len(all_groups)} assets.")


def calculate_single_script_parameters(script: str) -> bool:
    try:
        return process_target_group([script, sanitize_key(script)])
    except Exception as e:
        logger.error(f"[PARAM] Error on single script {script}: {e}")
        return False


if __name__ == "__main__":
    from firebase_manager import init_firebase
    init_firebase()
    print("Initiating full multi-target parameter calculation...")
    update_all_parameters()
    print("Parameter calculation complete.")