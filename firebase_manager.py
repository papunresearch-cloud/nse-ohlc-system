"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management[cite: 1, 4].
- Dynamic stock discovery pulling tickers from /param, /detailedDb, and /watchlist[cite: 2, 4].
- Safely handles stringified lists and nested nodes.
- Permanent anchoring of 4 master market indices (immune to purging/deletion)[cite: 4].
- In-memory calendar caching and urllib3 warning suppression.
- Guarded OHLC access for Child-1 (1 to 250) and Child-2 (index 0)[cite: 1, 4].
"""
import os
import ast
import json
import logging
import firebase_admin
from firebase_admin import credentials, db
from config import (
    logger,
    FIREBASE_CREDENTIALS,
    FIREBASE_DATABASE_URL,
    PATH_STOCKS,
    PATH_SCRIPTS,
    TARGET_OHLC_COUNT
)

# 1. Suppress connection pool drop warnings
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

_CALENDAR_CACHE = None

# Permanent master indices (immune to deletion)[cite: 4]
FIXED_INDICES = {
    "NIFTY50": "^NSEI",
    "NIFTY100": "^CNX100",
    "NIFTY MIDCAP 150": "NIFTYMIDCAP150.NS",
    "NIFTY SMALLCAP 250": "NIFTYSMLCAP250.NS"
}

# Known ticker directory for NSE stocks[cite: 8]
KNOWN_TICKERS = {
    "Bank of Maha": "MAHABANK.NS",
    "Bharat Electron": "BEL.NS",
    "Black Box": "BBOX.NS",
    "Caplin Point Lab": "CAPLIPOINT.NS",
    "Coal India": "COALINDIA.NS",
    "Dixon Technolog_": "DIXON.NS",
    "Dixon Technolog.": "DIXON.NS",
    "Dixon Technologies": "DIXON.NS",
    "Gokul Agro": "GOKULAGRO.NS",
    "Gravita India": "GRAVITA.NS",
    "HCL Technologies": "HCLTECH.NS",
    "REC Ltd": "RECLTD.NS",
    "Suzlon Energy": "SUZLON.NS",
    "Transport Corp.": "TCI.NS",
    "Transport Corp": "TCI.NS",
    "Va Tech Wabag": "WABAG.NS",
    "Abbott India": "ABBOTINDIA.NS",
    "Infosys": "INFY.NS",
    "Reliance Industries": "RELIANCE.NS",
    "TCS": "TCS.NS",
    "Vedanta": "VEDL.NS",
    "Wipro": "WIPRO.NS",
    "Kaynes Tech": "KAYNES.NS",
    "NMDC": "NMDC.NS"
}


# =====================================================================
# 1. CENTRALIZED FIREBASE INITIALIZATION
# =====================================================================
def init_firebase() -> None:
    """Idempotently initializes Firebase Admin SDK[cite: 1, 4]."""
    if not firebase_admin._apps:
        try:
            logger.info("Initializing Firebase Admin SDK connection...")
            cred_val = FIREBASE_CREDENTIALS
            if isinstance(cred_val, str) and cred_val.strip().startswith("{"):
                cred_dict = json.loads(cred_val)
                cred = credentials.Certificate(cred_dict)
            elif os.path.exists(str(cred_val)):
                cred = credentials.Certificate(cred_val)
            elif os.path.exists("serviceAccountKey.json"):
                cred = credentials.Certificate("serviceAccountKey.json")
            else:
                raise FileNotFoundError(
                    "Firebase credentials not found. Check FIREBASE_CREDENTIALS in config/env."
                )

            firebase_admin.initialize_app(cred, {
                "databaseURL": FIREBASE_DATABASE_URL
            })
            logger.info("Firebase Admin successfully connected.")
        except Exception as e:
            logger.critical(f"Fatal error initializing Firebase Admin: {e}", exc_info=True)
            raise e


def sanitize_key(key: str) -> str:
    """Sanitizes script name string for Firebase path safety[cite: 1, 4]."""
    if not key:
        return ""
    return (
        str(key)
        .strip()
        .replace(".", "_")
        .replace("$", "_")
        .replace("#", "_")
        .replace("[", "_")
        .replace("]", "_")
        .replace("/", "_")
    )


# =====================================================================
# 2. CALENDAR CONFIG ACCESS (WITH IN-MEMORY CACHE)
# =====================================================================
def get_calendar_config(force_reload: bool = False) -> dict:
    """Reads custom calendar overrides with in-memory caching[cite: 1]."""
    global _CALENDAR_CACHE
    if _CALENDAR_CACHE is not None and not force_reload:
        return _CALENDAR_CACHE

    init_firebase()
    try:
        ref = db.reference("config/nse_calendar")
        data = ref.get()
        _CALENDAR_CACHE = data if isinstance(data, dict) else {}
        return _CALENDAR_CACHE
    except Exception as e:
        logger.error(f"Error fetching calendar config: {e}")
        return _CALENDAR_CACHE or {}


def set_calendar_config(payload: dict) -> bool:
    """Saves calendar data in Firebase and updates the local cache[cite: 1]."""
    global _CALENDAR_CACHE
    init_firebase()
    try:
        ref = db.reference("config/nse_calendar")
        ref.set(payload)
        _CALENDAR_CACHE = payload
        return True
    except Exception as e:
        logger.error(f"Error setting calendar config: {e}")
        return False


# =====================================================================
# 3. ROBUST WATCHLIST & TICKER DISCOVERY
# =====================================================================
def get_master_watchlist_mapping() -> dict[str, str]:
    """
    Reads active stocks from /watchlist, /param, and /detailedDb[cite: 2, 4],
    resolving authentic tickers for all valid equities and indices[cite: 2, 4].
    Always returns exact display names as keys (matching master.py expectations)[cite: 1].
    """
    init_firebase()
    master_mapping = {k: v for k, v in FIXED_INDICES.items()}

    try:
        watchlist_root = db.reference("watchlist").get() or {}
        param_db = db.reference("param").get() or {}
        detailed_db = db.reference("detailedDb").get() or {}
        display_list = db.reference("display_list").get() or {}

        combined_db = {}
        if isinstance(detailed_db, dict):
            combined_db.update(detailed_db)
        if isinstance(param_db, dict):
            combined_db.update(param_db)

        def extract_items(raw_val):
            extracted = set()
            if not raw_val:
                return extracted
            if isinstance(raw_val, str):
                s = raw_val.strip()
                if s.startswith("[") and s.endswith("]"):
                    try:
                        parsed = ast.literal_eval(s)
                        if isinstance(parsed, list):
                            for p in parsed:
                                extracted.update(extract_items(p))
                            return extracted
                    except Exception:
                        pass
                if s not in ("Group", "Mkt Cap Rank inc."):
                    extracted.add(s)
            elif isinstance(raw_val, list):
                for item in raw_val:
                    extracted.update(extract_items(item))
            elif isinstance(raw_val, dict):
                for k, v in raw_val.items():
                    if k not in ("watchlist", "detailedDb", "lastSync"):
                        extracted.update(extract_items(k))
                    extracted.update(extract_items(v))
            return extracted

        candidate_names = extract_items(watchlist_root)
        candidate_names.update(extract_items(display_list))

        if not candidate_names and isinstance(param_db, dict):
            for k in param_db.keys():
                candidate_names.update(extract_items(k))

        for name in candidate_names:
            if not name or name in ("Group", "Mkt Cap Rank inc."):
                continue

            clean_name = str(name).strip()
            dot_name = clean_name.replace(".", "_")

            ticker = KNOWN_TICKERS.get(clean_name) or KNOWN_TICKERS.get(dot_name)

            if not ticker:
                info = (
                    combined_db.get(clean_name) or 
                    combined_db.get(dot_name) or 
                    {}
                )
                ticker = info.get("TICKER") or info.get("ticker") or info.get("Ticker")
                if not ticker:
                    code = info.get("CODE") or info.get("code") or info.get("NSE") or info.get("nse")
                    if code:
                        ticker = f"{str(code).strip()}.NS"

            if ticker:
                master_mapping[clean_name] = str(ticker).strip()

        logger.info(f"[MASTER-WATCHLIST] Successfully resolved {len(master_mapping)} total targets.")
        return master_mapping

    except Exception as e:
        logger.error(f"[MASTER-WATCHLIST] Failed to read master watchlist: {e}", exc_info=True)
        return master_mapping


def get_current_stocklist() -> dict[str, str]:
    """Reads current node at /stocklist[cite: 2, 4]."""
    init_firebase()
    try:
        data = db.reference(PATH_SCRIPTS).get() or {}
        if isinstance(data, dict):
            return {str(k).strip(): str(v).strip() for k, v in data.items() if k and v}
        return {}
    except Exception as e:
        logger.error(f"[STOCKLIST] Error reading /stocklist: {e}")
        return {}


def reconcile_stocklist_with_watchlist() -> tuple[bool, dict[str, str]]:
    """
    Synchronizes /stocklist with resolved watchlist targets[cite: 2, 4].
    Returns (was_updated, active_map) where active_map contains all indices + equities[cite: 2, 4].
    """
    init_firebase()
    master_map = get_master_watchlist_mapping()
    current_stocklist = get_current_stocklist()

    # Create sanitized mapping for Firebase storage
    safe_master_map = {sanitize_key(k): v for k, v in master_map.items()}

    diff_detected = False
    if set(safe_master_map.keys()) != set(current_stocklist.keys()):
        diff_detected = True
    else:
        for stock, ticker in safe_master_map.items():
            if current_stocklist.get(stock) != ticker:
                diff_detected = True
                break

    if diff_detected or not current_stocklist:
        logger.info(f"[RECONCILE] Updating /stocklist ({len(safe_master_map)} targets) in Firebase...")
        try:
            db.reference(PATH_SCRIPTS).set(safe_master_map)
        except Exception as e:
            logger.error(f"[RECONCILE] Failed to write /stocklist: {e}", exc_info=True)

    # Return master_map directly so master.py can see equities like 'Bank of Maha'
    return diff_detected, master_map


def get_stocklist_mapping() -> dict[str, str]:
    """Returns active target dictionary[cite: 4]."""
    init_firebase()
    master_map = get_master_watchlist_mapping()
    return master_map


def get_stocklist() -> list:
    """Returns list of active target names[cite: 2, 4]."""
    return list(get_stocklist_mapping().keys())


def get_scripts_list() -> list[str]:
    return get_stocklist()


# =====================================================================
# 4. OHLC DATABASE ACCESS
# =====================================================================
def get_stock_ohlc(display_name: str) -> dict:
    """Reads complete OHLC records from /stocks/<display_name>[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching OHLC for {display_name}: {e}")
        return {}


def save_stock_ohlc(display_name: str, payload: dict) -> bool:
    """Saves complete OHLC dataset into /stocks/<display_name>[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error saving OHLC for {display_name}: {e}")
        return False


def write_full_ohlc(display_name: str, ohlc_dict: dict) -> bool:
    """Updates historical records under /stocks/<display_name>[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}")
        ref.update(ohlc_dict)
        return True
    except Exception as e:
        logger.error(f"Error updating full OHLC for {display_name}: {e}")
        return False


write_historical_ohlc = write_full_ohlc


def update_live_candle(display_name: str, live_candle: dict) -> bool:
    """Writes intraday candle strictly to index 0[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
        ref.set(live_candle)
        return True
    except Exception as e:
        logger.error(f"Error writing live candle for {display_name}: {e}")
        return False


write_live_ohlc = update_live_candle


def clear_live_candle(display_name: str) -> bool:
    """Purges live index 0 using .delete()[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing live candle (0) for {display_name}: {e}")
        return False