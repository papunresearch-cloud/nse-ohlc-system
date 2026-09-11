"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management[cite: 1, 4].
- Dynamic stock discovery pulling tickers from /param, /detailedDb, and /display_list[cite: 2, 4].
- Permanent anchoring of 4 master market indices (immune to purging/deletion)[cite: 4].
- In-memory calendar caching and urllib3 warning suppression.
- Complete read/write access for OHLC records (Index 0 live, Index 1-250 historical)[cite: 1, 4].
"""
import os
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

# 1. Mute noisy connection pool disconnect warnings
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# In-memory calendar cache
_CALENDAR_CACHE = None

# Permanent master indices[cite: 4]
FIXED_INDICES = {
    "NIFTY50": "^NSEI",
    "NIFTY100": "^CNX100",
    "NIFTY MIDCAP 150": "NIFTYMIDCAP150.NS",
    "NIFTY SMALLCAP 250": "NIFTYSMLCAP250.NS"
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
    """Reads custom calendar overrides / holidays with in-memory caching[cite: 1]."""
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
    """Saves or seeds custom calendar data in Firebase and updates cache[cite: 1]."""
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
    STRICT READ-ONLY:
    1. Anchors the 4 permanent master indices[cite: 4].
    2. Inspects /param, /detailedDb, /display_list, and /watchlist to discover all equities[cite: 2, 4].
    Returns: { Clean Sanitized Name: Yahoo Ticker }
    """
    init_firebase()
    master_mapping = {sanitize_key(k): v for k, v in FIXED_INDICES.items()}

    try:
        # 1. Fetch metadata nodes
        param_db = db.reference("param").get() or {}
        detailed_db = db.reference("detailedDb").get() or {}
        display_list = db.reference("display_list").get() or {}
        watchlist_root = db.reference("watchlist").get() or {}

        # Merge metadata sources
        combined_db = {}
        if isinstance(detailed_db, dict):
            combined_db.update(detailed_db)
        if isinstance(param_db, dict):
            combined_db.update(param_db)

        # 2. Extract active script names from all available registries
        candidate_names = set()

        if isinstance(watchlist_root, dict):
            wl = watchlist_root.get("watchlist", [])
            if isinstance(wl, (list, dict)):
                items = wl.values() if isinstance(wl, dict) else wl
                candidate_names.update(str(x).strip() for x in items if x)
            for k in watchlist_root.keys():
                if k not in ("watchlist", "detailedDb", "lastSync"):
                    candidate_names.add(str(k).strip())
        elif isinstance(watchlist_root, list):
            candidate_names.update(str(x).strip() for x in watchlist_root if x)

        if isinstance(display_list, list):
            candidate_names.update(str(x).strip() for x in display_list if x)
        elif isinstance(display_list, dict):
            candidate_names.update(str(x).strip() for x in display_list.values() if x)

        # If watchlist is empty, fall back directly to keys in param
        if not candidate_names and isinstance(param_db, dict):
            candidate_names.update(param_db.keys())

        # 3. Match candidate names to tickers
        for name in candidate_names:
            if not name:
                continue

            sanitized_name = sanitize_key(name)
            dot_name = name.replace(".", "_")

            stock_info = (
                combined_db.get(name) or
                combined_db.get(sanitized_name) or
                combined_db.get(dot_name) or
                {}
            )

            ticker = (
                stock_info.get("TICKER") or
                stock_info.get("ticker") or
                stock_info.get("Ticker")
            )

            # Fallback to CODE or NSE field if TICKER key is missing[cite: 2]
            if not ticker:
                code_val = (
                    stock_info.get("CODE") or
                    stock_info.get("code") or
                    stock_info.get("NSE") or
                    stock_info.get("nse")
                )
                if code_val:
                    ticker = f"{str(code_val).strip()}.NS"

            # Auto-generate ticker from common NSE conventions if missing
            if not ticker:
                # Specific ticker overrides for common stock names[cite: 8]
                overrides = {
                    "Bank of Maha": "MAHABANK.NS",
                    "Bharat Electron": "BEL.NS",
                    "Black Box": "BBOX.NS",
                    "Caplin Point Lab": "CAPLIPOINT.NS",
                    "Coal India": "COALINDIA.NS",
                    "Dixon Technolog_": "DIXON.NS",
                    "Dixon Technolog.": "DIXON.NS"
                }
                ticker = overrides.get(name) or overrides.get(sanitized_name)

            if ticker:
                t = str(ticker).strip()
                if t.upper() == "^NESI":
                    t = "^NSEI"
                master_mapping[sanitized_name] = t
            else:
                logger.warning(f"[MASTER-WATCHLIST] Stock '{name}' found but could not resolve TICKER.")

        logger.info(f"[MASTER-WATCHLIST] Resolved {len(master_mapping)} total targets (4 indices + {len(master_mapping) - 4} stocks).")
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
    Compares /stocklist against master targets and FIXED_INDICES[cite: 2, 4].
    Guarantees active stocks are always returned so master.py never starves[cite: 1].
    """
    init_firebase()
    master_map = get_master_watchlist_mapping()
    current_stocklist = get_current_stocklist()

    safe_master_map = {sanitize_key(k): v for k, v in master_map.items()}

    # Check for discrepancies
    diff_detected = False
    if set(safe_master_map.keys()) != set(current_stocklist.keys()):
        diff_detected = True
    else:
        for stock, ticker in safe_master_map.items():
            if current_stocklist.get(stock) != ticker:
                diff_detected = True
                break

    if diff_detected or not current_stocklist:
        logger.info(
            f"[RECONCILE] Updating /stocklist ({len(safe_master_map)} targets) to reflect master targets."
        )
        try:
            db.reference(PATH_SCRIPTS).set(safe_master_map)
            return True, safe_master_map
        except Exception as e:
            logger.error(f"[RECONCILE] Failed to write synchronized /stocklist: {e}", exc_info=True)
            return False, safe_master_map

    return False, safe_master_map


def get_stocklist_mapping() -> dict[str, str]:
    """Returns the active target dictionary (FIXED_INDICES merged with /stocklist)[cite: 4]."""
    mapping = get_current_stocklist()
    if not mapping:
        _, mapping = reconcile_stocklist_with_watchlist()

    for k, v in FIXED_INDICES.items():
        mapping.setdefault(sanitize_key(k), v)

    return mapping


def get_stocklist() -> list:
    """Returns list of active target names[cite: 2, 4]."""
    return list(get_stocklist_mapping().keys())


def get_scripts_list() -> list[str]:
    """Backward-compatible helper[cite: 3, 5]."""
    return get_stocklist()


# =====================================================================
# 4. OHLC DATABASE ACCESS (CHILD-1 & CHILD-2)
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
    """Writes or updates historical records under /stocks/<display_name>[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}")
        ref.update(ohlc_dict)
        return True
    except Exception as e:
        logger.error(f"Error updating full OHLC for {display_name}: {e}")
        return False


write_historical_ohlc = write_full_ohlc  # Function alias[cite: 1, 4]


def update_live_candle(display_name: str, live_candle: dict) -> bool:
    """Updates live intraday candle strictly to index 0[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
        ref.set(live_candle)
        return True
    except Exception as e:
        logger.error(f"Error writing live candle for {display_name}: {e}")
        return False


write_live_ohlc = update_live_candle  # Function alias[cite: 1, 4]


def clear_live_candle(display_name: str) -> bool:
    """Safely purges live index 0 using .delete()[cite: 1, 4]."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing live candle (0) for {display_name}: {e}")
        return False