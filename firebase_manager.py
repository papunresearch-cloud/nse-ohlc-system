"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management[cite: 1].
- Reads stock mappings from /stocklist and master nodes (/watchlist, /detailedDb)[cite: 1].
- In-memory caching and urllib3 error suppression to prevent connection drop warnings.
- Complete read/write access for OHLC records (Index 0 live, Index 1-250 historical)[cite: 1].
- Calendar configuration handlers for market_calendar.py[cite: 1].
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

# Suppress urllib3 connection pool dropped socket warnings
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# In-memory calendar cache to avoid polling Firebase on every tick
_CALENDAR_CACHE = None


# =====================================================================
# 1. CENTRALIZED FIREBASE INITIALIZATION
# =====================================================================
def init_firebase() -> None:
    """Idempotently initializes Firebase Admin SDK[cite: 1]."""
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
# 3. STOCKLIST & WATCHLIST READS
# =====================================================================
def get_current_stocklist() -> dict[str, str]:
    """Reads current node at /stocklist[cite: 1]."""
    init_firebase()
    try:
        data = db.reference(PATH_SCRIPTS).get() or {}
        if isinstance(data, dict):
            return {str(k).strip(): str(v).strip() for k, v in data.items() if k and v}
        return {}
    except Exception as e:
        logger.error(f"[STOCKLIST] Error reading /stocklist: {e}")
        return {}


def get_stocklist_mapping() -> dict[str, str]:
    """Returns the name-to-ticker mapping from /stocklist[cite: 1, 4]."""
    return get_current_stocklist()


def get_stocklist() -> list:
    """Returns list of active script display names[cite: 1]."""
    return list(get_stocklist_mapping().keys())


def get_scripts_list() -> list[str]:
    """Backward-compatible helper returning list of keys from stocklist[cite: 4]."""
    return get_stocklist()


# =====================================================================
# 4. OHLC DATABASE ACCESS (CHILD-1 & CHILD-2)
# =====================================================================
def get_stock_ohlc(display_name: str) -> dict:
    """Reads complete OHLC records from /stocks/<display_name>[cite: 1]."""
    init_firebase()
    safe_name = sanitize_key(display_name)
    try:
        ref = db.reference(f"{PATH_STOCKS}/{safe_name}")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching OHLC for {display_name}: {e}")
        return {}


def save_stock_ohlc(display_name: str, payload: dict) -> bool:
    """Saves complete OHLC dataset into /stocks/<display_name>[cite: 1]."""
    init_firebase()
    safe_name = sanitize_key(display_name)
    try:
        ref = db.reference(f"{PATH_STOCKS}/{safe_name}")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error saving OHLC for {display_name}: {e}")
        return False


def write_full_ohlc(display_name: str, ohlc_dict: dict) -> bool:
    """Writes or updates historical records under /stocks/<display_name>[cite: 1, 4]."""
    init_firebase()
    safe_name = sanitize_key(display_name)
    try:
        ref = db.reference(f"{PATH_STOCKS}/{safe_name}")
        ref.update(ohlc_dict)
        return True
    except Exception as e:
        logger.error(f"Error updating full OHLC for {display_name}: {e}")
        return False


write_historical_ohlc = write_full_ohlc  # Function alias[cite: 1]


def update_live_candle(display_name: str, live_candle: dict) -> bool:
    """Updates live intraday candle strictly to index 0[cite: 1, 4]."""
    init_firebase()
    safe_name = sanitize_key(display_name)
    try:
        ref = db.reference(f"{PATH_STOCKS}/{safe_name}/0")
        ref.set(live_candle)
        return True
    except Exception as e:
        logger.error(f"Error writing live candle for {display_name}: {e}")
        return False


write_live_ohlc = update_live_candle  # Function alias[cite: 1]


def clear_live_candle(display_name: str) -> bool:
    """Safely purges live index 0 using .delete()[cite: 1]."""
    init_firebase()
    safe_name = sanitize_key(display_name)
    try:
        ref = db.reference(f"{PATH_STOCKS}/{safe_name}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing live candle (0) for {display_name}: {e}")
        return False