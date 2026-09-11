"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management.
- Dynamic stock discovery and reconciliation for /stocklist.
- Permanent anchoring of 4 master market indices (immune to purging/deletion).
- Pure read-only access to master nodes: /watchlist and /detailedDb.
- Guarded OHLC access for Child-1 (1 to 250) and Child-2 (index 0).
"""
import os
import json
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

# =====================================================================
# PERMANENT MASTER INDICES (Immune to deletion)
# =====================================================================
FIXED_INDICES = {
    "NIFTY50": "^NSEI",
    "NIFTY100": "^CNX100",
    "NIFTY MIDCAP 150": "NIFTYMIDCAP150.NS",
    "NIFTY SMALLCAP 250": "NIFTYSMLCAP250.NS"
}


# =====================================================================
# 1. CENTRALIZED FIREBASE INITIALIZATION
# =====================================================================
def init_firebase():
    """Idempotently initializes Firebase Admin SDK."""
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
    """Sanitizes script name string for Firebase path safety."""
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
# 2. CALENDAR CONFIG ACCESS (FOR MARKET_CALENDAR.PY)
# =====================================================================
def get_calendar_config() -> dict:
    """Reads custom calendar overrides / holidays from Firebase."""
    init_firebase()
    try:
        ref = db.reference("config/nse_calendar")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching calendar config: {e}")
        return {}


def set_calendar_config(payload: dict) -> bool:
    """Saves or seeds custom calendar data in Firebase."""
    init_firebase()
    try:
        ref = db.reference("config/nse_calendar")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error setting calendar config: {e}")
        return False


# =====================================================================
# 3. MASTER WATCHLIST READ-ONLY QUERIES & STOCKLIST SYNCHRONIZER
# =====================================================================
def get_master_watchlist_mapping() -> dict[str, str]:
    """
    STRICT READ-ONLY:
    1. Anchors the 4 permanent master indices.
    2. Reads active stocks from /watchlist and /detailedDb.
    Returns: { Clean Sanitized Name: Yahoo Ticker }
    """
    init_firebase()
    
    # 1. Start with the permanent, immutable indices
    master_mapping = {sanitize_key(k): v for k, v in FIXED_INDICES.items()}

    try:
        watchlist_root = db.reference("watchlist").get() or {}

        raw_names = []
        detailed_db = {}
        if isinstance(watchlist_root, dict):
            raw_names = watchlist_root.get("watchlist", [])
            detailed_db = watchlist_root.get("detailedDb", {})
        elif isinstance(watchlist_root, list):
            raw_names = watchlist_root

        if not detailed_db:
            detailed_db = db.reference("detailedDb").get() or {}

        active_names = []
        if isinstance(raw_names, dict):
            active_names = [str(v).strip() for v in raw_names.values() if v]
        elif isinstance(raw_names, list):
            active_names = [str(v).strip() for v in raw_names if v]

        param_db = db.reference("param").get() or {}

        # 2. Extract tickers and sanitize dictionary keys for Firebase safety
        for name in active_names:
            sanitized_name = sanitize_key(name)
            stock_info = (
                detailed_db.get(name) or 
                detailed_db.get(sanitized_name) or 
                detailed_db.get(name.replace(".", "_")) or 
                param_db.get(name) or 
                param_db.get(sanitized_name) or 
                {}
            )

            ticker = stock_info.get("TICKER") or stock_info.get("ticker") or stock_info.get("Ticker")

            # Fallback to CODE or NSE if TICKER key is missing
            if not ticker:
                code_val = (
                    stock_info.get("CODE") or 
                    stock_info.get("NSE") or 
                    stock_info.get("code") or 
                    stock_info.get("nse")
                )
                if code_val:
                    ticker = f"{str(code_val).strip()}.NS"

            if ticker:
                t = str(ticker).strip()
                if t.upper() == "^NESI":
                    t = "^NSEI"
                master_mapping[sanitized_name] = t
            else:
                logger.warning(f"[MASTER-WATCHLIST] Stock '{name}' has no TICKER or CODE in detailedDb/param.")

        return master_mapping
    except Exception as e:
        logger.error(f"[MASTER-WATCHLIST] Failed to read master watchlist: {e}", exc_info=True)
        return master_mapping


def get_current_stocklist() -> dict[str, str]:
    """Reads current node at /stocklist."""
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
    Compares /stocklist against the master /watchlist and FIXED_INDICES.
    Synchronizes /stocklist if discrepancies are detected.
    Returns: (was_changed: bool, active_stock_mapping: dict)
    """
    init_firebase()
    master_map = get_master_watchlist_mapping()
    current_stocklist = get_current_stocklist()

    # Safety Guard: Never clear stocklist if master watchlist returns empty
    if not master_map:
        logger.warning("[RECONCILE] Target map returned empty. Skipping sync to prevent accidental data loss.")
        return False, current_stocklist

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

    if diff_detected:
        logger.info(
            f"[RECONCILE] Discrepancy detected between target map ({len(safe_master_map)}) "
            f"and stocklist ({len(current_stocklist)})."
        )
        try:
            db.reference(PATH_SCRIPTS).set(safe_master_map)
            logger.info(f"[RECONCILE] /stocklist successfully synchronized with {len(safe_master_map)} targets.")
            return True, safe_master_map
        except Exception as e:
            logger.error(f"[RECONCILE] Failed to write synchronized /stocklist: {e}", exc_info=True)
            return False, current_stocklist

    return False, current_stocklist


def get_stocklist_mapping() -> dict[str, str]:
    """
    Returns the active target dictionary:
    Always includes the 4 permanent indices merged with /stocklist.
    """
    mapping = get_current_stocklist()
    if not mapping:
        _, mapping = reconcile_stocklist_with_watchlist()
    
    # Ensure permanent indices are present even if /stocklist was incomplete
    for k, v in FIXED_INDICES.items():
        mapping.setdefault(sanitize_key(k), v)
        
    return mapping


def get_stocklist() -> list:
    """Returns active target names list."""
    return list(get_stocklist_mapping().keys())


# =====================================================================
# 4. OHLC DATABASE ACCESS (CHILD-1 & CHILD-2)
# =====================================================================
def get_stock_ohlc(display_name: str) -> dict:
    """Reads complete OHLC records from /stocks/<display_name>."""
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
    """Saves entire OHLC payload to /stocks/<display_name>."""
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
    """Writes or updates historical records under /stocks/<display_name>."""
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
    """Updates live intraday candle strictly to index 0."""
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
    """Safely purges live index 0 using .delete()."""
    init_firebase()
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing live candle (0) for {display_name}: {e}")
        return False