"""
Handles all Firebase Admin SDK initialization, authentication, reads, and updates.
"""
import os
import json
import firebase_admin
from firebase_admin import credentials, db
from config import (
    FIREBASE_DATABASE_URL,
    FIREBASE_CREDENTIALS,
    PATH_SCRIPTS,
    PATH_STOCKS,
    PATH_CALENDAR,
    logger
)

_is_initialized = False


def init_firebase() -> None:
    """Idempotently initializes Firebase Admin SDK."""
    global _is_initialized
    if firebase_admin._apps or _is_initialized:
        return

    logger.info("Initializing Firebase Admin SDK connection...")
    cred_env = FIREBASE_CREDENTIALS

    try:
        if cred_env.strip().startswith("{"):
            cred_dict = json.loads(cred_env)
            cred = credentials.Certificate(cred_dict)
        elif os.path.exists(cred_env):
            cred = credentials.Certificate(cred_env)
        else:
            raise FileNotFoundError(
                "Firebase credentials not found via path or valid JSON string in FIREBASE_CREDENTIALS"
            )

        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DATABASE_URL
        })
        _is_initialized = True
        logger.info("Firebase Admin successfully connected.")
    except Exception as e:
        logger.critical(f"FATAL: Firebase initialization failed: {e}", exc_info=True)
        raise


def get_stocklist_mapping() -> dict[str, str]:
    """
    Dynamically maps active stock names to Yahoo Finance tickers directly
    from 'watchlist' and 'detailedDb' in memory without relying on a static
    'stocklist' node.
    """
    init_firebase()
    try:
        # 1. Fetch watchlist node
        watchlist_ref = db.reference("watchlist").get() or {}

        # Handle flat or nested Firebase structures
        if isinstance(watchlist_ref, dict) and "watchlist" in watchlist_ref:
            raw_names = watchlist_ref.get("watchlist", [])
            detailed_db = watchlist_ref.get("detailedDb", {})
        else:
            raw_names = watchlist_ref
            detailed_db = db.reference("detailedDb").get() or {}

        # 2. Extract active script names
        if isinstance(raw_names, dict):
            active_names = list(raw_names.values())
        elif isinstance(raw_names, list):
            active_names = raw_names
        else:
            active_names = []

        active_names = [str(n).strip() for n in active_names if n]

        # 3. Match each active script to its Yahoo Finance TICKER
        mapping = {}
        for name in active_names:
            stock_info = detailed_db.get(name) or {}
            ticker = stock_info.get("TICKER") or stock_info.get("ticker") or stock_info.get("Ticker")

            if ticker:
                t = str(ticker).strip()
                if t.upper() == "^NESI":  # Automatic typo correction
                    t = "^NSEI"
                mapping[name] = t
            else:
                logger.warning(f"[DISCOVERY] No valid TICKER found in detailedDb for active stock: '{name}'")

        logger.info(f"[DISCOVERY] Dynamically mapped {len(mapping)} active stocks from watchlist.")
        return mapping

    except Exception as e:
        logger.error(f"[DISCOVERY] Failed to build dynamic stock mapping: {e}", exc_info=True)
        return {}

def get_stock_ohlc(display_name: str) -> dict | list | None:
    """Fetches existing OHLC node for a specific display name."""
    init_firebase()
    safe_key = sanitize_key(display_name)
    ref = db.reference(f"{PATH_STOCKS}/{safe_key}")
    return ref.get()


def get_calendar_config() -> dict:
    """Fetches NSE calendar rules and overrides."""
    init_firebase()
    ref = db.reference(PATH_CALENDAR)
    data = ref.get()
    return data or {}


def set_calendar_config(payload: dict) -> None:
    """Seeds the NSE calendar config."""
    init_firebase()
    ref = db.reference(PATH_CALENDAR)
    ref.set(payload)


def write_full_ohlc(display_name: str, records: dict[str, dict]) -> bool:
    """Writes the complete 250 records under /stocks/<Display Name>."""
    init_firebase()
    safe_key = sanitize_key(display_name)
    ref = db.reference(f"{PATH_STOCKS}/{safe_key}")
    try:
        ref.set(records)
        return True
    except Exception as e:
        logger.error(f"Firebase full write failed for {display_name}: {e}")
        return False


def update_live_candle(display_name: str, candle: dict) -> bool:
    """Updates only node /stocks/<Display Name>/0."""
    init_firebase()
    safe_key = sanitize_key(display_name)
    ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
    try:
        ref.set(candle)
        return True
    except Exception as e:
        logger.error(f"Firebase live candle write failed for {display_name}: {e}")
        return False

def clear_live_candle(display_name: str) -> bool:
    """Safely removes index 0 from Firebase using .delete() instead of .set(None)."""
    try:
        safe_key = sanitize_key(display_name)
        ref = db.reference(f"{PATH_STOCKS}/{safe_key}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"[{display_name}] Failed to clear index 0: {e}")
        return False

def sanitize_key(key: str) -> str:
    """Sanitizes Firebase keys replacing invalid characters except spaces."""
    return key.replace(".", "_").replace("$", "").replace("#", "").replace("[", "").replace("]", "").replace("/", "_")