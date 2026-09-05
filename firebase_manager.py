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
    Reads from /stocklist/ in Firebase and returns a mapping:
    {"Reliance Industries": "RELIANCE.NS", "NIFTY50": "^NSEI", ...}
    """
    init_firebase()
    ref = db.reference(PATH_SCRIPTS)
    data = ref.get()
    if not data:
        return {}
    
    mapping = {}
    if isinstance(data, dict):
        for name, ticker in data.items():
            if name and ticker:
                # Sanitize ticker string and fix common typos
                t = str(ticker).strip()
                if t.upper() == "^NESI":  # Correct common Nifty typo automatically
                    t = "^NSEI"
                mapping[str(name).strip()] = t
    return mapping


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
    """Sets index 0 to None/null to prevent stale quote consumption."""
    try:
        ref = db.reference(f"{PATH_STOCKS}/{sanitize_key(display_name)}/0")
        ref.set(None)
        return True
    except Exception as e:
        logger.error(f"[{display_name}] Failed to clear index 0: {e}")
        return False

def sanitize_key(key: str) -> str:
    """Sanitizes Firebase keys replacing invalid characters except spaces."""
    return key.replace(".", "_").replace("$", "").replace("#", "").replace("[", "").replace("]", "").replace("/", "_")