"""
Handles all Firebase Admin SDK initialization, authentication, reads, and updates.
Credentials can be passed via environment variable (raw JSON) or local file.
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
        # Check if environment variable contains raw JSON string
        if cred_env.strip().startswith("{"):
            cred_dict = json.loads(cred_env)
            cred = credentials.Certificate(cred_dict)
        elif os.path.exists(cred_env):
            cred = credentials.Certificate(cred_env)
        else:
            raise FileNotFoundError(
                f"Firebase credentials not found via path or valid JSON string in FIREBASE_CREDENTIALS"
            )

        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DATABASE_URL
        })
        _is_initialized = True
        logger.info("Firebase Admin successfully connected.")
    except Exception as e:
        logger.critical(f"FATAL: Firebase initialization failed: {e}", exc_info=True)
        raise


def get_scripts_list() -> list[str]:
    """Reads script identifiers from /scripts/."""
    init_firebase()
    ref = db.reference(PATH_SCRIPTS)
    data = ref.get()
    if not data:
        return []
    if isinstance(data, dict):
        return [str(k).strip() for k in data.keys() if str(k).strip()]
    if isinstance(data, list):
        return [str(item).strip() for item in data if item]
    return []


def get_stock_ohlc(script: str) -> dict | list | None:
    """Fetches existing OHLC node for a specific script."""
    init_firebase()
    safe_script = sanitize_key(script)
    ref = db.reference(f"{PATH_STOCKS}/{safe_script}")
    return ref.get()


def get_calendar_config() -> dict:
    """Fetches NSE calendar rules and overrides from /config/nse_calendar."""
    init_firebase()
    ref = db.reference(PATH_CALENDAR)
    data = ref.get()
    return data or {}


def set_calendar_config(payload: dict) -> None:
    """Writes or seeds the NSE calendar config."""
    init_firebase()
    ref = db.reference(PATH_CALENDAR)
    ref.set(payload)


def write_full_ohlc(script: str, records: dict[str, dict]) -> bool:
    """
    Overwrites the full 250 OHLC records for a script.
    Guarantees index 0-249 schema.
    """
    init_firebase()
    safe_script = sanitize_key(script)
    ref = db.reference(f"{PATH_STOCKS}/{safe_script}")
    try:
        ref.set(records)
        return True
    except Exception as e:
        logger.error(f"Firebase full write failed for {script}: {e}")
        return False


def update_live_candle(script: str, candle: dict) -> bool:
    """Updates only node /stocks/<script>/0 during live trading."""
    init_firebase()
    safe_script = sanitize_key(script)
    ref = db.reference(f"{PATH_STOCKS}/{safe_script}/0")
    try:
        ref.set(candle)
        return True
    except Exception as e:
        logger.error(f"Firebase live candle write failed for {script}: {e}")
        return False


def sanitize_key(key: str) -> str:
    """Sanitizes Firebase keys replacing invalid characters."""
    return key.replace(".", "_").replace("$", "").replace("#", "").replace("[", "").replace("]", "").replace("/", "_")