"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection.
- Dynamic stock discovery pulling names from /watchlist/watchlist
  and tickers from /param/<stock_name>/TICKER.
- OHLC candle access for /stocks/<Script Name>.
- Calendar access for market_calendar.py.
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
    TARGET_OHLC_COUNT
)

# =====================================================================
# 1. CENTRALIZED INITIALIZATION
# =====================================================================
def init_firebase():
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        try:
            logger.info("Initializing Firebase Admin SDK connection...")
            cred_val = FIREBASE_CREDENTIALS
            
            if isinstance(cred_val, str) and cred_val.strip().startswith("{"):
                # Render cloud environment: JSON string
                cred_dict = json.loads(cred_val)
                cred = credentials.Certificate(cred_dict)
            elif os.path.exists(str(cred_val)):
                # Local environment: file path
                cred = credentials.Certificate(cred_val)
            elif os.path.exists("serviceAccountKey.json"):
                # Default local fallback
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
    return str(key).strip().replace(".", "_").replace("$", "_").replace("#", "_").replace("[", "_").replace("]", "_").replace("/", "_")


# =====================================================================
# 2. CALENDAR ACCESS (market_calendar.py)
# =====================================================================
def get_calendar_config() -> dict:
    init_firebase()
    try:
        ref = db.reference("config/nse_calendar")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching calendar config: {e}")
        return {}


def set_calendar_config(payload: dict) -> bool:
    init_firebase()
    try:
        ref = db.reference("config/nse_calendar")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error setting calendar config: {e}")
        return False


# =====================================================================
# 3. DYNAMIC WATCHLIST DISCOVERY
# =====================================================================
def get_stocklist_mapping() -> dict[str, str]:
    """
    Dynamically discovers active stock names and resolves their Yahoo tickers.
    - Script Names: Read from /watchlist/watchlist (array or dict)
    - Tickers: Read from /param/<Script Name>/TICKER
    """
    init_firebase()
    try:
        # 1. Fetch active script names
        watchlist_root = db.reference("watchlist").get() or {}
        
        raw_names = []
        if isinstance(watchlist_root, dict):
            # Matches your exact structure: /watchlist/watchlist array
            if "watchlist" in watchlist_root:
                raw_names = watchlist_root.get("watchlist", [])
            else:
                raw_names = list(watchlist_root.values())
        elif isinstance(watchlist_root, list):
            raw_names = watchlist_root

        # Normalize to clean string names
        active_names = []
        if isinstance(raw_names, dict):
            active_names = [str(v).strip() for v in raw_names.values() if v]
        elif isinstance(raw_names, list):
            active_names = [str(v).strip() for v in raw_names if v]

        if not active_names:
            logger.warning("[DISCOVERY] Watchlist is empty in Firebase.")
            return {}

        # 2. Fetch param node where ticker definitions live
        param_db = db.reference("param").get() or {}

        # 3. Build dynamic mapping: { Name: TICKER }
        mapping = {}
        for name in active_names:
            stock_info = param_db.get(name) or {}
            ticker = stock_info.get("TICKER") or stock_info.get("ticker") or stock_info.get("Ticker")

            if ticker:
                t = str(ticker).strip()
                if t.upper() == "^NESI":
                    t = "^NSEI"
                mapping[name] = t
            else:
                logger.warning(f"[DISCOVERY] Stock '{name}' listed in watchlist but TICKER missing under /param/{name}")

        logger.info(f"[DISCOVERY] Successfully mapped {len(mapping)} active stocks from watchlist and param.")
        return mapping

    except Exception as e:
        logger.error(f"[DISCOVERY] Error building dynamic stock mapping: {e}", exc_info=True)
        return {}


def get_stocklist() -> list:
    return list(get_stocklist_mapping().keys())


# =====================================================================
# 4. OHLC DATABASE ACCESS (CHILD-1 & CHILD-2)
# =====================================================================
def get_stock_ohlc(display_name: str) -> dict:
    """Reads complete OHLC records from /stocks/<display_name>."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching OHLC for {display_name}: {e}")
        return {}


def save_stock_ohlc(display_name: str, payload: dict) -> bool:
    """Sets entire OHLC node."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error saving OHLC for {display_name}: {e}")
        return False


def write_full_ohlc(display_name: str, ohlc_dict: dict) -> bool:
    """Updates historical records (indices 1–250) without overwriting live index 0."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}")
        ref.update(ohlc_dict)
        return True
    except Exception as e:
        logger.error(f"Error updating historical OHLC for {display_name}: {e}")
        return False


write_historical_ohlc = write_full_ohlc


def update_live_candle(display_name: str, live_candle: dict) -> bool:
    """Writes live intraday candle strictly to index 0."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}/0")
        ref.set(live_candle)
        return True
    except Exception as e:
        logger.error(f"Error updating live candle for {display_name}: {e}")
        return False


write_live_ohlc = update_live_candle


def clear_live_candle(display_name: str) -> bool:
    """Safely purges live index 0 using .delete()."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing index 0 for {display_name}: {e}")
        return False