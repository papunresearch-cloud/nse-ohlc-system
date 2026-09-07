"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management.
- Dynamic stock discovery from '/watchlist' and '/detailedDb' (No static stocklist node).
- Read/write access for OHLC candle historical records under '/stocks/<Script Name>'.
- Index 0 live scratchpad sanitization via .delete().
- Calendar configuration handlers for market_calendar.py.
- Export aliases for sync_child.py and live_child.py.
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
# 1. CENTRALIZED FIREBASE INITIALIZATION
# =====================================================================
def init_firebase():
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        try:
            logger.info("Initializing Firebase Admin SDK connection...")
            
            cred_val = FIREBASE_CREDENTIALS
            if isinstance(cred_val, str) and cred_val.strip().startswith("{"):
                # Render cloud environment: raw JSON string
                cred_dict = json.loads(cred_val)
                cred = credentials.Certificate(cred_dict)
            elif os.path.exists(str(cred_val)):
                # Local environment: file path
                cred = credentials.Certificate(cred_val)
            elif os.path.exists("serviceAccountKey.json"):
                # Local default fallback
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
# 2. CALENDAR CONFIG ACCESS (REQUIRED BY MARKET_CALENDAR.PY)
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
# 3. DYNAMIC WATCHLIST DISCOVERY (STRATEGY 3)
# =====================================================================
def get_stocklist_mapping() -> dict[str, str]:
    """
    Dynamically maps active stock names to Yahoo Finance tickers directly
    from 'watchlist' and 'detailedDb' in Firebase memory.
    """
    init_firebase()
    try:
        watchlist_node = db.reference("watchlist").get() or {}
        detailed_db = db.reference("detailedDb").get() or {}

        # Safely extract active script names (handles flat list or dictionary)
        if isinstance(watchlist_node, dict) and "watchlist" in watchlist_node:
            raw_names = watchlist_node.get("watchlist", [])
        elif isinstance(watchlist_node, dict):
            raw_names = list(watchlist_node.values())
        elif isinstance(watchlist_node, list):
            raw_names = watchlist_node
        else:
            raw_names = []

        active_names = [str(n).strip() for n in raw_names if n]

        mapping = {}
        for name in active_names:
            stock_info = detailed_db.get(name) or {}
            ticker = stock_info.get("TICKER") or stock_info.get("ticker") or stock_info.get("Ticker")

            if ticker:
                t = str(ticker).strip()
                if t.upper() == "^NESI":
                    t = "^NSEI"
                mapping[name] = t
            else:
                logger.warning(f"[DISCOVERY] No TICKER found in detailedDb for active stock: '{name}'")

        logger.info(f"[DISCOVERY] Dynamically mapped {len(mapping)} active stocks from watchlist.")
        return mapping

    except Exception as e:
        logger.error(f"[DISCOVERY] Failed to build dynamic stock mapping: {e}", exc_info=True)
        return {}


def get_stocklist() -> list:
    """Returns active stock names list."""
    mapping = get_stocklist_mapping()
    return list(mapping.keys())


# =====================================================================
# 4. OHLC DATA ACCESS (CHILD-1 & CHILD-2 SUPPORT)
# =====================================================================
def get_stock_ohlc(display_name: str) -> dict:
    """Reads the complete OHLC dictionary for a stock under /stocks/<display_name>."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching OHLC data for {display_name}: {e}")
        return {}


def save_stock_ohlc(display_name: str, payload: dict) -> bool:
    """Saves historical OHLC records into /stocks/<display_name>."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error saving OHLC for {display_name}: {e}")
        return False


def write_full_ohlc(display_name: str, ohlc_dict: dict) -> bool:
    """Writes historical candles (indices 1 to 250) without wiping live index 0."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}")
        ref.update(ohlc_dict)
        return True
    except Exception as e:
        logger.error(f"Error updating full OHLC for {display_name}: {e}")
        return False


# Alias for sync_child
write_historical_ohlc = write_full_ohlc


def update_live_candle(display_name: str, live_candle: dict) -> bool:
    """Writes live intraday candle strictly to index 0."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}/0")
        ref.set(live_candle)
        return True
    except Exception as e:
        logger.error(f"Error writing live candle for {display_name}: {e}")
        return False


# Alias for backwards compatibility
write_live_ohlc = update_live_candle


def clear_live_candle(display_name: str) -> bool:
    """Safely purges index 0 using .delete() instead of .set(None)."""
    init_firebase()
    try:
        ref = db.reference(f"{PATH_STOCKS}/{display_name}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing live candle (0) for {display_name}: {e}")
        return False