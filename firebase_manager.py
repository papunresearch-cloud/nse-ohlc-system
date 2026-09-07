"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management.
- Dynamic stock discovery from '/watchlist' and '/detailedDb' (No static stocklist node).
- Read/write access for OHLC candle historical records under '/stocks/<Script Name>'.
- Index 0 live scratchpad sanitization.
"""
import os
import firebase_admin
from firebase_admin import credentials, db
from config import logger, FIREBASE_CRED_PATH, FIREBASE_DB_URL

# =====================================================================
# 1. CENTRALIZED FIREBASE INITIALIZATION
# =====================================================================
def init_firebase():
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        try:
            logger.info("Initializing Firebase Admin SDK connection...")
            if not os.path.exists(FIREBASE_CRED_PATH):
                raise FileNotFoundError(
                    f"Firebase credentials file not found at: {FIREBASE_CRED_PATH}"
                )

            cred = credentials.Certificate(FIREBASE_CRED_PATH)
            firebase_admin.initialize_app(cred, {
                "databaseURL": FIREBASE_DB_URL
            })
            logger.info("Firebase Admin successfully connected.")
        except Exception as e:
            logger.critical(f"Fatal error initializing Firebase Admin: {e}", exc_info=True)
            raise e


# =====================================================================
# 2. DYNAMIC WATCHLIST DISCOVERY (STRATEGY 3)
# =====================================================================
def get_stocklist_mapping() -> dict[str, str]:
    """
    Dynamically maps active stock names to Yahoo Finance tickers directly
    from 'watchlist' and 'detailedDb' in Firebase memory.
    Eliminates reliance on an intermediate static 'stocklist' node.
    """
    init_firebase()
    try:
        # Fetch root watchlist
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

        # Match each active script to its Yahoo Finance TICKER
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
                logger.warning(f"[DISCOVERY] No TICKER found in detailedDb for active stock: '{name}'")

        logger.info(f"[DISCOVERY] Dynamically mapped {len(mapping)} active stocks from watchlist.")
        return mapping

    except Exception as e:
        logger.error(f"[DISCOVERY] Failed to build dynamic stock mapping: {e}", exc_info=True)
        return {}


# =====================================================================
# 3. OHLC DATA ACCESS
# =====================================================================
def get_stock_ohlc(display_name: str) -> dict:
    """Reads the complete OHLC dictionary for a stock under /stocks/<display_name>."""
    init_firebase()
    try:
        ref = db.reference(f"stocks/{display_name}")
        data = ref.get()
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error(f"Error fetching OHLC data for {display_name}: {e}")
        return {}


def save_stock_ohlc(display_name: str, payload: dict) -> bool:
    """Saves historical OHLC records into /stocks/<display_name>."""
    init_firebase()
    try:
        ref = db.reference(f"stocks/{display_name}")
        ref.set(payload)
        return True
    except Exception as e:
        logger.error(f"Error saving OHLC for {display_name}: {e}")
        return False


def clear_live_candle(display_name: str) -> bool:
    """Clears Index 0 for a stock to sanitize live pre/post market states."""
    init_firebase()
    try:
        ref = db.reference(f"stocks/{display_name}/0")
        ref.delete()
        return True
    except Exception as e:
        logger.error(f"Error clearing live candle (0) for {display_name}: {e}")
        return False