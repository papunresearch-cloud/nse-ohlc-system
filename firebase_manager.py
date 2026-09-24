"""
FIREBASE MANAGER
- Centralized Firebase Admin SDK connection and lifecycle management.
- Dynamic stock discovery and reconciliation for /stocklist.
- Permanent anchoring of 4 master market indices (immune to purging/deletion).
- Safe targeted mutations for /watchlist/detailedDb and /stocklist.
- Guarded OHLC access for Child-1 (1 to 300) and Child-2 (index 0).
- Garbage Collector: purges orphaned OHLC records from /stocks, /param, and /alerts on deletion.
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
    "NIFTYMID150": "NIFTYMIDCAP150.NS",
    "NIFTYSM250": "NIFTYSMLCAP250.NS"
}


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
# MASTER WATCHLIST READ-ONLY QUERIES & STOCKLIST SYNCHRONIZER
# =====================================================================
def get_master_watchlist_mapping() -> dict[str, str]:
    """
    1. Anchors the 4 permanent master indices.
    2. Reads active stocks from /watchlist and maps to TICKER.
    Returns: { Clean Sanitized Name: Yahoo Ticker }
    """
    init_firebase()
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

        current_stocklist = get_current_stocklist()

        for name in active_names:
            sanitized_name = sanitize_key(name)
            stock_info = (
                detailed_db.get(name) or 
                detailed_db.get(sanitized_name) or 
                detailed_db.get(name.replace(".", "_")) or 
                {}
            )

            # Preference: detailedDb TICKER -> existing /stocklist mapping -> fallback to .NS
            ticker = stock_info.get("TICKER") or current_stocklist.get(sanitized_name)
            if not ticker:
                ticker = f"{sanitized_name}.NS"

            master_mapping[sanitized_name] = str(ticker).strip()

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
    Synchronizes /stocklist with active stocks in /watchlist.
    Purges orphaned OHLC records from /stocks, /param, and /alerts without touching other symbols.
    """
    init_firebase()
    master_map = get_master_watchlist_mapping()
    current_stocklist = get_current_stocklist()

    if not master_map:
        logger.warning("[RECONCILE] Target map returned empty. Skipping sync to prevent accidental data loss.")
        return False, current_stocklist

    safe_master_map = {sanitize_key(k): v for k, v in master_map.items()}

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
            # 1. Update /stocklist with exact active target set
            db.reference(PATH_SCRIPTS).set(safe_master_map)
            
            # 2. Garbage Collector: Purge orphaned records
            orphans = set(current_stocklist.keys()) - set(safe_master_map.keys())
            fixed_keys = {sanitize_key(k) for k in FIXED_INDICES.keys()}

            for orphan in orphans:
                if orphan in fixed_keys:
                    continue  # Guard fixed indices from deletion

                logger.info(f"[GARBAGE COLLECTOR] Purging records for deleted stock: {orphan}")
                try:
                    db.reference(f"{PATH_STOCKS}/{orphan}").delete()
                except Exception as del_err:
                    logger.warning(f"[GARBAGE COLLECTOR] Failed to purge {PATH_STOCKS}/{orphan}: {del_err}")

                try:
                    db.reference(f"param/{orphan}").delete()
                except Exception as del_param_err:
                    logger.warning(f"[GARBAGE COLLECTOR] Failed to purge param/{orphan}: {del_param_err}")

                # Purge /alerts and /alerts/stock_controls
                try:
                    db.reference(f"alerts/{orphan}").delete()
                    db.reference(f"alerts/stock_controls/{orphan}").delete()
                except Exception as del_alert_err:
                    logger.warning(f"[GARBAGE COLLECTOR] Failed to purge alerts for {orphan}: {del_alert_err}")

            logger.info(f"[RECONCILE] /stocklist synchronized with {len(safe_master_map)} targets.")
            return True, safe_master_map
        except Exception as e:
            logger.error(f"[RECONCILE] Failed to write synchronized /stocklist: {e}", exc_info=True)
            return False, current_stocklist

    return False, current_stocklist


def get_stocklist_mapping(force_reconcile: bool = True) -> dict[str, str]:
    """Returns the active target dictionary."""
    if force_reconcile:
        _, mapping = reconcile_stocklist_with_watchlist()
    else:
        mapping = get_current_stocklist()
        if not mapping:
            _, mapping = reconcile_stocklist_with_watchlist()

    for k, v in FIXED_INDICES.items():
        mapping.setdefault(sanitize_key(k), v)

    return mapping


def get_stocklist() -> list:
    """Returns active target names list."""
    return list(get_stocklist_mapping(force_reconcile=False).keys())


# =====================================================================
# OHLC DATABASE ACCESS (CHILD-1 & CHILD-2)
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