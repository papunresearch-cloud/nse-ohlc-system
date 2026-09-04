"""
Configuration module for the NSE Equity OHLC Database Maintenance System.
"""
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# ENVIRONMENT & RUNTIME FLAGS
# ==========================================
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("true", "1", "yes")
SIMULATED_TIME = os.getenv("SIMULATED_TIME", None)

LOG_LEVEL = logging.DEBUG if DEBUG else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NSE_OHLC_SYSTEM")

# ==========================================
# FIREBASE CONFIGURATION
# ==========================================
FIREBASE_DATABASE_URL = os.getenv(
    "FIREBASE_DATABASE_URL",
    "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app/"
)
FIREBASE_CREDENTIALS = os.getenv("FIREBASE_CREDENTIALS", "serviceAccountKey.json")

# Database Paths: Reading from 'stocklist' node
PATH_SCRIPTS = "stocklist"
PATH_STOCKS = "stocks"
PATH_CALENDAR = "config/nse_calendar"

# ==========================================
# CORE CONSTANTS
# ==========================================
TARGET_OHLC_COUNT = 250
DEFAULT_SAFETY_MARGIN = 5
TIMEZONE = "Asia/Kolkata"

# ==========================================
# SCHEDULING INTERVALS (Seconds)
# ==========================================
LIVE_UPDATE_INTERVAL_SEC = int(os.getenv("LIVE_UPDATE_INTERVAL_SEC", "300"))
HIST_SYNC_INTERVAL_SEC = int(os.getenv("HIST_SYNC_INTERVAL_SEC", "1200"))
MARKET_CHECK_INTERVAL_SEC = int(os.getenv("MARKET_CHECK_INTERVAL_SEC", "60"))

# ==========================================
# YAHOO RATE-LIMIT PROTECTION
# ==========================================
REQUEST_DELAY_SEC = float(os.getenv("REQUEST_DELAY_SEC", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "2.0"))
COOLDOWN_ON_429_SEC = int(os.getenv("COOLDOWN_ON_429_SEC", "60"))