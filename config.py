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
# TIMING & SCHEDULE WINDOWS (IST)
# ==========================================
SYNC_WINDOW_START_HOUR = 8
SYNC_WINDOW_START_MIN = 0
SYNC_WINDOW_DEADLINE_HOUR = 8
SYNC_WINDOW_DEADLINE_MIN = 30

SYNC_RETRY_INTERVAL_SEC = 300       # Retry failed syncs every 5 mins between 08:00 and 08:30
LIVE_UPDATE_INTERVAL_SEC = 300      # 5 mins live candle refresh
PULSE_VALIDITY_SEC = 30             # 30-second TTL pulse duration
HEARTBEAT_TICK_SEC = 2              # Fast main loop poll rate

# ==========================================
# YAHOO RATE-LIMIT PROTECTION
# ==========================================
REQUEST_DELAY_SEC = float(os.getenv("REQUEST_DELAY_SEC", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "2.0"))
COOLDOWN_ON_429_SEC = int(os.getenv("COOLDOWN_ON_429_SEC", "60"))

# ==========================================
# INDEXING BOUNDARIES
# ==========================================
HISTORICAL_START_INDEX = 1
TARGET_OHLC_COUNT = 250      # Exactly indices 1 through 250 for history
INTRADAY_INDEX = "0"         # Reserved strictly for live intraday market bar

# Market Session Reset Times (IST)
MARKET_PRE_OPEN_RESET_TIME = "09:00"
MARKET_POST_CLOSE_RESET_TIME = "16:00"