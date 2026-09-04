"""
Yahoo Finance interaction manager.
Provides safe ticker resolution, retry with backoff, rate-limit cooldowns,
and date/price sanitization for daily and intraday intervals.
"""
import time
from datetime import datetime, date
import requests
import yfinance as yf
import pandas as pd
import pytz
from config import (
    TIMEZONE,
    REQUEST_DELAY_SEC,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    COOLDOWN_ON_429_SEC,
    logger
)

# Shared browser-mimicking session to bypass basic cloud blocks
_session = requests.Session()
_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
})

IST = pytz.timezone(TIMEZONE)


def get_yahoo_ticker(script: str) -> str:
    """
    Translates script identifiers to Yahoo-compatible tickers.
    Indexes starting with '^' are not suffixed with '.NS'.
    """
    cleaned = script.strip().upper()
    if cleaned.startswith("^"):
        return cleaned
    # Custom mapping dictionary for special overrides
    custom_map = {
        "NIFTY50": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "SENSEX": "^BSESN"
    }
    if cleaned in custom_map:
        return custom_map[cleaned]
    if not cleaned.endswith(".NS") and not cleaned.endswith(".BO"):
        return f"{cleaned}.NS"
    return cleaned


def download_historical_daily(script: str, days_needed: int) -> pd.DataFrame:
    """
    Downloads daily historical OHLC with retries, exponential backoff,
    and 429 rate-limit cooldown protection.
    """
    ticker_sym = get_yahoo_ticker(script)
    # Estimate calendar days needed (~1.45 multiplier for weekends/holidays)
    calendar_days = max(int(days_needed * 1.5) + 15, 30)
    period_str = f"{calendar_days}d"
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_DELAY_SEC)
            ticker = yf.Ticker(ticker_sym, session=_session)
            hist = ticker.history(period=period_str, interval="1d", auto_adjust=False)
            
            if hist.empty:
                logger.warning(f"Empty historical dataframe returned for {script} (Attempt {attempt}/{MAX_RETRIES})")
                time.sleep(BACKOFF_FACTOR ** attempt)
                continue
            
            return _normalize_df(hist)

        except requests.exceptions.HTTPError as he:
            if hasattr(he.response, 'status_code') and he.response.status_code == 429:
                logger.error(f"HTTP 429 Rate Limit encountered on {script}. Entering cooldown {COOLDOWN_ON_429_SEC}s...")
                time.sleep(COOLDOWN_ON_429_SEC)
            else:
                logger.warning(f"HTTP error on {script}: {he}. Retrying...")
                time.sleep(BACKOFF_FACTOR ** attempt)
        except Exception as e:
            if "429" in str(e):
                logger.error(f"Rate limit detected for {script}. Cooling down...")
                time.sleep(COOLDOWN_ON_429_SEC)
            else:
                logger.warning(f"Fetch failed for {script} ({e}). Attempt {attempt}/{MAX_RETRIES}")
            time.sleep(BACKOFF_FACTOR ** attempt)

    logger.error(f"Exhausted all {MAX_RETRIES} attempts fetching historical data for {script}")
    return pd.DataFrame()


def download_intraday_today(script: str) -> dict | None:
    """
    Retrieves current active candle for index 0 during LIVE market hours.
    Extracts open, high, low, and current price (as close).
    """
    ticker_sym = get_yahoo_ticker(script)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_DELAY_SEC)
            ticker = yf.Ticker(ticker_sym, session=_session)
            
            # Fast info retrieval preferred
            fast = getattr(ticker, "fast_info", None)
            if fast:
                try:
                    last_price = float(fast.last_price)
                    day_open = float(fast.open)
                    day_high = float(fast.day_high)
                    day_low = float(fast.day_low)
                    
                    if day_open > 0 and last_price > 0:
                        today_str = datetime.now(IST).strftime("%Y-%m-%d")
                        return {
                            "date": today_str,
                            "open": round(day_open, 2),
                            "high": round(max(day_high, last_price, day_open), 2),
                            "low": round(min(day_low, last_price, day_open), 2),
                            "close": round(last_price, 2)
                        }
                except Exception:
                    pass  # Fall back to 1d history
            
            hist = ticker.history(period="1d", interval="5m", auto_adjust=False)
            if not hist.empty:
                hist = _normalize_df(hist)
                day_open = float(hist['open'].iloc[0])
                day_high = float(hist['high'].max())
                day_low = float(hist['low'].min())
                last_price = float(hist['close'].iloc[-1])
                today_str = datetime.now(IST).strftime("%Y-%m-%d")
                
                return {
                    "date": today_str,
                    "open": round(day_open, 2),
                    "high": round(max(day_high, last_price), 2),
                    "low": round(min(day_low, last_price), 2),
                    "close": round(last_price, 2)
                }

        except Exception as e:
            if "429" in str(e):
                logger.error(f"429 rate limit during live fetch on {script}. Sleeping {COOLDOWN_ON_429_SEC}s")
                time.sleep(COOLDOWN_ON_429_SEC)
            else:
                logger.warning(f"Live fetch failed for {script}: {e}. Attempt {attempt}/{MAX_RETRIES}")
            time.sleep(BACKOFF_FACTOR)

    return None


def get_latest_available_trading_date(script: str) -> date | None:
    """Queries small 5-day window to ascertain latest valid date finalized by Yahoo."""
    ticker_sym = get_yahoo_ticker(script)
    try:
        time.sleep(REQUEST_DELAY_SEC)
        ticker = yf.Ticker(ticker_sym, session=_session)
        hist = ticker.history(period="5d", interval="1d", auto_adjust=False)
        if hist.empty:
            return None
        hist = _normalize_df(hist)
        latest_str = hist['date'].iloc[-1]
        return datetime.strptime(latest_str, "%Y-%m-%d").date()
    except Exception as e:
        logger.warning(f"Unable to query latest Yahoo date for {script}: {e}")
        return None


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes DataFrame timestamps to YYYY-MM-DD strings in Asia/Kolkata."""
    df = df.copy()
    if df.empty:
        return df

    # Normalize Index to Datetime in IST
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            df.index = df.index.tz_localize(pytz.UTC).tz_convert(IST)
        else:
            df.index = df.index.tz_convert(IST)
        df['date'] = df.index.strftime("%Y-%m-%d")
    else:
        df['date'] = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d")

    df.columns = [str(col).lower() for col in df.columns]
    req_cols = ['date', 'open', 'high', 'low', 'close']
    return df[req_cols].dropna()