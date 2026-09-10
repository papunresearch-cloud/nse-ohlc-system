"""
yahoo_manager.py - Data Vendor Client & Normalizer
Guarantees a clean DataFrame with explicit columns:
['date', 'open', 'high', 'low', 'close', 'volume']
Includes intraday fetching for CHILD-2, exchange date lookups,
and automatic ticker normalization/translation for market indices.
"""
import time
import math
from datetime import datetime, timedelta, date
import pandas as pd
import yfinance as yf
import pytz

from config import (
    TIMEZONE,
    REQUEST_DELAY_SEC,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    COOLDOWN_ON_429_SEC,
    logger
)

IST = pytz.timezone(TIMEZONE)


def get_yahoo_ticker(script: str) -> str:
    """
    Translates script identifiers or keys to valid Yahoo Finance tickers.
    Intercepts and cleans accidental .NS additions on index benchmarks.
    """
    if not script:
        return ""
    cleaned = str(script).strip()
    if cleaned.startswith("^"):
        return cleaned

    # 1. Normalize: strip accidental .NS / .BO and spaces to inspect the core symbol
    bare = cleaned.upper().replace(".NS", "").replace(".BO", "").replace(" ", "").replace("_", "")

    # 2. Benchmark Index Map
    index_map = {
        "NIFTY50": "^NSEI",
        "NIFTY100": "^CNX100",
        "NIFTYMIDCAP150": "NIFTYMIDCAP150.NS",
        "NIFTYSMALLCAP250": "^CNXSC",
        "NIFTYSMLCAP250": "^CNXSC",
        "BANKNIFTY": "^NSEBANK",
        "SENSEX": "^BSESN"
    }

    if bare in index_map:
        return index_map[bare]

    # 3. Standard equity fallback
    if not cleaned.endswith(".NS") and not cleaned.endswith(".BO"):
        return f"{cleaned.replace(' ', '')}.NS"
        
    return cleaned


def download_historical_daily(ticker: str, days_needed: int) -> pd.DataFrame:
    """
    Downloads daily historical candles from Yahoo Finance and returns a normalized
    DataFrame where 'date' is a concrete column formatted as 'YYYY-MM-DD'.
    
    Returns:
        pd.DataFrame with columns ['date', 'open', 'high', 'low', 'close', 'volume'],
        or an empty DataFrame on failure.
    """
    actual_ticker = get_yahoo_ticker(ticker)
    calendar_days = math.ceil(days_needed * 1.5 + 15)
    end_dt = datetime.now(IST)
    start_dt = end_dt - timedelta(days=calendar_days)

    start_str = start_dt.strftime("%Y-%m-%d")
    # Buffer added to prevent exclusive end-date clipping on UTC/IST boundaries
    end_str = (end_dt + timedelta(days=2)).strftime("%Y-%m-%d")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_DELAY_SEC)
            
            df_raw = yf.download(
                tickers=actual_ticker,
                start=start_str,
                end=end_str,
                interval="1d",
                auto_adjust=False,
                progress=False
            )

            if df_raw is not None and not df_raw.empty:
                normalized = _normalize_df(df_raw)
                if not normalized.empty:
                    return normalized

            logger.warning(f"[{actual_ticker}] Attempt {attempt}: Received empty or invalid historical payload.")

        except Exception as e:
            err_str = str(e).lower()
            if "429" in err_str or "too many requests" in err_str:
                logger.warning(f"[{actual_ticker}] HTTP 429 encountered. Cooling down for {COOLDOWN_ON_429_SEC}s...")
                time.sleep(COOLDOWN_ON_429_SEC)
            else:
                backoff = BACKOFF_FACTOR ** attempt
                logger.warning(f"[{actual_ticker}] Attempt {attempt} failed: {e}. Backing off {backoff:.1f}s...")
                time.sleep(backoff)

    return pd.DataFrame()


def download_intraday_today(ticker: str) -> dict:
    """
    Fetches the active live market candle for ticker (CHILD-2 / Index 0).
    Returns dict: {'date': 'YYYY-MM-DD', 'open': float, 'high': float, 'low': float, 'close': float, 'volume': int}
    """
    actual_ticker = get_yahoo_ticker(ticker)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_DELAY_SEC)
            t = yf.Ticker(actual_ticker)
            df = t.history(period="1d", interval="1m")
            
            if df is None or df.empty:
                # Fallback to daily 1d snapshot if 1m is momentarily empty
                df = t.history(period="1d", interval="1d")

            if df is not None and not df.empty:
                # Flatten MultiIndex if present
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df.columns = [str(c).strip().lower() for c in df.columns]

                now_ist = datetime.now(IST)
                today_str = now_ist.strftime("%Y-%m-%d")

                # Accumulate intraday stats
                c_open = float(df["open"].iloc[0])
                c_high = float(df["high"].max())
                c_low = float(df["low"].min())
                c_close = float(df["close"].iloc[-1])
                c_vol = int(df["volume"].sum()) if "volume" in df.columns else 0

                return {
                    "date": today_str,
                    "open": round(c_open, 2),
                    "high": round(c_high, 2),
                    "low": round(c_low, 2),
                    "close": round(c_close, 2),
                    "volume": c_vol
                }

        except Exception as e:
            logger.warning(f"[{actual_ticker}] Intraday fetch attempt {attempt} failed: {e}")
            time.sleep(1)

    return {}


def get_latest_available_trading_date(ticker: str) -> date:
    """
    Interrogates Yahoo Finance to discover the latest completed daily session date.
    """
    actual_ticker = get_yahoo_ticker(ticker)
    try:
        t = yf.Ticker(actual_ticker)
        df = t.history(period="5d", interval="1d")
        if df is not None and not df.empty:
            last_dt = df.index[-1]
            if hasattr(last_dt, "date"):
                return last_dt.date()
            return datetime.strptime(str(last_dt)[:10], "%Y-%m-%d").date()
    except Exception as e:
        logger.warning(f"[{actual_ticker}] Failed to detect latest vendor date: {e}")
    return None


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens MultiIndex headers, moves DatetimeIndex into a concrete 'date' column,
    converts timestamps to IST, and lowercases all column names.
    Rejects partial frames missing essential OHLC price feeds.
    """
    clean_df = df.copy()

    # 1. Flatten MultiIndex columns
    if isinstance(clean_df.columns, pd.MultiIndex):
        clean_df.columns = clean_df.columns.get_level_values(0)

    # 2. Extract DatetimeIndex into a dedicated 'date' column
    if isinstance(clean_df.index, pd.DatetimeIndex):
        dt_index = clean_df.index
        if dt_index.tz is None:
            dt_index = dt_index.tz_localize("UTC").tz_convert(IST)
        else:
            dt_index = dt_index.tz_convert(IST)
        clean_df["date"] = dt_index.strftime("%Y-%m-%d")
        clean_df = clean_df.reset_index(drop=True)
    elif "Date" in clean_df.columns or "date" in clean_df.columns:
        date_col = "Date" if "Date" in clean_df.columns else "date"
        clean_df["date"] = pd.to_datetime(clean_df[date_col]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
        if date_col != "date":
            clean_df = clean_df.drop(columns=[date_col])

    # 3. Normalize all column headers to lowercase strings
    clean_df.columns = [str(c).strip().lower() for c in clean_df.columns]

    # 4. Strict Column Integrity Gate (Reject missing OHLC feeds)
    price_cols = ["open", "high", "low", "close"]
    if not all(col in clean_df.columns for col in price_cols):
        return pd.DataFrame()

    # Volume defaults safely to 0 if absent
    if "volume" not in clean_df.columns:
        clean_df["volume"] = 0

    required_cols = ["date", "open", "high", "low", "close", "volume"]
    return clean_df[required_cols]