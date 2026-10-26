"""
===============================================================================
SCREENER AUTOMATED DOWNLOAD & ETL SYNC WORKER (WEBSHARE PROXY ENABLED)
===============================================================================
Automates:
1. Proxied login & CSV export from screener.in via requests.Session.
2. Copies screener.csv to root directory for BASIC.py ingestion.
3. Runs the Screener ETL pipeline (RUN_PIPELINE.py / BASIC.py).
4. Updates health telemetry to /system_status/screener_sync ("Last Screener DB").
5. Deletes temporary files from local storage upon completion.
===============================================================================
"""

import os
import re
import sys
import shutil
from pathlib import Path
from datetime import datetime
import pytz
import requests

from firebase_admin import db
from config import TIMEZONE, logger

IST = pytz.timezone(TIMEZONE)

# =====================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# =====================================================================
SCREENER_USERNAME = os.environ.get("SCREENER_USERNAME", "").strip()
SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD", "").strip()

# Clean proxy string in case quotes or whitespace were pasted
RAW_PROXY = os.environ.get("PROXY_URL", "").strip().strip('"').strip("'")
PROXY_URL = RAW_PROXY if RAW_PROXY.startswith(("http://", "https://", "socks5://")) else f"http://{RAW_PROXY}" if RAW_PROXY else ""

SCREENER_LOGIN_URL = "https://www.screener.in/login/"
SCREENER_PAGE_URL = "https://www.screener.in/screens/3271948/screener/"
SCREENER_EXPORT_API_URL = "https://www.screener.in/api/export/screen/3271948/"

TEMP_DOWNLOAD_DIR = Path.cwd() / "temp_screener_download"
OUTPUT_CSV_FILE = TEMP_DOWNLOAD_DIR / "screener.csv"

# Pipeline ETL runner resolution
try:
    from RUN_PIPELINE import main as run_screener_pipeline
except ImportError:
    try:
        from basic import run_pipeline as run_screener_pipeline
    except ImportError:
        try:
            from BASIC import run_pipeline as run_screener_pipeline
        except ImportError:
            run_screener_pipeline = None


def download_screener_csv() -> Path:
    """Authenticates and downloads screener.csv through the Webshare proxy."""
    if not PROXY_URL:
        raise ValueError("PROXY_URL is missing. Please set PROXY_URL in Render Environment Variables.")
    if not SCREENER_USERNAME or not SCREENER_PASSWORD:
        raise ValueError("Screener credentials missing. Set SCREENER_USERNAME & SCREENER_PASSWORD in Render.")

    TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if OUTPUT_CSV_FILE.exists():
        OUTPUT_CSV_FILE.unlink()

    session = requests.Session()
    session.proxies = {
        "http": PROXY_URL,
        "https": PROXY_URL,
    }
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    })

    # Step 1: GET login page to obtain initial CSRF token cookie
    logger.info("[AUTO-SYNC] Connecting to Screener login page via proxy...")
    login_page = session.get(SCREENER_LOGIN_URL, timeout=30)
    
    if login_page.status_code != 200:
        raise RuntimeError(f"Could not open login page via proxy (HTTP {login_page.status_code}).")

    csrf_token = session.cookies.get("csrftoken") or ""
    if not csrf_token:
        match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
        if match:
            csrf_token = match.group(1)

    # Step 2: POST credentials to authenticate session
    logger.info("[AUTO-SYNC] Authenticating credentials via proxy...")
    login_payload = {
        "username": SCREENER_USERNAME,
        "password": SCREENER_PASSWORD,
        "csrfmiddlewaretoken": csrf_token
    }
    login_headers = {
        "Referer": SCREENER_LOGIN_URL
    }

    login_resp = session.post(
        SCREENER_LOGIN_URL,
        data=login_payload,
        headers=login_headers,
        timeout=30,
        allow_redirects=True
    )

    if "sessionid" not in session.cookies:
        raise RuntimeError(
            "Authentication failed on Screener.in. "
            "Please check SCREENER_USERNAME and SCREENER_PASSWORD in Render Environment."
        )

    logger.info("[AUTO-SYNC] Screener authentication successful.")

    # Step 3: Trigger CSV export request
    logger.info("[AUTO-SYNC] Requesting CSV export for screen 3271948 via proxy...")
    csrf_token = session.cookies.get("csrftoken", csrf_token)
    export_headers = {
        "Referer": SCREENER_PAGE_URL
    }

    export_resp = session.post(
        SCREENER_EXPORT_API_URL,
        data={"csrfmiddlewaretoken": csrf_token},
        headers=export_headers,
        timeout=60
    )

    # Fallback to GET export URL if POST returned non-200
    if export_resp.status_code != 200:
        logger.info("[AUTO-SYNC] POST returned non-200. Attempting direct GET export fallback...")
        fallback_url = "https://www.screener.in/screens/3271948/export/"
        export_resp = session.get(fallback_url, headers=export_headers, timeout=60)

    if export_resp.status_code != 200:
        raise RuntimeError(
            f"Screener export request failed with HTTP {export_resp.status_code}: {export_resp.text[:200]}"
        )

    # Step 4: Write CSV to local storage
    with open(OUTPUT_CSV_FILE, "wb") as f:
        f.write(export_resp.content)

    if not OUTPUT_CSV_FILE.exists() or OUTPUT_CSV_FILE.stat().st_size == 0:
        raise FileNotFoundError("Downloaded screener.csv is empty or was not created.")

    logger.info(
        f"[AUTO-SYNC] Screener CSV downloaded successfully: {OUTPUT_CSV_FILE} "
        f"({OUTPUT_CSV_FILE.stat().st_size} bytes)"
    )
    return OUTPUT_CSV_FILE


def execute_auto_screener_sync(database_date: str = None):
    """Executes the automated download, pipeline execution, health update, and file cleanup."""
    if not run_screener_pipeline:
        raise RuntimeError("Screener ETL pipeline runner is missing (RUN_PIPELINE.py or BASIC.py).")

    now_ist = datetime.now(IST)
    effective_date = database_date or now_ist.strftime("%Y-%m-%d")

    downloaded_file = None
    root_screener = Path.cwd() / "screener.csv"

    try:
        # Step 1: Download screener.csv via proxy
        downloaded_file = download_screener_csv()

        # Step 2: Copy to root cwd for BASIC.py
        shutil.copyfile(downloaded_file, root_screener)

        # Step 3: Run existing Screener ETL pipeline
        logger.info("[AUTO-SYNC] Executing Screener ETL pipeline...")
        run_screener_pipeline()
        logger.info("[AUTO-SYNC] Pipeline stages finished successfully.")

        # Step 4: Update health telemetry -> Last Screener DB
        now_ist_str = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
        db.reference("system_status/screener_sync").update({
            "Date of database data": effective_date,
            "last_screener_db": now_ist_str,
            "last_sync_time": now_ist_str,
            "status": "SUCCESS",
            "sync_source": "AUTO_SYNC"
        })
        logger.info(f"[AUTO-SYNC] Health status updated: Last Screener DB -> {now_ist_str}")

    except Exception as e:
        logger.error(f"[AUTO-SYNC] Error during auto screener sync: {e}", exc_info=True)
        try:
            db.reference("system_status/screener_sync").update({
                "status": f"FAILED: {str(e)[:100]}",
                "last_error_time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            })
        except Exception:
            pass
        raise e

    finally:
        # Step 5: Clean up temporary screener.csv files from storage
        logger.info("[AUTO-SYNC] Cleaning up temporary storage...")
        try:
            if downloaded_file and downloaded_file.exists():
                downloaded_file.unlink()
            if root_screener.exists():
                root_screener.unlink()
            if TEMP_DOWNLOAD_DIR.exists():
                shutil.rmtree(TEMP_DOWNLOAD_DIR, ignore_errors=True)
            logger.info("[AUTO-SYNC] Temporary files cleaned successfully.")
        except Exception as clean_err:
            logger.warning(f"[AUTO-SYNC] Cleanup warning: {clean_err}")