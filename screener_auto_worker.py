"""
===============================================================================
SCREENER AUTOMATED DOWNLOAD & ETL SYNC WORKER (OPTION 2: HTTP SESSION)
===============================================================================
Automates:
1. Direct login to Screener.in via requests.Session (zero Playwright).
2. Authenticated export download of screen 3271948.
3. Screener ETL & scoring pipeline execution (RUN_PIPELINE.py / BASIC.py).
4. Health telemetry update to /system_status/screener_sync ("Last Screener DB").
5. Clean deletion of temporary files from local storage.
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
# CREDENTIALS CONFIGURATION
# =====================================================================
# You can set these directly as strings below, or configure them
# as Environment Variables on Render (SCREENER_USERNAME & SCREENER_PASSWORD)
SCREENER_USERNAME = os.environ.get("SCREENER_USERNAME", "mailme.supriyabiswas@gmail.com")
SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD", "screener2026")

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
    """Authenticates with Screener.in and downloads screener.csv via requests.Session."""
    TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if OUTPUT_CSV_FILE.exists():
        OUTPUT_CSV_FILE.unlink()

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    })

    # Step 1: GET login page to obtain initial CSRF token cookie
    logger.info("[AUTO-SYNC] Fetching Screener login page...")
    login_page = session.get(SCREENER_LOGIN_URL, timeout=30)
    csrf_token = session.cookies.get("csrftoken") or ""
    if not csrf_token:
        match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
        if match:
            csrf_token = match.group(1)

    # Step 2: POST credentials to authenticate session
    logger.info("[AUTO-SYNC] Submitting credentials to Screener...")
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
            "Failed to authenticate with Screener.in. "
            "Please check your username and password, or check if Screener is requesting a captcha."
        )

    logger.info("[AUTO-SYNC] Screener authentication successful.")

    # Step 3: Trigger CSV export request
    logger.info(f"[AUTO-SYNC] Requesting CSV export for screen 3271948...")
    csrf_token = session.cookies.get("csrftoken", csrf_token)
    export_headers = {
        "Referer": SCREENER_PAGE_URL
    }

    # Screener export button is a POST form action to /api/export/screen/3271948/
    export_resp = session.post(
        SCREENER_EXPORT_API_URL,
        data={"csrfmiddlewaretoken": csrf_token},
        headers=export_headers,
        timeout=60
    )

    # Fallback to GET if Screener allows authenticated GET export
    if export_resp.status_code != 200:
        logger.info("[AUTO-SYNC] POST export returned non-200. Attempting direct authenticated GET fallback...")
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
        # Step 1: Direct download screener.csv via HTTP session
        downloaded_file = download_screener_csv()

        # Step 2: Copy to root cwd if downstream scripts expect 'screener.csv' in root
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