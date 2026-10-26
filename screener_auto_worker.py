"""
===============================================================================
SCREENER AUTOMATED DOWNLOAD & ETL SYNC WORKER (WEBSHARE PROXY ENABLED)
===============================================================================
Automates:
1. Authenticates via Webshare proxy with Screener credentials / session cookie.
2. Loads https://www.screener.in/screens/3271948/screener/ to obtain fresh CSRF.
3. Posts to /api/export/screen/3271948/ with matching CSRF and Referer headers.
4. Validates downloaded CSV content (rejects HTML fallbacks).
5. Runs Screener ETL pipeline (BASIC.py / RUN_PIPELINE.py).
6. Updates Firebase telemetry under /system_status/screener_sync and cleans up.
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

# Clean proxy string and credentials
RAW_PROXY = os.environ.get("PROXY_URL", "").strip().strip('"').strip("'")
PROXY_URL = RAW_PROXY if RAW_PROXY.startswith(("http://", "https://", "socks5://")) else f"http://{RAW_PROXY}" if RAW_PROXY else ""

SCREENER_SESSION_ID = os.environ.get("SCREENER_SESSION_ID", "").strip().strip('"').strip("'")
SCREENER_USERNAME = os.environ.get("SCREENER_USERNAME", "").strip()
SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD", "").strip()

SCREENER_LOGIN_URL = "https://www.screener.in/login/"
# NOTE: The slug '/screener/' is required by Screener.in to prevent HTTP 404
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
    """Authenticates and downloads screener.csv through proxy."""
    if not PROXY_URL:
        raise ValueError("PROXY_URL is missing. Please set PROXY_URL in Render Environment Variables.")

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
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })

    # Step 1: Authentication
    if SCREENER_SESSION_ID:
        logger.info("[AUTO-SYNC] Using SCREENER_SESSION_ID for authentication...")
        session.cookies.set("sessionid", SCREENER_SESSION_ID, domain=".screener.in")
    else:
        if not SCREENER_USERNAME or not SCREENER_PASSWORD:
            raise ValueError("Configure SCREENER_SESSION_ID or SCREENER_USERNAME & SCREENER_PASSWORD in Render.")

        logger.info("[AUTO-SYNC] Connecting to Screener login page via proxy...")
        login_page = session.get(SCREENER_LOGIN_URL, timeout=30)
        csrf_token = session.cookies.get("csrftoken") or ""
        if not csrf_token:
            match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
            if match:
                csrf_token = match.group(1)

        logger.info("[AUTO-SYNC] Authenticating credentials via proxy...")
        login_payload = {
            "username": SCREENER_USERNAME,
            "password": SCREENER_PASSWORD,
            "csrfmiddlewaretoken": csrf_token,
            "next": ""
        }
        login_headers = {
            "Referer": SCREENER_LOGIN_URL,
            "Origin": "https://www.screener.in"
        }

        login_resp = session.post(
            SCREENER_LOGIN_URL,
            data=login_payload,
            headers=login_headers,
            timeout=30,
            allow_redirects=True
        )

        if "sessionid" not in session.cookies:
            raise RuntimeError("Authentication failed on Screener.in. Please check credentials.")

        logger.info("[AUTO-SYNC] Screener authentication successful.")

    # Step 2: Access the screen page (with '/screener/' slug) to get screen CSRF token
    logger.info(f"[AUTO-SYNC] Accessing screen page: {SCREENER_PAGE_URL}...")
    page_resp = session.get(SCREENER_PAGE_URL, timeout=30)
    if page_resp.status_code != 200:
        raise RuntimeError(f"Failed to access screen page: HTTP {page_resp.status_code}")

    html = page_resp.text

    # Extract CSRF token from page HTML or session cookies
    page_csrf = ""
    csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', html)
    if csrf_match:
        page_csrf = csrf_match.group(1)
    else:
        page_csrf = session.cookies.get("csrftoken", "")

    # Look for the export action form URL inside the page markup
    export_action_match = re.search(r'<form[^>]+action=["\']([^"\']*export[^"\']*)["\']', html, re.IGNORECASE)
    if export_action_match:
        action = export_action_match.group(1)
        export_url = action if action.startswith("http") else f"https://www.screener.in{action}"
    else:
        export_url = SCREENER_EXPORT_API_URL

    logger.info(f"[AUTO-SYNC] Export target endpoint: {export_url}")

    # Step 3: Trigger export POST request
    export_headers = {
        "Referer": SCREENER_PAGE_URL,
        "Origin": "https://www.screener.in"
    }
    export_payload = {"csrfmiddlewaretoken": page_csrf} if page_csrf else {}

    export_resp = session.post(
        export_url,
        data=export_payload,
        headers=export_headers,
        timeout=60
    )

    # Step 4: Validate content (Must be CSV, not HTML error page)
    content_head = export_resp.content[:200].decode("utf-8", errors="ignore").strip().lower()
    if content_head.startswith("<!doctype html") or "<html" in content_head:
        raise ValueError(
            f"Export request returned an HTML page instead of CSV (HTTP {export_resp.status_code}). "
            "Please verify that this Screener account has permission to export this screen."
        )

    if export_resp.status_code != 200:
        raise RuntimeError(f"Export download failed with HTTP {export_resp.status_code}")

    with open(OUTPUT_CSV_FILE, "wb") as f:
        f.write(export_resp.content)

    if not OUTPUT_CSV_FILE.exists() or OUTPUT_CSV_FILE.stat().st_size == 0:
        raise FileNotFoundError("Downloaded screener.csv is empty or missing.")

    logger.info(
        f"[AUTO-SYNC] Screener CSV downloaded successfully: {OUTPUT_CSV_FILE} "
        f"({OUTPUT_CSV_FILE.stat().st_size} bytes)"
    )
    return OUTPUT_CSV_FILE


def execute_auto_screener_sync(database_date: str = None):
    """Coordinates download, ETL pipeline execution, health update, and cleanup."""
    if not run_screener_pipeline:
        raise RuntimeError("Screener ETL pipeline runner is missing (RUN_PIPELINE.py or BASIC.py).")

    now_ist = datetime.now(IST)
    effective_date = database_date or now_ist.strftime("%Y-%m-%d")

    downloaded_file = None
    root_screener = Path.cwd() / "screener.csv"

    try:
        # Step 1: Download validated CSV
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