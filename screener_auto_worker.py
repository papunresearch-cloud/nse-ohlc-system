"""
===============================================================================
SCREENER AUTOMATED DOWNLOAD & ETL SYNC WORKER (DYNAMIC EXPORT DETECTOR)
===============================================================================
Automates:
1. Authenticated session via Webshare Proxy.
2. Dynamically scans the screen HTML for the exact Export button / form action.
3. Verifies that the downloaded content is genuine CSV data (not HTML).
4. Runs Screener ETL pipeline (BASIC.py / RUN_PIPELINE.py).
5. Updates Firebase health status and cleans up local storage.
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
# CONFIGURATION & CREDENTIALS
# =====================================================================
RAW_PROXY = os.environ.get("PROXY_URL", "").strip().strip('"').strip("'")
PROXY_URL = RAW_PROXY if RAW_PROXY.startswith(("http://", "https://", "socks5://")) else f"http://{RAW_PROXY}" if RAW_PROXY else ""

SCREENER_SESSION_ID = os.environ.get("SCREENER_SESSION_ID", "").strip().strip('"').strip("'")
SCREENER_USERNAME = os.environ.get("SCREENER_USERNAME", "").strip()
SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD", "").strip()

SCREENER_LOGIN_URL = "https://www.screener.in/login/"
SCREENER_PAGE_URL = "https://www.screener.in/screens/3271948/"

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
    """Authenticates and dynamically locates the CSV export link from the screen page."""
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

    # Step 1: Establish Authentication
    if SCREENER_SESSION_ID:
        logger.info("[AUTO-SYNC] Using SCREENER_SESSION_ID for authentication...")
        session.cookies.set("sessionid", SCREENER_SESSION_ID, domain=".screener.in")
    else:
        logger.info("[AUTO-SYNC] Authenticating via login form through proxy...")
        login_page = session.get(SCREENER_LOGIN_URL, timeout=30)
        csrf_token = session.cookies.get("csrftoken") or ""
        if not csrf_token:
            match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
            if match:
                csrf_token = match.group(1)

        login_payload = {
            "username": SCREENER_USERNAME,
            "password": SCREENER_PASSWORD,
            "csrfmiddlewaretoken": csrf_token,
            "next": ""
        }
        login_resp = session.post(
            SCREENER_LOGIN_URL,
            data=login_payload,
            headers={"Referer": SCREENER_LOGIN_URL, "Origin": "https://www.screener.in"},
            timeout=30,
            allow_redirects=True
        )

        if "sessionid" not in session.cookies:
            raise RuntimeError("Authentication failed on Screener.in. Check username/password.")

    logger.info("[AUTO-SYNC] Screener authentication established.")

    # Step 2: Fetch the Screen Page HTML to extract the exact Export Form/Action
    logger.info(f"[AUTO-SYNC] Scanning screen page: {SCREENER_PAGE_URL}...")
    page_resp = session.get(SCREENER_PAGE_URL, timeout=30)
    if page_resp.status_code != 200:
        raise RuntimeError(f"Failed to access screen page (HTTP {page_resp.status_code})")

    html = page_resp.text
    csrf_token = session.cookies.get("csrftoken", "")

    # Step 3: Discover export endpoints from HTML
    # Look for: <form action="...export..." or <a href="...export..."
    export_action_match = re.search(r'<form[^>]+action=["\']([^"\']*export[^"\']*)["\']', html, re.IGNORECASE)
    export_link_match = re.search(r'<a[^>]+href=["\']([^"\']*export[^"\']*)["\']', html, re.IGNORECASE)

    export_url = None
    is_post = True

    if export_action_match:
        action = export_action_match.group(1)
        export_url = action if action.startswith("http") else f"https://www.screener.in{action}"
        is_post = True
        logger.info(f"[AUTO-SYNC] Detected dynamic export form action: {export_url}")
    elif export_link_match:
        link = export_link_match.group(1)
        export_url = link if link.startswith("http") else f"https://www.screener.in{link}"
        is_post = False
        logger.info(f"[AUTO-SYNC] Detected export URL link: {export_url}")
    else:
        # Fallback to standard export route
        export_url = "https://www.screener.in/api/export/screen/3271948/"
        logger.warning(f"[AUTO-SYNC] Dynamic export element not found in HTML. Using default endpoint: {export_url}")

    export_headers = {
        "Referer": SCREENER_PAGE_URL,
        "Origin": "https://www.screener.in"
    }

    # Step 4: Execute download request
    if is_post:
        export_resp = session.post(
            export_url,
            data={"csrfmiddlewaretoken": csrf_token} if csrf_token else {},
            headers=export_headers,
            timeout=60
        )
    else:
        export_resp = session.get(export_url, headers=export_headers, timeout=60)

    # Step 5: Validate file content (Ensure it is a valid CSV, not an HTML error page)
    content_preview = export_resp.content[:150].decode("utf-8", errors="ignore").strip().lower()
    if content_preview.startswith("<!doctype html") or "<html" in content_preview:
        raise ValueError(
            "Screener returned an HTML webpage instead of a CSV file. "
            "This usually means the screen has no export permission on this account or the endpoint requires a premium tier."
        )

    with open(OUTPUT_CSV_FILE, "wb") as f:
        f.write(export_resp.content)

    if not OUTPUT_CSV_FILE.exists() or OUTPUT_CSV_FILE.stat().st_size == 0:
        raise FileNotFoundError("Downloaded screener.csv is empty or missing.")

    logger.info(
        f"[AUTO-SYNC] Valid CSV successfully downloaded: {OUTPUT_CSV_FILE} "
        f"({OUTPUT_CSV_FILE.stat().st_size} bytes)"
    )
    return OUTPUT_CSV_FILE


def execute_auto_screener_sync(database_date: str = None):
    """Coordinates download, ETL scoring pipeline, and telemetry updates."""
    if not run_screener_pipeline:
        raise RuntimeError("Screener ETL pipeline runner is missing (RUN_PIPELINE.py / BASIC.py).")

    now_ist = datetime.now(IST)
    effective_date = database_date or now_ist.strftime("%Y-%m-%d")

    downloaded_file = None
    root_screener = Path.cwd() / "screener.csv"

    try:
        # Step 1: Download validated CSV
        downloaded_file = download_screener_csv()

        # Step 2: Position file for BASIC.py
        shutil.copyfile(downloaded_file, root_screener)

        # Step 3: Run existing Screener ETL pipeline
        logger.info("[AUTO-SYNC] Executing Screener ETL pipeline...")
        run_screener_pipeline()
        logger.info("[AUTO-SYNC] Pipeline stages finished successfully.")

        # Step 4: Update health telemetry
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
        # Step 5: Clean up temporary files
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