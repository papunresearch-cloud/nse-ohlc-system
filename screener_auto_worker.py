"""
===============================================================================
SCREENER AUTOMATED DOWNLOAD & ETL SYNC WORKER (DYNAMIC MARKUP SCANNER)
===============================================================================
Automates:
1. Authenticates via Webshare Proxy with credentials / session cookie.
2. Dynamically scans https://www.screener.in/screens/3271948/ to detect:
   - Dynamic form actions (<form action="...export...">)
   - Export download links (<a href="...export...">)
   - Form-embedded CSRF tokens
3. Validates content (ensures genuine CSV, prevents HTML error saves).
4. Feeds screener.csv to Screener ETL pipeline (BASIC.py / RUN_PIPELINE.py).
5. Updates health telemetry to Firebase and safely deletes temporary files.
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
    """Authenticates and dynamically inspects the screen DOM to export CSV."""
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

    # -------------------------------------------------------------
    # Step 1: Authentication (Session Cookie or Form Login)
    # -------------------------------------------------------------
    if SCREENER_SESSION_ID:
        logger.info("[AUTO-SYNC] Using SCREENER_SESSION_ID for direct authenticated session...")
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

    # -------------------------------------------------------------
    # Step 2: Dynamically Scan Screen HTML Page
    # -------------------------------------------------------------
    logger.info(f"[AUTO-SYNC] Scanning screen page HTML at {SCREENER_PAGE_URL}...")
    page_resp = session.get(SCREENER_PAGE_URL, timeout=30)
    if page_resp.status_code != 200:
        raise RuntimeError(f"Failed to access screen page: HTTP {page_resp.status_code}")

    html = page_resp.text

    # Extract CSRF token from page DOM if present
    page_csrf = ""
    csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', html)
    if csrf_match:
        page_csrf = csrf_match.group(1)
    else:
        page_csrf = session.cookies.get("csrftoken", "")

    # -------------------------------------------------------------
    # Step 3: Parse Markup to Detect the Export Trigger
    # -------------------------------------------------------------
    # Case A: <form action="...export..." method="post">
    export_form_match = re.search(r'<form[^>]+action=["\']([^"\']*export[^"\']*)["\']', html, re.IGNORECASE)
    # Case B: <a href="...export...">
    export_link_match = re.search(r'<a[^>]+href=["\']([^"\']*export[^"\']*)["\']', html, re.IGNORECASE)

    export_url = None
    is_post = True

    if export_form_match:
        action = export_form_match.group(1)
        export_url = action if action.startswith("http") else f"https://www.screener.in{action}"
        is_post = True
        logger.info(f"[AUTO-SYNC] Discovered dynamic export form action: {export_url}")
    elif export_link_match:
        link = export_link_match.group(1)
        export_url = link if link.startswith("http") else f"https://www.screener.in{link}"
        is_post = False
        logger.info(f"[AUTO-SYNC] Discovered dynamic export download link: {export_url}")
    else:
        # Fallback to standard API route if dynamic pattern not matched
        export_url = "https://www.screener.in/api/export/screen/3271948/"
        is_post = True
        logger.warning(f"[AUTO-SYNC] Markup scan did not find export tag. Falling back to default: {export_url}")

    export_headers = {
        "Referer": SCREENER_PAGE_URL,
        "Origin": "https://www.screener.in"
    }

    # -------------------------------------------------------------
    # Step 4: Execute Download
    # -------------------------------------------------------------
    if is_post:
        export_resp = session.post(
            export_url,
            data={"csrfmiddlewaretoken": page_csrf} if page_csrf else {},
            headers=export_headers,
            timeout=60
        )
    else:
        export_resp = session.get(export_url, headers=export_headers, timeout=60)

    # -------------------------------------------------------------
    # Step 5: Content Verification Guard
    # -------------------------------------------------------------
    # Check that response is valid CSV content and not HTML markup
    content_head = export_resp.content[:200].decode("utf-8", errors="ignore").strip().lower()
    if content_head.startswith("<!doctype html") or "<html" in content_head or "<body" in content_head:
        raise ValueError(
            "Screener returned HTML webpage instead of CSV. "
            "The detected export action requires elevated permissions or returned a redirect."
        )

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