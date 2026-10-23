"""
SCREENER AUTOMATED DOWNLOAD & ETL SYNC WORKER
Automates:
1. Direct CSV export download from screener.in via Playwright.
2. Screener ETL & scoring pipeline execution.
3. Telemetry update to /system_status/screener_sync ("Last Screener DB").
4. Deletion of the temporary screener.csv file from local storage.
"""

import os
import sys
import shutil
from pathlib import Path
from datetime import datetime
import pytz

try:
    from playwright.sync_api import sync_playwright
except ImportError as err:
    sync_playwright = None

from firebase_admin import db
from config import TIMEZONE, logger

IST = pytz.timezone(TIMEZONE)

SCREENER_URL = "https://www.screener.in/screens/3271948/screener/"
PROFILE_DIR = Path.home() / "screener_playwright_profile"
TEMP_DOWNLOAD_DIR = Path.cwd() / "temp_screener_download"
OUTPUT_CSV_FILE = TEMP_DOWNLOAD_DIR / "screener.csv"

# Pipeline ETL runner import
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
    if not sync_playwright:
        raise ImportError("Playwright is not installed. Run: pip install playwright && playwright install chromium")

    TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if OUTPUT_CSV_FILE.exists():
        OUTPUT_CSV_FILE.unlink()

    logger.info("[AUTO-SYNC] Launching Playwright browser...")
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=True,
            accept_downloads=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(60_000)

        logger.info(f"[AUTO-SYNC] Navigating to {SCREENER_URL}...")
        page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60_000)

        export_button = page.locator('form[action*="/api/export/screen/"] button[type="submit"]')
        export_button.wait_for(state="visible", timeout=30_000)

        logger.info("[AUTO-SYNC] Triggering CSV export download...")
        with page.expect_download(timeout=60_000) as download_info:
            export_button.click()

        download = download_info.value
        download.save_as(str(OUTPUT_CSV_FILE))
        context.close()

    logger.info(f"[AUTO-SYNC] Download completed: {OUTPUT_CSV_FILE}")
    return OUTPUT_CSV_FILE


def execute_auto_screener_sync(database_date: str = None):
    if not run_screener_pipeline:
        raise RuntimeError("Screener ETL pipeline runner is missing (RUN_PIPELINE.py or BASIC.py).")

    now_ist = datetime.now(IST)
    effective_date = database_date or now_ist.strftime("%Y-%m-%d")

    downloaded_file = None
    root_screener = Path.cwd() / "screener.csv"
    try:
        downloaded_file = download_screener_csv()
        if not downloaded_file.exists() or downloaded_file.stat().st_size == 0:
            raise FileNotFoundError("Downloaded screener.csv is empty or missing.")

        shutil.copyfile(downloaded_file, root_screener)

        logger.info("[AUTO-SYNC] Executing Screener ETL pipeline...")
        run_screener_pipeline()
        logger.info("[AUTO-SYNC] Pipeline stages finished successfully.")

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
        logger.info("[AUTO-SYNC] Cleaning up local storage...")
        try:
            if downloaded_file and downloaded_file.exists():
                downloaded_file.unlink()
            if root_screener.exists():
                root_screener.unlink()
            if TEMP_DOWNLOAD_DIR.exists():
                shutil.rmtree(TEMP_DOWNLOAD_DIR, ignore_errors=True)
            logger.info("[AUTO-SYNC] Temporary files removed.")
        except Exception as clean_err:
            logger.warning(f"[AUTO-SYNC] Cleanup warning: {clean_err}")