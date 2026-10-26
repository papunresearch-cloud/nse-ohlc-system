"""
===============================================================================
SCREENER AUTO SYNC WORKER (CLOUD-SAFE PIPELINE TRIGGER)
===============================================================================
Bypasses direct scraping to eliminate [Errno 101] network unreachable errors.
Executes the ETL pipeline directly, updates health telemetry, and completes.
===============================================================================
"""

import os
from pathlib import Path
from datetime import datetime
import pytz

from firebase_admin import db
from config import TIMEZONE, logger

IST = pytz.timezone(TIMEZONE)

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


def execute_auto_screener_sync(database_date: str = None):
    """Executes the pipeline directly and updates health telemetry."""
    if not run_screener_pipeline:
        raise RuntimeError("Screener ETL pipeline runner is missing (RUN_PIPELINE.py / BASIC.py).")

    now_ist = datetime.now(IST)
    effective_date = database_date or now_ist.strftime("%Y-%m-%d")

    try:
        logger.info("[AUTO-SYNC] Initiating Screener ETL pipeline execution...")
        run_screener_pipeline()
        logger.info("[AUTO-SYNC] Pipeline stages finished successfully.")

        # Update health telemetry -> Last Screener DB
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