import os
import subprocess
import sys
import re
import json
from datetime import datetime
import pytz

import firebase_admin
from firebase_admin import credentials, db

# =====================================================================
# CONFIGURATION BLOCK
# =====================================================================
# Pipeline sequence: Ingestion & PK derivation -> Metric derivations -> Scoring
scripts = [
    "BASIC.py",                # 1. Copies screener.csv, sets primary key CODE, saves to Firebase
    "DPE-DPB-MCAP.py",         # 2. Computes DPB%, DPE%, LGCAP, PCCAP
    "GROWTH-SCORE.py",         # 3. Computes and attaches G-score
    "FUNDAMENTAL-SCORE.py",    # 4. Computes and attaches F-score
    "TECHNICAL-SCORE.py"       # 5. Computes and attaches T-score
]

FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"
TIMEZONE = "Asia/Kolkata"
IST = pytz.timezone(TIMEZONE)

# Metrics to sync from /SCREENER into /watchlist/detailedDb
SCREENER_SYNC_METRICS = [
    "CODE", "DPB%", "DPE%", "DY", "F-score", "G-score", "PB", "PCCAP", 
    "PE", "PS", "T-score", "YPG", "YSG", "industry", "pg-1", "sector", "sg-ttm",
    "mcap", "roe-0", "roe-3y", "roa-0", "roa-3y", "roce-0", "roce-3y", 
    "sg-3y", "pg-3", "DE", "BVgr", "advdp", "FII", "DFII", "DII", 
    "DDII", "PRH", "DPRH", "Last Qtr"
]
# =====================================================================

def init_firebase():
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_KEY_FILE):
            cred = credentials.Certificate(FIREBASE_KEY_FILE)
        elif os.environ.get("FIREBASE_SERVICE_ACCOUNT_KEY"):
            cred = credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_KEY"]))
        elif os.environ.get("FIREBASE_CREDENTIALS"):
            val = os.environ["FIREBASE_CREDENTIALS"].strip()
            cred = credentials.Certificate(val if os.path.exists(val) else json.loads(val))
        else:
            raise FileNotFoundError(f"Firebase credentials not found ({FIREBASE_KEY_FILE})")

        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL.rstrip('/')
        })

def sanitize_firebase_key(key: str) -> str:
    """Sanitizes stock key strings for Firebase RTDB path compatibility."""
    if not key:
        return ""
    return re.sub(r'[.#$\[\]/]', '', str(key)).strip().upper()

def sync_detailed_db():
    """
    Executes AFTER all pipeline scripts have completed.
    Fetches completed /SCREENER data and merges all fresh screener metrics
    into /watchlist/detailedDb for all active watchlist stocks while
    preserving manual user tags (GROUP, REVIEW, DURATION, REMARK, DATE, TICKER).
    """
    print(f"\n{'='*60}")
    print("[SYNC] Propagating completed /SCREENER data to /watchlist/detailedDb...")
    print(f"{'='*60}")

    init_firebase()

    wl_ref = db.reference("watchlist")
    wl_data = wl_ref.get() or {}

    screener_ref = db.reference("SCREENER")
    screener_data = screener_ref.get() or {}

    active_stocks = wl_data.get("watchlist", [])
    current_detailed_db = wl_data.get("detailedDb", {})

    if not active_stocks:
        print("[WARN] No active stocks detected in /watchlist/watchlist. Skipping sync.")
        return

    # Normalize screener records to key-lookup dictionary if stored as a list
    screener_lookup = {}
    if isinstance(screener_data, dict):
        screener_lookup = screener_data
    elif isinstance(screener_data, list):
        for rec in screener_data:
            if isinstance(rec, dict) and rec.get("CODE"):
                screener_lookup[sanitize_firebase_key(rec["CODE"])] = rec

    updates = {}
    for item in active_stocks:
        clean_key = sanitize_firebase_key(item)
        if not clean_key:
            continue

        screener_stock = screener_lookup.get(clean_key, {})
        existing_stock = current_detailed_db.get(clean_key, {})

        # Extract only matching screener metrics that are present
        extracted_metrics = {
            k: screener_stock[k]
            for k in SCREENER_SYNC_METRICS
            if k in screener_stock and screener_stock[k] is not None
        }

        # Merge with existing curation tags to prevent data loss
        merged_stock = {
            **existing_stock,
            **extracted_metrics,
            "GROUP": existing_stock.get("GROUP", "GROUP-0"),
            "REVIEW": existing_stock.get("REVIEW", "NR"),
            "DURATION": existing_stock.get("DURATION", "NR"),
            "REMARK": existing_stock.get("REMARK", ""),
            "DATE": existing_stock.get("DATE", datetime.now(IST).strftime("%d-%m-%Y")),
            "TICKER": existing_stock.get("TICKER", f"{clean_key}.NS")
        }

        updates[f"watchlist/detailedDb/{clean_key}"] = merged_stock

    if updates:
        db.reference().update(updates)
        print(f"[OK] Successfully updated /watchlist/detailedDb for {len(updates)} watchlist stocks.")
    else:
        print("[INFO] No watchlist stocks were updated.")

def run_script(script_name):
    print(f"\n{'='*60}")
    print(f">>> Executing: {script_name}")
    print(f"{'='*60}")

    if not os.path.exists(script_name):
        print(f"[ERROR] Script file '{script_name}' not found in current directory.")
        sys.exit(1)

    # Force child processes to execute with UTF-8 encoding to prevent Windows cp1252 errors
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    process = subprocess.Popen(
        [sys.executable, script_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
        errors="replace",
        env=env
    )

    for line in iter(process.stdout.readline, ''):
        print(line, end="")

    process.stdout.close()
    return_code = process.wait()

    if return_code != 0:
        print(f"\n[ERROR] Execution halted! Error occurred inside '{script_name}' (Exit Code: {return_code})")
        sys.exit(return_code)

    print(f"[OK] Completed successfully: {script_name}")

def main():
    print("Starting Screener ETL & Scoring Master Pipeline...")
    total_steps = len(scripts)

    # 1. Execute all 5 pipeline scripts sequentially
    for idx, script in enumerate(scripts, start=1):
        print(f"\n[Step {idx}/{total_steps}]")
        run_script(script)

    print(f"\n{'='*60}")
    print(f"[OK] All {total_steps} stages completed successfully! Firebase /SCREENER updated.")
    print(f"{'='*60}")

    # 2. Call sync_detailed_db only after the entire screener database update finishes
    try:
        sync_detailed_db()
    except Exception as e:
        print(f"[ERROR] Failed to execute sync_detailed_db: {e}")

if __name__ == "__main__":
    main()