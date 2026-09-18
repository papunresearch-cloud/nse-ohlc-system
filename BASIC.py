"""
===============================================================================
SCREENER BASE INGESTION (BASIC.py) - DIRECT PUBLIC LINK STREAMING
===============================================================================
* Downloads screener.csv directly from Google Drive using a public share link.
* Completely eliminates OAuth tokens, browser logins, and googleapiclient.
* Derives primary key 'CODE' (NSE > BSE > Name fallback).
* Cleans, sanitizes, and writes directly to Firebase Realtime Database at /SCREENER.
* Updates /system_status/screener_sync with success timestamp.
===============================================================================
"""

import io
import os
import re
import json
from datetime import datetime
import pytz
import requests
import numpy as np
import pandas as pd

import firebase_admin
from firebase_admin import credentials, db

# =====================================================================
# 1. PASTE YOUR GOOGLE DRIVE LINK HERE
# =====================================================================
GDRIVE_SHARE_LINK = "https://drive.google.com/file/d/1hveFXGaHo-eMlQxDcYhanVAQFzHgaXtQ/view?usp=sharing"

# Firebase Realtime Database Config
FIREBASE_TARGET_NODE = "SCREENER"
FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"
TIMEZONE = "Asia/Kolkata"
IST = pytz.timezone(TIMEZONE)

# Column mapping from screener.csv to database keys
column_mapping = {
    "Name": "Name",
    "BSE Code": "BSE", 
    "NSE Code": "NSE",
    "Industry Group": "sector",
    "Industry": "industry",
    "Current Price": "cmp",
    "Market Capitalization": "mcap",
    "Price to book value": "PB",
    "Historical PBV 3Years": "3PB",
    "Price to Earning": "PE",
    "Historical PE 3Years": "3PE",
    "Price to Sales": "PS",
    "Return on equity": "roe-0",
    "Return on equity preceding year": "roe-1",
    "Average return on equity 3Years": "roe-3y",
    "Expected quarterly sales growth": "sg-eq",
    "Sales growth": "sg-ttm",
    "Sales growth 3Years": "sg-3y",
    "Profit growth": "pg-1",
    "Profit growth 3Years": "pg-3",
    "Average dividend payout 3years": "advdp",
    "Return on assets": "roa-0",
    "Return on assets preceding year": "roa-1",
    "Return on assets 3years": "roa-3y",
    "Debt to equity": "DE",
    "Dividend yield": "DY",
    "AVGOPM": "OPM",
    "AVERAGE OCF EBIT PCT": "OCF%",
    "AVERAGE FCF EBIT PCT": "FCF%",
    "CWIPTOGROSSBLOCK": "CWIP",
    "DILUTION": "Dlutn",
    "BVGR": "BVgr",
    "RSI": "RSI",
    "DMA 50": "50ma",
    "DMA 200": "200ma",
    "High price": "52wh",
    "Low price": "52wl",
    "Return over 1week": "1wr",
    "Return over 1month": "1mr",
    "Return over 3months": "3mr",
    "Return over 6months": "6mr",
    "Return over 1year": "1yr",
    "Return over 3years": "3yr",
    "FII holding": "FII",
    "Change in FII holding": "DFII",
    "DII holding": "DII",
    "Change in DII holding": "DDII",
    "Promoter holding": "PRH",
    "Change in promoter holding": "DPRH",
    "YOY Quarterly sales growth": "YSG",
    "YOY Quarterly profit growth": "YPG"
}

# =====================================================================
# 2. HELPER FUNCTIONS
# =====================================================================
def extract_file_id(link_or_id: str) -> str:
    """Extracts the 33-character Google Drive ID from any shared URL."""
    link_or_id = link_or_id.strip()
    match = re.search(r'[-\w]{25,}', link_or_id)
    if match:
        return match.group(0)
    return link_or_id

def download_csv_from_drive(file_id: str) -> pd.DataFrame:
    """Streams the CSV into memory using standard HTTP without authentication."""
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    
    response = session.get(download_url, stream=True)
    
    # Handle Google Drive large-file virus scan confirmation if prompted
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            confirm_url = f"{download_url}&confirm={value}"
            response = session.get(confirm_url, stream=True)
            break

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to download file from Google Drive (HTTP {response.status_code}). "
            "Please ensure the file sharing setting is set to 'Anyone with the link can view'."
        )

    return pd.read_csv(io.BytesIO(response.content))

def init_firebase():
    """Initializes Firebase Admin SDK using disk file or Render Environment Variable."""
    if not firebase_admin._apps:
        if os.path.exists("serviceAccountKey.json"):
            cred = credentials.Certificate("serviceAccountKey.json")
        elif os.environ.get("FIREBASE_SERVICE_ACCOUNT_KEY"):
            key_dict = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_KEY"])
            cred = credentials.Certificate(key_dict)
        elif os.environ.get("FIREBASE_CREDENTIALS"):
            key_dict = json.loads(os.environ["FIREBASE_CREDENTIALS"])
            cred = credentials.Certificate(key_dict)
        else:
            raise FileNotFoundError("Firebase credentials not found (serviceAccountKey.json)")

        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

def sanitize_firebase_key(key: str) -> str:
    """Strips forbidden Firebase Realtime Database characters."""
    if not key:
        return ""
    return re.sub(r'[.#$\[\]/]', '', str(key)).strip().upper()

def derive_primary_key(bse: str, nse: str, name: str) -> str:
    """Derives primary key: 1. NSE Code, 2. BSE Code, 3. Name."""
    nse_clean = str(nse).strip() if pd.notna(nse) else ""
    bse_clean = str(bse).strip() if pd.notna(bse) else ""
    name_clean = str(name).strip() if pd.notna(name) else ""

    if nse_clean and nse_clean.lower() != "nan":
        chosen = nse_clean
    elif bse_clean and bse_clean.lower() != "nan":
        chosen = bse_clean
    else:
        chosen = name_clean

    return sanitize_firebase_key(chosen)

# =====================================================================
# 3. PIPELINE RUNNER
# =====================================================================
def run_pipeline():
    file_id = extract_file_id(GDRIVE_SHARE_LINK)
    if not file_id or "PASTE_YOUR" in file_id:
        raise ValueError("Please paste your valid Google Drive link in GDRIVE_SHARE_LINK.")

    print(f"[INFO] Downloading screener.csv directly via Google Drive link (ID: {file_id})...")
    df = download_csv_from_drive(file_id)
    print(f"[OK] Successfully loaded CSV ({len(df)} rows).")

    # Column filtering & renaming
    valid_cols = [c for c in column_mapping.keys() if c in df.columns]
    extracted_df = df[valid_cols].rename(columns=column_mapping)

    # Resolve primary key CODE column
    bse_col = extracted_df["BSE"] if "BSE" in extracted_df.columns else [""] * len(extracted_df)
    nse_col = extracted_df["NSE"] if "NSE" in extracted_df.columns else [""] * len(extracted_df)
    name_col = extracted_df["Name"] if "Name" in extracted_df.columns else [""] * len(extracted_df)

    extracted_df["CODE"] = [
        derive_primary_key(b, n, nm)
        for b, n, nm in zip(bse_col, nse_col, name_col)
    ]

    # Remove empty or duplicate keys
    extracted_df = extracted_df[extracted_df["CODE"] != ""].copy()
    extracted_df = extracted_df.drop_duplicates(subset=["CODE"], keep="first")

    # Sanitize infinities and NaNs for JSON serialization
    cleaned_df = extracted_df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    # Convert DataFrame to a keyed dictionary: { "CODE": { ...record... } }
    keyed_records = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")

    print("[INFO] Uploading keyed dictionary to Firebase Realtime Database...")
    init_firebase()
    ref = db.reference(FIREBASE_TARGET_NODE)
    ref.set(keyed_records)

    # Write status telemetry timestamp
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    db.reference("system_status/screener_sync").set({
        "last_updated": now_ist,
        "record_count": len(keyed_records),
        "status": "SUCCESS"
    })
    
    print(f"[OK] Success! Uploaded {len(keyed_records)} keyed records to Firebase node: /{FIREBASE_TARGET_NODE}")

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print(f"[ERROR] Execution failed: {e}")
        raise