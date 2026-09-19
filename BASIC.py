"""
===============================================================================
SCREENER BASE INGESTION (BASIC.py) - OPTION 2: SHARED FOLDER STREAMING
===============================================================================
* Monitors a public Google Drive folder dynamically for 'screener.csv'.
* Always pulls the freshest daily file without needing code edits or new file IDs.
* Explicitly extracts and formats 'roce-0' and 'roce-3y'.
* Formats YYYYMM / float inputs into clean "Month, Year" labels under "Last Qtr".
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
# 1. CONFIGURATION & CONSTANTS
# =====================================================================
# Paste your shared public SCREENER folder link here (set once, never change):
GDRIVE_FOLDER_LINK = "https://drive.google.com/drive/folders/1nPgfQC1mHmmVsND5uzoNQ_Wtt1_-JCt5?usp=drive_link"
TARGET_FILE_NAME = "screener.csv"

# Firebase Realtime Database Config
FIREBASE_TARGET_NODE = "SCREENER"
FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"
TIMEZONE = "Asia/Kolkata"
IST = pytz.timezone(TIMEZONE)

# Standard Column Mapping
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
    "Return on capital employed": "roce-0",
    "Average return on capital employed 3Years": "roce-3y",
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
def extract_id(link_or_id: str) -> str:
    """Extracts alphanumeric ID from Google Drive URL or raw ID."""
    link_or_id = link_or_id.strip()
    match = re.search(r'[-\w]{25,}', link_or_id)
    return match.group(0) if match else link_or_id

def download_file_by_id(file_id: str) -> pd.DataFrame:
    """Streams CSV directly into pandas memory from Google Drive ID."""
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    
    response = session.get(download_url, stream=True)
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            confirm_url = f"{download_url}&confirm={value}"
            response = session.get(confirm_url, stream=True)
            break

    if response.status_code != 200:
        raise RuntimeError(f"Failed downloading file ID '{file_id}' (HTTP {response.status_code}).")

    return pd.read_csv(io.BytesIO(response.content))

def download_latest_screener_csv() -> pd.DataFrame:
    """Finds 'screener.csv' dynamically inside the shared folder."""
    target_id = extract_id(GDRIVE_FOLDER_LINK)
    if not target_id or "PASTE_YOUR" in target_id:
        raise ValueError("Please paste your public Google Drive folder link in GDRIVE_FOLDER_LINK.")

    # Fallback check if user provided direct file URL
    if "file/d/" in GDRIVE_FOLDER_LINK:
        return download_file_by_id(target_id)

    # Inspect public folder HTML view to grab screener.csv file ID
    folder_url = f"https://drive.google.com/embeddedfolderview?id={target_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    res = requests.get(folder_url, headers=headers)
    
    if res.status_code != 200:
        return download_file_by_id(target_id)

    # Primary regex: extracts file ID associated with screener.csv in Drive folder view
    pattern = rf'data-id="([-\w]{{25,}})"[^>]*>.*?{re.escape(TARGET_FILE_NAME)}'
    matches = re.findall(pattern, res.text, re.IGNORECASE | re.DOTALL)
    
    if not matches:
        alt_pattern = rf'{re.escape(TARGET_FILE_NAME)}.*?href="[^"]*id=([-\w]{{25,}})"'
        matches = re.findall(alt_pattern, res.text, re.IGNORECASE | re.DOTALL)

    if matches:
        latest_file_id = matches[0]
        print(f"[INFO] Found '{TARGET_FILE_NAME}' in folder (File ID: {latest_file_id})")
        return download_file_by_id(latest_file_id)
    else:
        print("[WARN] Folder index parsing fallback: attempting direct download with target ID...")
        return download_file_by_id(target_id)

def init_firebase():
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
    if not key:
        return ""
    return re.sub(r'[.#$\[\]/]', '', str(key)).strip().upper()

def derive_primary_key(nse: str, bse: str, name: str) -> str:
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

def format_last_qtr(value):
    if pd.isna(value) or value is None:
        return None
    try:
        val_str = str(int(float(value))).strip()
    except (ValueError, TypeError):
        val_str = str(value).strip().split('.')[0]

    if len(val_str) == 6 and val_str.isdigit():
        year, month = val_str[:4], val_str[4:6]
        month_names = {
            "01": "Jan", "02": "Feb", "03": "March", "04": "April",
            "05": "May", "06": "June", "07": "July", "08": "Aug",
            "09": "Sept", "10": "Oct", "11": "Nov", "12": "Dec"
        }
        if month in month_names:
            return f"{month_names[month]}, {year}"
    return None

def clean_numeric_col(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace(["-", "nan", "None", ""], np.nan),
        errors="coerce"
    )

# =====================================================================
# 3. PIPELINE RUNNER
# =====================================================================
def run_pipeline():
    print(f"[INFO] Streaming daily screener data automatically from shared folder...")
    df = download_latest_screener_csv()
    print(f"[OK] Successfully loaded CSV ({len(df)} rows).")

    # Clean non-breaking spaces, BOM, and whitespace from CSV headers
    df.columns = [str(c).replace('\xa0', ' ').replace('\ufeff', '').strip() for c in df.columns]

    rename_dict = {}

    # 1. Alphanumeric normalized lookup for standard columns
    norm_csv_map = {re.sub(r'[^a-z0-9]', '', c.lower()): c for c in df.columns}
    for std_name, target_key in column_mapping.items():
        std_norm = re.sub(r'[^a-z0-9]', '', std_name.lower())
        if std_norm in norm_csv_map:
            actual_col = norm_csv_map[std_norm]
            rename_dict[actual_col] = target_key

    # 2. Collision-free matching for ROCE and Result Date
    for col in df.columns:
        norm = re.sub(r'[^a-z0-9]', '', col.lower())

        # Last Result Date
        if "result" in norm and "date" in norm:
            rename_dict[col] = "Last Qtr"
            continue

        # ROCE Detection (Checks 3-Year first to avoid collision)
        is_roce = ("capital" in norm and "employed" in norm) or "roce" in norm
        if is_roce:
            if any(term in norm for term in ["3year", "3years", "3yr", "3y"]):
                rename_dict[col] = "roce-3y"
            else:
                rename_dict[col] = "roce-0"

    extracted_df = df[list(rename_dict.keys())].rename(columns=rename_dict).copy()

    # Clean numeric fields for ROCE
    if "roce-0" in extracted_df.columns:
        extracted_df["roce-0"] = clean_numeric_col(extracted_df["roce-0"])
    if "roce-3y" in extracted_df.columns:
        extracted_df["roce-3y"] = clean_numeric_col(extracted_df["roce-3y"])

    # Format Last Qtr
    if "Last Qtr" in extracted_df.columns:
        extracted_df["Last Qtr"] = extracted_df["Last Qtr"].apply(format_last_qtr)
    else:
        extracted_df["Last Qtr"] = None

    # Derive primary key 'CODE'
    nse_col = extracted_df["NSE"] if "NSE" in extracted_df.columns else [""] * len(extracted_df)
    bse_col = extracted_df["BSE"] if "BSE" in extracted_df.columns else [""] * len(extracted_df)
    name_col = extracted_df["Name"] if "Name" in extracted_df.columns else [""] * len(extracted_df)

    extracted_df["CODE"] = [
        derive_primary_key(n, b, nm)
        for n, b, nm in zip(nse_col, bse_col, name_col)
    ]

    # Remove empty or duplicate keys
    extracted_df = extracted_df[extracted_df["CODE"] != ""].copy()
    extracted_df = extracted_df.drop_duplicates(subset=["CODE"], keep="first")

    # Sanitize infinities and NaNs for JSON serialization
    cleaned_df = extracted_df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    # Convert to keyed dictionary
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