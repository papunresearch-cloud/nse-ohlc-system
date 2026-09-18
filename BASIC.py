"""
===============================================================================
SCREENER BASE INGESTION (BASIC.py)
===============================================================================
* Downloads screener.csv from Google Drive using headless OAuth token authentication.
* Eliminates browser pop-ups (InstalledAppFlow) to run reliably on Render.
* Cleans and sanitizes columns, deriving the primary key 'CODE'.
* Overwrites and saves data as a keyed dictionary directly under /SCREENER/<CODE>.
* Publishes update telemetry to /system_status/screener_sync.
===============================================================================
"""

import io
import os
import re
import json
import logging
from datetime import datetime
import pytz
import numpy as np
import pandas as pd

import firebase_admin
from firebase_admin import credentials, db
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger("NSE_OHLC_SYSTEM")

# ==========================================
# 1. CONFIGURATION & CONSTANTS
# ==========================================
SCOPES = ['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/drive']

GDRIVE_FOLDER_NAME = "SCREENER"
GDRIVE_FILE_NAME = "screener.csv"

FIREBASE_TARGET_NODE = "SCREENER"
FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"
TIMEZONE = "Asia/Kolkata"
IST = pytz.timezone(TIMEZONE)

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

# ==========================================
# 2. HEADLESS GOOGLE DRIVE CLIENT
# ==========================================
def get_drive_service():
    """
    Authenticates and returns Google Drive API service.
    Reads token from local token.json or Render's GDRIVE_TOKEN_JSON secret env var.
    Never attempts to launch a local browser popup.
    """
    creds = None

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    elif os.environ.get('GDRIVE_TOKEN_JSON'):
        try:
            token_info = json.loads(os.environ['GDRIVE_TOKEN_JSON'])
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        except Exception as err:
            logger.error(f"[GDRIVE] Error parsing GDRIVE_TOKEN_JSON environment variable: {err}")

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            if os.path.exists('token.json'):
                with open('token.json', 'w') as token_file:
                    token_file.write(creds.to_json())
        except Exception as e:
            logger.error(f"[GDRIVE] Token refresh error: {e}")
            creds = None

    if not creds or not creds.valid:
        raise RuntimeError(
            "[GDRIVE ERROR] Missing or invalid 'token.json'. "
            "Ensure token.json exists in Render Secret Files or GDRIVE_TOKEN_JSON is configured."
        )

    return build('drive', 'v3', credentials=creds)

def get_folder_id(service, folder_name: str):
    """Searches Drive for the parent folder."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    folders = results.get('files', [])
    if folders:
        return folders[0]['id']
    return None

def get_file_id(service, folder_id: str, file_name: str):
    """Finds target file within specified folder."""
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

# ==========================================
# 3. FIREBASE SETUP & PRIMARY KEY HELPERS
# ==========================================
def init_firebase():
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_KEY_FILE):
            cred = credentials.Certificate(FIREBASE_KEY_FILE)
        elif os.environ.get("FIREBASE_SERVICE_ACCOUNT_KEY"):
            key_dict = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_KEY"])
            cred = credentials.Certificate(key_dict)
        else:
            raise FileNotFoundError(f"Firebase credentials not found ({FIREBASE_KEY_FILE}).")

        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

def sanitize_firebase_key(key: str) -> str:
    """Strips invalid Firebase RTDB characters (. # $ / [ ]) and trims."""
    if not key:
        return ""
    return re.sub(r'[.#$\[\]/]', '', str(key)).strip().upper()

def derive_primary_key(bse: str, nse: str, name: str) -> str:
    """
    Derives primary key:
    1. NSE Code
    2. BSE Code (if NSE missing)
    3. Name (fallback)
    """
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

# ==========================================
# 4. MAIN PIPELINE EXECUTION
# ==========================================
def run_pipeline():
    print("[INFO] Connecting to Google Drive...")
    service = get_drive_service()
    
    print(f"[INFO] Locating folder '{GDRIVE_FOLDER_NAME}'...")
    folder_id = get_folder_id(service, GDRIVE_FOLDER_NAME)
    if not folder_id:
        raise FileNotFoundError(f"Folder '{GDRIVE_FOLDER_NAME}' not found in Google Drive.")

    print(f"[INFO] Locating file '{GDRIVE_FILE_NAME}'...")
    file_id = get_file_id(service, folder_id, GDRIVE_FILE_NAME)
    if not file_id:
        raise FileNotFoundError(f"File '{GDRIVE_FILE_NAME}' not found inside folder '{GDRIVE_FOLDER_NAME}'.")

    print(f"[INFO] Streaming '{GDRIVE_FILE_NAME}' into memory...")
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    df = pd.read_csv(fh)
    print(f"[OK] Successfully loaded CSV ({len(df)} rows).")

    # Filter columns to only those defined in the mapping
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

    # Remove invalid or duplicate codes
    extracted_df = extracted_df[extracted_df["CODE"] != ""].copy()
    extracted_df = extracted_df.drop_duplicates(subset=["CODE"], keep="first")

    # Sanitize infinities and NaNs for JSON serialization
    cleaned_df = extracted_df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    # Convert DataFrame to a keyed dictionary: { "CODE": { ...fields... } }
    keyed_records = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")

    print("[INFO] Uploading keyed dictionary to Firebase Realtime Database...")
    init_firebase()
    ref = db.reference(FIREBASE_TARGET_NODE)
    ref.set(keyed_records)

    # Update real-time sync telemetry
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