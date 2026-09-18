import io
import os
import re
import firebase_admin
from firebase_admin import credentials, db
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import numpy as np
import pandas as pd

# ==========================================
# 1. CONFIGURATION
# ==========================================
SCOPES = ['https://www.googleapis.com/auth/drive']

GDRIVE_FOLDER_NAME = "SCREENER"
GDRIVE_FILE_NAME = "screener.csv"

FIREBASE_TARGET_NODE = "SCREENER"
FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"

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
# 2. GOOGLE DRIVE HELPERS
# ==========================================
def get_drive_service():
    """Authenticates and returns the Google Drive API service."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)

def get_folder_id(service, folder_name):
    """Searches Drive for the folder; prints accessible folders if missing."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    folders = results.get('files', [])
    
    if folders:
        return folders[0]['id']
    
    print(f"\n[INFO] Could not find folder '{folder_name}'. Listing accessible folders:")
    all_folders = service.files().list(
        q="mimeType='application/vnd.google-apps.folder' and trashed=false",
        spaces='drive',
        pageSize=30,
        fields='files(id, name)'
    ).execute().get('files', [])
    
    for f in all_folders:
        print(f"  [Folder] {f['name']} (ID: {f['id']})")
    print()
    return None

def get_file_id(service, folder_id, file_name):
    """Locates a file inside the identified parent folder."""
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

# ==========================================
# 3. FIREBASE SETUP & PRIMARY KEY DERIVATION
# ==========================================
def init_firebase():
    """Initializes Firebase Admin SDK for Realtime Database."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

def sanitize_firebase_key(key: str) -> str:
    """
    Strips forbidden Firebase Realtime Database path characters:
    . $ # [ ] / and control characters, trimmed and uppercase.
    """
    if not key:
        return ""
    cleaned = re.sub(r'[.#$\[\]/]', '', str(key)).strip().upper()
    return cleaned

def derive_primary_key(bse: str, nse: str, name: str) -> str:
    """
    Selects primary identifier:
    1. NSE Code (clean alphanumeric symbol e.g., DIXON, RELIANCE)
    2. BSE Code (if NSE is missing)
    3. Name (fallback if both missing)
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
# 4. EXECUTION PIPELINE
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

    # Column filtering & renaming (only matching configured columns)
    valid_cols = [c for c in column_mapping.keys() if c in df.columns]
    extracted_df = df[valid_cols].rename(columns=column_mapping)

    # 1. Derive clean Primary Key column 'CODE'
    bse_col = extracted_df["BSE"] if "BSE" in extracted_df.columns else ""
    nse_col = extracted_df["NSE"] if "NSE" in extracted_df.columns else ""
    name_col = extracted_df["Name"] if "Name" in extracted_df.columns else ""

    extracted_df["CODE"] = [
        derive_primary_key(b, n, nm)
        for b, n, nm in zip(bse_col, nse_col, name_col)
    ]

    # Remove rows where no valid primary key could be resolved
    extracted_df = extracted_df[extracted_df["CODE"] != ""].copy()

    # Drop duplicate primary keys if any exist in the CSV (keeps first occurrence)
    extracted_df = extracted_df.drop_duplicates(subset=["CODE"], keep="first")

    # Sanitize invalid float/NaN/inf values for clean JSON serialization
    cleaned_df = extracted_df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    # 2. Convert DataFrame to a keyed dictionary: { "CODE": { ...stock record... } }
    keyed_records = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")

    print("[INFO] Uploading keyed dictionary to Firebase Realtime Database...")
    init_firebase()
    ref = db.reference(FIREBASE_TARGET_NODE)
    ref.set(keyed_records)
    
    print(f"[OK] Success! Uploaded {len(keyed_records)} keyed records to Firebase node: /{FIREBASE_TARGET_NODE}")

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print(f"[ERROR] Execution failed: {e}")