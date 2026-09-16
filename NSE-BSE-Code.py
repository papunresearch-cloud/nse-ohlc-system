import firebase_admin
from firebase_admin import credentials, db
import numpy as np
import pandas as pd

# ==========================================
# CONFIGURATION
# ==========================================
FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"
FIREBASE_TARGET_NODE = "SCREENER"

def init_firebase():
    """Initializes the Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

def get_code(bse, nse):
    """Derives CODE preferring NSE over BSE."""
    if bse == "" and nse == "":
        return ""
    elif bse == "":
        return nse
    elif nse == "":
        return bse
    else:
        return nse

def update_screener_codes():
    print("Connecting to Firebase Realtime Database...")
    init_firebase()
    
    ref = db.reference(FIREBASE_TARGET_NODE)
    data = ref.get()

    if not data:
        print(f"❌ Error: No data found at Firebase node '/{FIREBASE_TARGET_NODE}'.")
        return

    print(f"Fetched records from Firebase. Loading into DataFrame...")
    # Read Firebase data directly into DataFrame and treat all fields as strings
    df = pd.DataFrame(data)

    # Ensure required columns exist
    if "BSE" not in df.columns:
        df["BSE"] = ""
    if "NSE" not in df.columns:
        df["NSE"] = ""

    # Fill NA/None with empty strings for consistent parsing
    bse_series = df["BSE"].fillna("").astype(str)
    nse_series = df["NSE"].fillna("").astype(str)

    # Generate the CODE column
    df["CODE"] = [
        str(get_code(bse.strip(), nse.strip()))
        for bse, nse in zip(bse_series, nse_series)
    ]

    # Sanitize invalid float/NaN values to None for clean JSON serialization
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    records = cleaned_df.to_dict(orient="records")

    print(f"Writing updated data with 'CODE' column back to Firebase...")
    ref.set(records)
    print(f"[OK] Success! Updated {len(records)} records in Firebase node '/{FIREBASE_TARGET_NODE}'.")

if __name__ == "__main__":
    try:
        update_screener_codes()
    except Exception as e:
        print(f"[ERROR] Execution failed: {e}")