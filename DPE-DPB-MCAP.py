import firebase_admin
from firebase_admin import credentials, db
import numpy as np
import pandas as pd

# =====================================================================
# CONFIGURATION BLOCK
# =====================================================================
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

def safe_division(num, denom):
    """Calculates percentage deviation safely, returning NaN on division by zero."""
    if pd.isna(num) or pd.isna(denom) or denom == 0:
        return np.nan
    return 100 * (num - denom) / denom

def update_dpe_dpb_metrics():
    print("Connecting to Firebase Realtime Database...")
    init_firebase()

    ref = db.reference(FIREBASE_TARGET_NODE)
    data = ref.get()

    if not data:
        print(f"Error: No data found at Firebase node '/{FIREBASE_TARGET_NODE}'.")
        return

    print("Fetched records from Firebase. Loading into DataFrame...")
    df = pd.DataFrame(data)

    # Ensure numeric conversion (invalid entries become NaN)
    for col in ["PB", "3PB", "PE", "3PE", "mcap"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            print(f"Warning: Column '{col}' not found in database.")

    # Calculate % deviation of PE & PB with respect to their 3-year average
    if "PB" in df.columns and "3PB" in df.columns:
        df["DPB%"] = df.apply(lambda row: safe_division(row["PB"], row["3PB"]), axis=1).round(2)
    else:
        df["DPB%"] = np.nan

    if "PE" in df.columns and "3PE" in df.columns:
        df["DPE%"] = df.apply(lambda row: safe_division(row["PE"], row["3PE"]), axis=1).round(2)
    else:
        df["DPE%"] = np.nan

    # Calculate LGCAP (log10 of mcap)
    if "mcap" in df.columns:
        df["LGCAP"] = df["mcap"].apply(lambda x: np.log10(x) if pd.notna(x) and x > 0 else np.nan).round(2)
        # Calculate PCCAP (percentile rank of mcap)
        df["PCCAP"] = (df["mcap"].rank(pct=True) * 100).round(2)
    else:
        df["LGCAP"] = np.nan
        df["PCCAP"] = np.nan

    # Sanitize invalid float/NaN values to None so Firebase accepts JSON serialization
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    records = cleaned_df.to_dict(orient="records")

    print(f"Writing updated records back to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(records)
    print(f"Success! Columns 'DPB%', 'DPE%', 'LGCAP', and 'PCCAP' added to Firebase.")

if __name__ == "__main__":
    try:
        update_dpe_dpb_metrics()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")