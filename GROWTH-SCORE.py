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

SG_REFERENCE = 0.25  # Threshold factor for closeness in sg-c

def init_firebase():
    """Initializes the Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

# =====================================================================
# PART 1: PG-C CALCULATION LOGIC
# =====================================================================
def calculate_pg_c(row):
    pg1, pg3 = row["pg-1"], row["pg-3"]
    v1, v3 = pd.notna(pg1), pd.notna(pg3)
    vx = abs(pg1 - pg3) > 100

    if v1 and not v3:
        return pg1
    if not v1 and v3:
        return 0.0
    if not v1 and not v3:
        return 0.0
    if v1 and v3 and vx:
        return min(pg1, pg3)
    if v1 and v3 and not vx:
        return (0.60 * min(pg1, pg3)) + (0.40 * max(pg1, pg3))
    return 0.0

# =====================================================================
# PART 2: SG-C CALCULATION LOGIC
# =====================================================================
def calculate_sg_c(row):
    eq, ttm, sg3y = row["sg-eq"], row["sg-ttm"], row["sg-3y"]
    v_eq, v_ttm, v_3y = pd.notna(eq), pd.notna(ttm), pd.notna(sg3y)

    if not v_eq and not v_ttm and not v_3y:
        return 0.0
    if v_ttm and not v_eq and not v_3y:
        return ttm
    if v_3y and not v_eq and not v_ttm:
        return sg3y
    if v_eq and not v_ttm and not v_3y:
        return eq

    if not v_3y and v_eq and v_ttm:
        if abs(ttm - eq) < 0.2 * abs(ttm):
            return (0.5 * eq) + (0.5 * ttm)
        return ttm
    if not v_eq and v_ttm and v_3y:
        return (0.75 * ttm) + (0.25 * sg3y)
    if not v_ttm and v_eq and v_3y:
        return 0.0

    if v_eq and v_ttm and v_3y:
        x1, x2, x3 = eq, ttm, sg3y
        x12, x23, x13 = abs(x1 - x2), abs(x2 - x3), abs(x1 - x3)
        k = min(np.median([x1, x2, x3]) * SG_REFERENCE, 50)

        if (x12 < k) and (x23 < k) and (x13 < k):
            return (0.2 * x1) + (0.5 * x2) + (0.3 * x3)

        pairs = {"12": x12, "23": x23, "13": x13}
        closest = min(pairs, key=pairs.get)

        if closest == "12":
            return (0.6 * x2) + (0.4 * x1)
        elif closest == "23":
            return (0.6 * x2) + (0.4 * x3)
        elif closest == "13":
            return (0.8 * x3) + (0.2 * x1)

        return np.median([x1, x2, x3])

    return 0.0

# =====================================================================
# PART 3: GROWTH SCORE (G-SCORE) M & N BUCKETS
# =====================================================================
def pg_score(x):
    if pd.isna(x):
        return np.nan
    if x < -50:
        return 0
    elif x < -25:
        return 1
    elif x < -10:
        return 2
    elif x < 0:
        return 3
    elif x < 10:
        return 4
    elif x < 20:
        return 5
    elif x < 30:
        return 6
    elif x < 40:
        return 7
    elif x < 50:
        return 8
    else:
        return 9

def sg_score(x):
    if pd.isna(x):
        return np.nan
    if x < -10:
        return 0
    elif x < 0:
        return 1
    elif x < 10:
        return 2
    elif x < 15:
        return 3
    elif x < 20:
        return 4
    elif x < 25:
        return 5
    elif x < 30:
        return 6
    elif x < 35:
        return 7
    elif x < 40:
        return 8
    else:
        return 9

# =====================================================================
# MAIN PIPELINE
# =====================================================================
def update_growth_scores():
    print("Connecting to Firebase Realtime Database...")
    init_firebase()

    ref = db.reference(FIREBASE_TARGET_NODE)
    data = ref.get()

    if not data:
        print(f"Error: No data found at Firebase node '/{FIREBASE_TARGET_NODE}'.")
        return

    print("Fetched records from Firebase. Loading into DataFrame...")
    df = pd.DataFrame(data)

    # 1. Clean input numeric columns
    for col in ["pg-1", "pg-3", "sg-eq", "sg-ttm", "sg-3y"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    # 2. Compute intermediate metrics
    df["_pg_c"] = df.apply(calculate_pg_c, axis=1).round(2)
    df["_sg_c"] = df.apply(calculate_sg_c, axis=1).round(2)
    df["_m"] = df["_sg_c"].apply(sg_score)
    df["_n"] = df["_pg_c"].apply(pg_score)

    # 3. Compute G-score
    df["G-score"] = np.nan
    valid = df["_m"].notna() & df["_n"].notna()

    temp = df.loc[valid].copy()
    temp = temp.sort_values(
        by=["_m", "_n", "_sg_c"],
        ascending=[True, True, False]
    )

    temp["_position"] = temp.groupby(["_m", "_n"]).cumcount()
    temp["_group_size"] = temp.groupby(["_m", "_n"])["_m"].transform("size")
    temp["_p"] = (temp["_position"] + 1) / (temp["_group_size"] + 1)
    temp["G-score"] = (10 * temp["_m"] + temp["_n"] + temp["_p"]).round(2)

    df.loc[temp.index, "G-score"] = temp["G-score"]

    # 4. Strictly drop all intermediate and temporary columns
    df.drop(
        columns=["_pg_c", "_sg_c", "_m", "_n", "pg-c", "sg-c"],
        inplace=True,
        errors="ignore"
    )
    print(f"G-score calculated for {len(temp)} stocks.")

    # 5. Sanitize and upload to Firebase
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    records = cleaned_df.to_dict(orient="records")

    print(f"Writing records with only 'G-score' added back to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(records)
    print(f"[OK] Success! Single column 'G-score' updated in Firebase.")

if __name__ == "__main__":
    try:
        update_growth_scores()
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")