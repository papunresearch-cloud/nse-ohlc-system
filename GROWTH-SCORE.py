import os
import json
import math
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
    """Initializes Firebase Admin SDK from local file or Render Environment Variable."""
    if not firebase_admin._apps:
        # Check if credential JSON is provided via Render Environment Variable
        env_cred = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
        if env_cred:
            print("[INFO] Authenticating Firebase using environment variable...")
            cred_dict = json.loads(env_cred)
            cred = credentials.Certificate(cred_dict)
        elif os.path.exists(FIREBASE_KEY_FILE):
            print(f"[INFO] Authenticating Firebase using local file: {FIREBASE_KEY_FILE}...")
            cred = credentials.Certificate(FIREBASE_KEY_FILE)
        else:
            raise FileNotFoundError(
                f"[ERROR] Service account credentials not found! Neither '{FIREBASE_KEY_FILE}' "
                "nor 'FIREBASE_SERVICE_ACCOUNT_JSON' environment variable exists."
            )
            
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })


def update_growth_scores():
    print("[INFO] Connecting to Firebase Realtime Database...")
    init_firebase()

    ref = db.reference(FIREBASE_TARGET_NODE)
    data = ref.get()

    if not data:
        print(f"[ERROR] No data found at Firebase node '/{FIREBASE_TARGET_NODE}'.")
        return

    print(f"[INFO] Loaded raw records from Firebase. Total items: {len(data)}")
    
    # Preserve original keys (e.g. Stock tickers if data is a dict)
    is_dict = isinstance(data, dict)
    if is_dict:
        df = pd.DataFrame.from_dict(data, orient="index")
        df["_firebase_key"] = df.index  # Keep exact database node key
    else:
        df = pd.DataFrame(data)
        df["_firebase_key"] = df.index

    # Diagnostic check for required columns
    required_cols = ["pg-1", "pg-3", "sg-eq", "sg-ttm", "sg-3y"]
    print(f"[INFO] Available columns in database: {list(df.columns)}")
    for col in required_cols:
        if col not in df.columns:
            print(f"[WARNING] Column '{col}' missing from Firebase data; filling with NaN.")
            df[col] = np.nan

    # 1. Validate metric ranges [-100, 9999]
    for col in required_cols:
        res = [validate_metric(v) for v in df[col]]
        df[f"_{col}_valid"] = [r[0] for r in res]
        df[f"_{col}_val"] = [r[1] for r in res]

    # 2. Check G1, G2, and sg-eq eligibility
    df["_G1_eligible"] = df["_sg-ttm_valid"] & df["_pg-1_valid"]
    df["_G2_eligible"] = df["_sg-3y_valid"] & df["_pg-3_valid"]
    df["_sgeq_eligible"] = df["_sg-eq_valid"]

    g1_count = df["_G1_eligible"].sum()
    print(f"[INFO] Rows eligible for scoring (G1 passed): {g1_count} out of {len(df)}")
    if g1_count == 0:
        print("[WARNING] Zero rows passed G1 eligibility! Check if 'pg-1' and 'sg-ttm' contain numeric values.")

    # 3. Calculate SG and PG
    df["_sg"] = np.nan
    df["_pg"] = np.nan

    for idx, r in df.iterrows():
        if not r["_G1_eligible"]:
            continue
        
        # SG derivation
        if r["_G2_eligible"] and r["_sgeq_eligible"]:
            df.at[idx, "_sg"] = 0.6 * (0.25 * r["_sg-eq_val"] + 0.75 * r["_sg-ttm_val"]) + 0.4 * r["_sg-3y_val"]
        elif r["_G2_eligible"] and not r["_sgeq_eligible"]:
            df.at[idx, "_sg"] = 0.6 * r["_sg-ttm_val"] + 0.4 * r["_sg-3y_val"]
        elif not r["_G2_eligible"] and r["_sgeq_eligible"]:
            df.at[idx, "_sg"] = 0.6 * r["_sg-ttm_val"] + 0.4 * r["_sg-eq_val"]
        else:
            df.at[idx, "_sg"] = r["_sg-ttm_val"]

        # PG derivation
        if r["_G2_eligible"]:
            df.at[idx, "_pg"] = 0.6 * r["_pg-1_val"] + 0.4 * r["_pg-3_val"]
        else:
            df.at[idx, "_pg"] = r["_pg-1_val"]

    # 4. Clipping [-50, 100]
    df["_sgc"] = np.nan
    df["_pgc"] = np.nan
    for idx, r in df.iterrows():
        sg_v, pg_v = r["_sg"], r["_pg"]
        if pd.isna(sg_v) or math.isinf(sg_v) or sg_v < -100 or sg_v > 9999:
            continue
        if pd.isna(pg_v) or math.isinf(pg_v) or pg_v < -100 or pg_v > 9999:
            continue
        df.at[idx, "_sgc"] = max(-50.0, min(100.0, sg_v))
        df.at[idx, "_pgc"] = max(-50.0, min(100.0, pg_v))

    # 5. Group assignment
    df["_group"] = np.nan
    for idx, r in df.iterrows():
        sgc, pgc = r["_sgc"], r["_pgc"]
        if pd.isna(sgc) or pd.isna(pgc):
            continue
        if sgc <= 0 and pgc <= 0:
            df.at[idx, "_group"] = "A1"
        elif sgc > 0 and pgc <= 0:
            df.at[idx, "_group"] = "A2"
        elif sgc <= 0 and pgc > 0:
            df.at[idx, "_group"] = "A3"
        elif sgc > 0 and pgc > 0:
            df.at[idx, "_group"] = "A4"

    df["G-score"] = np.nan

    # 6. Scoring Groups A1, A2, and A3
    group_configs = {
        'A1': {'wt_sg': 0.50, 'wt_pg': 0.50, 'base': 0.0, 'scale': 10.0, 'mid': 5.0},
        'A2': {'wt_sg': 0.60, 'wt_pg': 0.40, 'base': 10.01, 'scale': 20.0, 'mid': 20.01},
        'A3': {'wt_sg': 0.25, 'wt_pg': 0.75, 'base': 30.01, 'scale': 20.0, 'mid': 40.01}
    }
    for grp, cfg in group_configs.items():
        sub_idx = df[df["_group"] == grp].index
        if len(sub_idx) == 0:
            continue
        raw_vals = cfg['wt_sg'] * df.loc[sub_idx, "_sgc"] + cfg['wt_pg'] * df.loc[sub_idx, "_pgc"]
        min_v = raw_vals.min()
        max_v = raw_vals.max()
        if len(sub_idx) == 1 or math.isclose(min_v, max_v):
            df.loc[sub_idx, "G-score"] = cfg['mid']
        else:
            df.loc[sub_idx, "G-score"] = cfg['base'] + cfg['scale'] * (raw_vals - min_v) / (max_v - min_v)

    # 7. Scoring Group A4 (Sugeno Fuzzy Model)
    a4_idx = df[df["_group"] == "A4"].index
    print(f"[INFO] Cohort counts: A1={(df['_group']=='A1').sum()}, A2={(df['_group']=='A2').sum()}, A3={(df['_group']=='A3').sum()}, A4={len(a4_idx)}")

    if len(a4_idx) > 0:
        a4_sgc = df.loc[a4_idx, "_sgc"]
        a4_pgc = df.loc[a4_idx, "_pgc"]

        Lmn_sg, Umx_sg, M_sg = compute_a4_cutoffs(a4_sgc)
        Lmn_pg, Umx_pg, M_pg = compute_a4_cutoffs(a4_pgc)

        s_scores = {}
        for idx in a4_idx:
            sg_val = df.at[idx, "_sgc"]
            pg_val = df.at[idx, "_pgc"]

            b_sg, n_sg, g_sg = calculate_memberships(sg_val, Lmn_sg, Umx_sg, M_sg)
            b_pg, n_pg, g_pg = calculate_memberships(pg_val, Lmn_pg, Umx_pg, M_pg)

            s_val = evaluate_sugeno_a4(b_sg, n_sg, g_sg, b_pg, n_pg, g_pg)
            if not math.isnan(s_val):
                s_scores[idx] = s_val

        if s_scores:
            s_series = pd.Series(s_scores)
            s_min = s_series.min()
            s_max = s_series.max()

            for idx, s_val in s_scores.items():
                if math.isclose(s_min, s_max):
                    df.at[idx, "G-score"] = 75.0
                else:
                    g_val = 50.01 + 50.0 * (s_val - s_min) / (s_max - s_min)
                    df.at[idx, "G-score"] = min(100.0, g_val)

    df["G-score"] = df["G-score"].round(2)
    scored_count = df["G-score"].notna().sum()
    print(f"[INFO] G-score successfully calculated for {scored_count} stocks.")

    # 8. Update Firebase safely using multi-path update
    # This prevents replacing the whole node or losing original schema keys
    updates = {}
    for _, row in df.iterrows():
        key = str(row["_firebase_key"])
        score = row["G-score"]
        # Firebase skips None; if NaN or unscored, set to None or 0.0 depending on preference
        updates[f"{key}/G-score"] = None if pd.isna(score) else float(score)

    print(f"[INFO] Pushing {len(updates)} G-score field updates to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.update(updates)
    print(f"[OK] Success! 'G-score' updated across all items in Firebase.")