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
    """Initializes the Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

# =====================================================================
# PART 1: VALIDATION & DATA CHECKS
# =====================================================================
def validate_metric(val):
    """
    Validates if a metric value is finite and in [-100, 9999] inclusive[cite: 2, 4].
    Returns: (is_valid: bool, numeric_val: float)
    """
    if pd.isna(val):
        return False, np.nan
    try:
        fval = float(val)
    except (ValueError, TypeError):
        return False, np.nan
    
    if math.isnan(fval) or math.isinf(fval):
        return False, np.nan
    if fval < -100 or fval > 9999:
        return False, fval
    
    return True, fval

# =====================================================================
# PART 2: A4 FUZZY MEMBERSHIP & SUGENO LOGIC
# =====================================================================
def compute_a4_cutoffs(series):
    """
    Computes Lmn (5th percentile), Umx (95th percentile) via linear interpolation,
    and M (center of gravity) trimming floor(0.05 * N) from both ends[cite: 2, 4].
    """
    vals = np.sort(series.dropna().values)
    n = len(vals)
    if n == 0:
        return np.nan, np.nan, np.nan
    
    Lmn = float(np.percentile(vals, 5, method='linear'))
    Umx = float(np.percentile(vals, 95, method='linear'))
    
    trim_k = math.floor(0.05 * n)
    if trim_k > 0 and (2 * trim_k < n):
        trimmed_vals = vals[trim_k : n - trim_k]
    else:
        trimmed_vals = vals
        
    M = float(np.mean(trimmed_vals))
    return Lmn, Umx, M


def calculate_memberships(x, Lmn, Umx, M):
    """
    Calculates Bad, Normal, and Good memberships with straight-line transitions[cite: 2, 4].
    Handles zero-width boundaries and guarantees sum(memberships) == 1.0[cite: 2, 4].
    """
    if pd.isna(x):
        return np.nan, np.nan, np.nan
    
    # Special Case: Lmn == Umx[cite: 2, 4]
    if math.isclose(Lmn, Umx):
        if math.isclose(x, Lmn):
            return 0.0, 1.0, 0.0
        elif x < Lmn:
            return 1.0, 0.0, 0.0
        else:
            return 0.0, 0.0, 1.0

    # Rule 1: x <= Lmn[cite: 2, 4]
    if x <= Lmn:
        return 1.0, 0.0, 0.0
    
    # Rule 2: Lmn < x < M[cite: 2, 4]
    if x < M:
        denom = M - Lmn
        if math.isclose(denom, 0.0):
            return 0.0, 1.0, 0.0
        bad = (M - x) / denom
        normal = (x - Lmn) / denom
        return bad, normal, 0.0
    
    # Rule 3: x == M[cite: 2, 4]
    if math.isclose(x, M):
        return 0.0, 1.0, 0.0
    
    # Rule 4: M < x < Umx[cite: 2, 4]
    if x < Umx:
        denom = Umx - M
        if math.isclose(denom, 0.0):
            return 0.0, 0.0, 1.0
        normal = (Umx - x) / denom
        good = (x - M) / denom
        return 0.0, normal, good
    
    # Rule 5: x >= Umx[cite: 2, 4]
    return 0.0, 0.0, 1.0


def evaluate_sugeno_a4(bad_sg, norm_sg, good_sg, bad_pg, norm_pg, good_pg):
    """
    Evaluates zero-order Sugeno rules (Columns: SG, Rows: PG)[cite: 2, 4]:
    Firing strength = product of memberships[cite: 2, 4].
    """
    if any(pd.isna([bad_sg, norm_sg, good_sg, bad_pg, norm_pg, good_pg])):
        return np.nan
    
    rules = [
        (good_pg * good_sg, 100.0),
        (good_pg * norm_sg, 85.0),
        (good_pg * bad_sg, 62.0),
        (norm_pg * good_sg, 75.0),
        (norm_pg * norm_sg, 50.0),
        (norm_pg * bad_sg, 25.0),
        (bad_pg * good_sg, 38.0),
        (bad_pg * norm_sg, 12.0),
        (bad_pg * bad_sg, 0.0),
    ]
    
    total_wt = sum(w for w, _ in rules)
    if total_wt <= 0 or math.isnan(total_wt):
        return np.nan
    
    S = sum(w * score for w, score in rules) / total_wt
    return S

# =====================================================================
# PART 3: GROWTH SCORE PIPELINE
# =====================================================================
def update_growth_scores():
    print("[INFO] Connecting to Firebase Realtime Database...")
    init_firebase()

    ref = db.reference(FIREBASE_TARGET_NODE)
    data = ref.get()

    if not data:
        print(f"[ERROR] No data found at Firebase node '/{FIREBASE_TARGET_NODE}'.")
        return

    print("[INFO] Loading records into DataFrame...")
    if isinstance(data, dict):
        df = pd.DataFrame.from_dict(data, orient="index")
    else:
        df = pd.DataFrame(data)

    required_cols = ["pg-1", "pg-3", "sg-eq", "sg-ttm", "sg-3y"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    # 1. Validate metric ranges [-100, 9999][cite: 2, 4]
    for col in required_cols:
        res = [validate_metric(v) for v in df[col]]
        df[f"_{col}_valid"] = [r[0] for r in res]
        df[f"_{col}_val"] = [r[1] for r in res]

    # 2. Check G1, G2, and sg-eq eligibility[cite: 2, 4]
    df["_G1_eligible"] = df["_sg-ttm_valid"] & df["_pg-1_valid"]
    df["_G2_eligible"] = df["_sg-3y_valid"] & df["_pg-3_valid"]
    df["_sgeq_eligible"] = df["_sg-eq_valid"]

    # 3. Calculate SG and PG[cite: 2, 4]
    df["_sg"] = np.nan
    df["_pg"] = np.nan

    for idx, r in df.iterrows():
        if not r["_G1_eligible"]:
            continue
        
        # Derived SG[cite: 2, 4]
        if r["_G2_eligible"] and r["_sgeq_eligible"]:
            df.at[idx, "_sg"] = 0.6 * (0.25 * r["_sg-eq_val"] + 0.75 * r["_sg-ttm_val"]) + 0.4 * r["_sg-3y_val"]
        elif r["_G2_eligible"] and not r["_sgeq_eligible"]:
            df.at[idx, "_sg"] = 0.6 * r["_sg-ttm_val"] + 0.4 * r["_sg-3y_val"]
        elif not r["_G2_eligible"] and r["_sgeq_eligible"]:
            df.at[idx, "_sg"] = 0.6 * r["_sg-ttm_val"] + 0.4 * r["_sg-eq_val"]
        else:
            df.at[idx, "_sg"] = r["_sg-ttm_val"]

        # Derived PG[cite: 2, 4]
        if r["_G2_eligible"]:
            df.at[idx, "_pg"] = 0.6 * r["_pg-1_val"] + 0.4 * r["_pg-3_val"]
        else:
            df.at[idx, "_pg"] = r["_pg-1_val"]

    # 4. Range validation and clipping [-50, 100][cite: 2, 4]
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

    # 5. Group assignment (A1 to A4)[cite: 2, 4]
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

    # 6. Scoring Groups A1, A2, and A3[cite: 2, 4]
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

    # 7. Scoring Group A4 (Zero-Order Sugeno Fuzzy Model)[cite: 2, 4]
    a4_idx = df[df["_group"] == "A4"].index
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

    # Round G-score to 2 decimal places
    df["G-score"] = df["G-score"].round(2)

    # 8. Drop all internal/intermediate helper columns[cite: 3]
    temp_cols = [c for c in df.columns if c.startswith("_")]
    df.drop(columns=temp_cols, inplace=True, errors="ignore")

    scored_count = df["G-score"].notna().sum()
    print(f"[INFO] G-score calculated for {scored_count} stocks.")

    # 9. Sanitize and write back to Firebase[cite: 3]
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    if "CODE" in cleaned_df.columns:
        payload = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")
    else:
        payload = cleaned_df.to_dict(orient="index")

    print(f"[INFO] Writing records with updated 'G-score' back to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(payload)
    print(f"[OK] Success! Single column 'G-score' updated in Firebase.")

# =====================================================================
# ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    try:
        update_growth_scores()
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")