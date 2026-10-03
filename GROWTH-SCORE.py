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
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

# =====================================================================
# 1. VALIDATION & SANITIZATION LOGIC
# =====================================================================
def validate_metric(val):
    """
    Validates if a metric value is finite and in [-100, 9999] inclusive.
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
# 2. MAIN PIPELINE
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

    required_cols = ['sg-eq', 'sg-ttm', 'sg-3y', 'pg-1', 'pg-3']
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    # 1. Metric Cell Validation [-100, 9999]
    for col in required_cols:
        res = [validate_metric(v) for v in df[col]]
        df[f"{col}_val"] = [r[1] for r in res]
        df[f"{col}_valid"] = [r[0] for r in res]

    # 2. Eligibility & G1 Gatekeeper
    df['G1_eligible'] = df['sg-ttm_valid'] & df['pg-1_valid']
    df['G2_eligible'] = df['sg-3y_valid'] & df['pg-3_valid']
    df['sgeq_eligible'] = df['sg-eq_valid']
    df['eligible'] = df['G1_eligible']

    # 3. Derive SG and PG
    df['sg'] = np.nan
    df['pg'] = np.nan

    for idx, r in df.iterrows():
        if not r['eligible']:
            continue
        
        # SG Derivation
        if r['G1_eligible'] and r['G2_eligible'] and r['sgeq_eligible']:
            df.at[idx, 'sg'] = 0.6 * (0.25 * r['sg-eq_val'] + 0.75 * r['sg-ttm_val']) + 0.4 * r['sg-3y_val']
        elif r['G1_eligible'] and r['G2_eligible'] and not r['sgeq_eligible']:
            df.at[idx, 'sg'] = 0.6 * r['sg-ttm_val'] + 0.4 * r['sg-3y_val']
        elif r['G1_eligible'] and not r['G2_eligible'] and r['sgeq_eligible']:
            df.at[idx, 'sg'] = 0.6 * r['sg-ttm_val'] + 0.4 * r['sg-eq_val']
        elif r['G1_eligible'] and not r['G2_eligible'] and not r['sgeq_eligible']:
            df.at[idx, 'sg'] = r['sg-ttm_val']
            
        # PG Derivation
        if r['G1_eligible'] and r['G2_eligible']:
            df.at[idx, 'pg'] = 0.6 * r['pg-1_val'] + 0.4 * r['pg-3_val']
        elif r['G1_eligible'] and not r['G2_eligible']:
            df.at[idx, 'pg'] = r['pg-1_val']

    # 4. Clipping to [-50, 100]
    df['sgc'] = np.nan
    df['pgc'] = np.nan

    for idx, r in df.iterrows():
        if not r['eligible']:
            continue
        sg_val, pg_val = r['sg'], r['pg']
        if pd.isna(sg_val) or math.isinf(sg_val) or sg_val < -100 or sg_val > 9999:
            df.at[idx, 'eligible'] = False
            continue
        if pd.isna(pg_val) or math.isinf(pg_val) or pg_val < -100 or pg_val > 9999:
            df.at[idx, 'eligible'] = False
            continue
            
        df.at[idx, 'sgc'] = max(-50.0, min(100.0, sg_val))
        df.at[idx, 'pgc'] = max(-50.0, min(100.0, pg_val))

    # =================================================================
    # CONTINUOUS WEIGHTED G-SCORE NORMALIZED TO 0 - 100 SCALE
    # =================================================================
    df['G-score'] = np.nan
    eligible_mask = df['eligible'] & df['sgc'].notna() & df['pgc'].notna()

    if eligible_mask.any():
        sgc = df.loc[eligible_mask, 'sgc']
        pgc = df.loc[eligible_mask, 'pgc']

        # diff = ABS(sgc - pgc)
        diff = (sgc - pgc).abs()

        # kf = 100 / (1 + EXP(LN(9) * (diff - 50) / 25))
        kf = 100.0 / (1.0 + np.exp(np.log(9.0) * (diff - 50.0) / 25.0))

        # k = 0.25 + 0.0075 * kf
        k = 0.25 + (0.0075 * kf)

        # fv = (sgc + k * pgc) / (1 + k)
        fv = (sgc + k * pgc) / (1.0 + k)

        # Min-Max Rescaling to 0 - 100 scale
        fv_min = fv.min()
        fv_max = fv.max()

        if pd.notna(fv_min) and pd.notna(fv_max):
            if math.isclose(fv_min, fv_max):
                df.loc[eligible_mask, 'G-score'] = 50.0
            else:
                g_norm = ((fv - fv_min) / (fv_max - fv_min)) * 100.0
                df.loc[eligible_mask, 'G-score'] = g_norm.round(2)

    scored_count = df['G-score'].notna().sum()
    print(f"[INFO] G-score computed for {scored_count} stocks.")

    # 9. Clean up all temporary columns
    calc_cols = [
        'G1_eligible', 'G2_eligible', 'sgeq_eligible', 'eligible',
        'sg', 'pg', 'sgc', 'pgc', 'group', 'sugeno_S', 'G_score_calc'
    ] + [f"{c}_val" for c in required_cols] + [f"{c}_valid" for c in required_cols]
    calc_cols += ['_pg_c', '_sg_c', '_m', '_n', 'pg-c', 'sg-c']
    
    df.drop(columns=calc_cols, inplace=True, errors="ignore")

    # 10. Sanitize and write back to Firebase
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    if "CODE" in cleaned_df.columns:
        payload = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")
    else:
        payload = cleaned_df.to_dict(orient="index")

    print(f"[INFO] Writing records with updated 'G-score' to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(payload)
    print(f"[OK] Success! Single column 'G-score' updated in Firebase.")

if __name__ == "__main__":
    try:
        update_growth_scores()
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")