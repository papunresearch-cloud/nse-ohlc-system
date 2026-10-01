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
# 2. SUGENO & FUZZY LOGIC FOR GROUP A4
# =====================================================================
def compute_a4_cutoffs(series):
    """
    Computes Lmn (5th percentile), Umx (95th percentile) via linear interpolation,
    and M (center of gravity) trimming floor(0.05 * N) from both ends.
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
    Calculates Bad, Normal, and Good memberships with straight-line transitions.
    Guarantees sum(memberships) == 1.0.
    """
    if pd.isna(x):
        return np.nan, np.nan, np.nan
    
    if math.isclose(Lmn, Umx):
        if math.isclose(x, Lmn):
            return 0.0, 1.0, 0.0
        elif x < Lmn:
            return 1.0, 0.0, 0.0
        else:
            return 0.0, 0.0, 1.0

    if x <= Lmn:
        return 1.0, 0.0, 0.0
    
    if x < M:
        denom = M - Lmn
        if math.isclose(denom, 0.0):
            return 0.0, 1.0, 0.0
        bad = (M - x) / denom
        normal = (x - Lmn) / denom
        return bad, normal, 0.0
    
    if math.isclose(x, M):
        return 0.0, 1.0, 0.0
    
    if x < Umx:
        denom = Umx - M
        if math.isclose(denom, 0.0):
            return 0.0, 0.0, 1.0
        normal = (Umx - x) / denom
        good = (x - M) / denom
        return 0.0, normal, good
    
    return 0.0, 0.0, 1.0

def evaluate_sugeno_a4(bad_sg, norm_sg, good_sg, bad_pg, norm_pg, good_pg):
    """
    Evaluates zero-order Sugeno rules (Columns: SG, Rows: PG):
    Firing strength = product of memberships.
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
    
    return sum(w * score for w, score in rules) / total_wt

# =====================================================================
# 3. MAIN PIPELINE
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

    # 5. Group Partitioning (A1, A2, A3, A4)
    # Fix: Explicitly create 'group' as object dtype to prevent float64 assignment error
    df['group'] = pd.Series(index=df.index, dtype='object')
    for idx, r in df.iterrows():
        if not r['eligible']:
            continue
        sgc, pgc = r['sgc'], r['pgc']
        if sgc <= 0 and pgc <= 0:
            df.at[idx, 'group'] = 'A1'
        elif sgc > 0 and pgc <= 0:
            df.at[idx, 'group'] = 'A2'
        elif sgc <= 0 and pgc > 0:
            df.at[idx, 'group'] = 'A3'
        elif sgc > 0 and pgc > 0:
            df.at[idx, 'group'] = 'A4'

    # 6. Rescale Groups A1, A2, and A3
    df['G_score_calc'] = np.nan
    group_configs = {
        'A1': {'wt_sg': 0.5, 'wt_pg': 0.5, 'base': 0.0, 'scale': 10.0, 'mid': 5.0},
        'A2': {'wt_sg': 0.6, 'wt_pg': 0.4, 'base': 10.01, 'scale': 20.0, 'mid': 20.01},
        'A3': {'wt_sg': 0.25, 'wt_pg': 0.75, 'base': 30.01, 'scale': 20.0, 'mid': 40.01}
    }
    
    for grp, cfg in group_configs.items():
        sub_idx = df[df['group'] == grp].index
        if len(sub_idx) == 0:
            continue
        
        raw_vals = cfg['wt_sg'] * df.loc[sub_idx, 'sgc'] + cfg['wt_pg'] * df.loc[sub_idx, 'pgc']
        min_v = raw_vals.min()
        max_v = raw_vals.max()
        
        if len(sub_idx) == 1 or math.isclose(min_v, max_v):
            df.loc[sub_idx, 'G_score_calc'] = cfg['mid']
        else:
            df.loc[sub_idx, 'G_score_calc'] = cfg['base'] + cfg['scale'] * (raw_vals - min_v) / (max_v - min_v)

    # 7. Score A4 using Sugeno Fuzzy Inference
    a4_idx = df[df['group'] == 'A4'].index
    if len(a4_idx) > 0:
        a4_sgc = df.loc[a4_idx, 'sgc']
        a4_pgc = df.loc[a4_idx, 'pgc']
        
        Lmn_sg, Umx_sg, M_sg = compute_a4_cutoffs(a4_sgc)
        Lmn_pg, Umx_pg, M_pg = compute_a4_cutoffs(a4_pgc)
        
        df['sugeno_S'] = np.nan
        for idx in a4_idx:
            sg_val = df.at[idx, 'sgc']
            pg_val = df.at[idx, 'pgc']
            
            b_sg, n_sg, g_sg = calculate_memberships(sg_val, Lmn_sg, Umx_sg, M_sg)
            b_pg, n_pg, g_pg = calculate_memberships(pg_val, Lmn_pg, Umx_pg, M_pg)
            
            S = evaluate_sugeno_a4(b_sg, n_sg, g_sg, b_pg, n_pg, g_pg)
            if not math.isnan(S):
                df.at[idx, 'sugeno_S'] = S
                
        valid_S = df.loc[a4_idx, 'sugeno_S'].dropna()
        if len(valid_S) > 0:
            S_min = valid_S.min()
            S_max = valid_S.max()
            
            for idx in a4_idx:
                s_val = df.at[idx, 'sugeno_S']
                if pd.notna(s_val):
                    if math.isclose(S_min, S_max):
                        df.at[idx, 'G_score_calc'] = 75.0
                    else:
                        g_val = 50.01 + 50.0 * (s_val - S_min) / (S_max - S_min)
                        df.at[idx, 'G_score_calc'] = min(100.0, g_val)

    # 8. Assign final G-score
    df['G-score'] = df['G_score_calc'].round(2)
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