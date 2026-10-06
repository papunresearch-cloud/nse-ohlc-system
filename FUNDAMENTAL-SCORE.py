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

OUTPUT_COLUMN = "F-score"
SENTINEL_DISQUALIFIED = 101.0

# Mathematical Model Constants
L_PARAM = 1.0
B_PARAM = 0.025
T0_PARAM = 7.5
P_PARAM = 25.0


def init_firebase():
    """Initializes Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })


# =====================================================================
# 1. VALIDATION FUNCTIONS
# =====================================================================
def validate_value(val):
    """
    Validates that a profitability value is a finite real number
    in the inclusive range [-999, 999]. Missing, NaN, text, and infinities
    are treated as invalid.
    """
    if pd.isna(val) or isinstance(val, bool):
        return False, np.nan
    try:
        fval = float(val)
    except (ValueError, TypeError):
        return False, np.nan

    if math.isnan(fval) or math.isinf(fval):
        return False, np.nan
    if fval < -999.0 or fval > 999.0:
        return False, np.nan

    return True, fval


# =====================================================================
# 2. ROW-LEVEL EVALUATION PIPELINE
# =====================================================================
def evaluate_stock_row(row):
    """
    Evaluates a single company row:
    - Atomically validates groups G1 (0-year), G2 (1-year), G3 (3-year).
    - Rejects the row (sentinel 101) if G1 is invalid.
    - Cascades G1-only, G1+G2, or G1+G2+G3 based on group validity.
    - Clamps derived ROE-c and ROA-c into [-25, 50].
    - Computes logistic K and blended Fv.
    Returns: (status: 'OK' | 'DISQUALIFIED', fv_value: float)
    """
    # Group 1: Latest Year
    g1_roe_ok, r0 = validate_value(row.get("roe-0"))
    g1_roa_ok, a0 = validate_value(row.get("roa-0"))
    g1_valid = g1_roe_ok and g1_roa_ok

    # Rule: If G1 is rejected, do not calculate score for the row
    if not g1_valid:
        return "DISQUALIFIED", SENTINEL_DISQUALIFIED

    # Group 2: Preceding Year
    g2_roe_ok, r1 = validate_value(row.get("roe-1"))
    g2_roa_ok, a1 = validate_value(row.get("roa-1"))
    g2_valid = g2_roe_ok and g2_roa_ok

    # Group 3: 3-Year Average
    g3_roe_ok, r3y = validate_value(row.get("roe-3y"))
    g3_roa_ok, a3y = validate_value(row.get("roa-3y"))
    g3_valid = g3_roe_ok and g3_roa_ok

    # Cascade logic:
    # 1. If G1 accepted, G2 rejected -> use G1 only (reject G3 even if valid)
    # 2. If G1 and G2 accepted, G3 rejected -> use G1 and G2
    # 3. If all three accepted -> use all three
    if not g2_valid:
        roe_raw = r0
        roa_raw = a0
    elif not g3_valid:
        roe_raw = (0.60 * r0) + (0.40 * r1)
        roa_raw = (0.60 * a0) + (0.40 * a1)
    else:
        roe_raw = (0.40 * r0) + (0.35 * r1) + (0.25 * r3y)
        roa_raw = (0.40 * a0) + (0.35 * a1) + (0.25 * a3y)

    # Clamping: Rf = max(-25, min(50, R)) separately for ROE-c & ROA-c
    roe_c = max(-25.0, min(50.0, roe_raw))
    roa_c = max(-25.0, min(50.0, roa_raw))

    # K calculation: K = L / ((1 + exp(-B * (ROA-c - T0))) ^ P)
    exponent = -B_PARAM * (roa_c - T0_PARAM)
    # Numerical safeguard for large exp inputs
    clipped_exp = max(-500.0, min(500.0, exponent))
    exp_term = math.exp(clipped_exp)
    k = L_PARAM / (1.0 + exp_term ** P_PARAM)

    # Fundamental value: Fv = (ROA-c + K * ROE-c) / (1 + K)
    fv = (roa_c + (k * roe_c)) / (1.0 + k)

    return "OK", fv


# =====================================================================
# 3. MAIN EXECUTION PIPELINE
# =====================================================================
def run_fundamental_scoring():
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

    profitability_cols = ["roe-0", "roe-1", "roe-3y", "roa-0", "roa-1", "roa-3y"]
    for col in profitability_cols:
        if col not in df.columns:
            df[col] = np.nan

    print("[INFO] Validating year groups and deriving fundamental value (Fv)...")
    eval_results = df.apply(evaluate_stock_row, axis=1)

    statuses = [res[0] for res in eval_results]
    fv_values = [res[1] for res in eval_results]

    df["_eval_status"] = statuses
    df["_fv"] = fv_values

    valid_mask = df["_eval_status"] == "OK"
    disqualified_mask = df["_eval_status"] == "DISQUALIFIED"

    # Initialize F-score column
    df[OUTPUT_COLUMN] = np.nan

    # Assign sentinel 101 to disqualified rows
    df.loc[disqualified_mask, OUTPUT_COLUMN] = SENTINEL_DISQUALIFIED

    # Cross-sectional normalization: F-score 0 to 100 for valid stocks
    if valid_mask.any():
        valid_fv = df.loc[valid_mask, "_fv"]
        fv_min = valid_fv.min()
        fv_max = valid_fv.max()

        print(f"[INFO] Cross-sectional bounds: Fv_min = {fv_min:.4f}, Fv_max = {fv_max:.4f}")

        if math.isclose(fv_min, fv_max):
            # Degenerate case fallback
            df.loc[valid_mask, OUTPUT_COLUMN] = 50.0
        else:
            normalized_scores = ((valid_fv - fv_min) / (fv_max - fv_min)) * 100.0
            df.loc[valid_mask, OUTPUT_COLUMN] = normalized_scores.round(2)

    valid_count = valid_mask.sum()
    disqualified_count = disqualified_mask.sum()
    print(f"[INFO] Scoring finished: {valid_count} scored stocks, {disqualified_count} disqualified stocks (assigned 101).")

    # Clean intermediate calculation columns
    df.drop(columns=["_eval_status", "_fv", "roe-c", "roa-c"], inplace=True, errors="ignore")

    # Sanitize NaN/inf values for JSON serialization
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    # Persist keyed dictionary structure using CODE as primary key
    if "CODE" in cleaned_df.columns:
        payload = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")
    else:
        payload = cleaned_df.to_dict(orient="index")

    print(f"[INFO] Writing records with '{OUTPUT_COLUMN}' back to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(payload)
    print(f"[OK] Success! Single column '{OUTPUT_COLUMN}' updated in Firebase.")


if __name__ == "__main__":
    try:
        run_fundamental_scoring()
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")