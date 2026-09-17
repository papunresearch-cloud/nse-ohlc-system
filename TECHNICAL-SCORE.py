import firebase_admin
from firebase_admin import credentials, db
import numpy as np
import pandas as pd

# =====================================================================
# CONFIGURATION
# =====================================================================
FIREBASE_KEY_FILE = "serviceAccountKey.json"
FIREBASE_DB_URL = "https://stock-dashboard-5c25c-default-rtdb.asia-southeast1.firebasedatabase.app"
FIREBASE_TARGET_NODE = "SCREENER"

# DMA constants
pl, ph = 1, 90
n = 4.5
ke = 0.5
w1 = round(1 / np.power(1, 0.25), 2)
w2 = round(1 / np.power(50, 0.25), 2)
w3 = round(1 / np.power(200, 0.25), 2)

# RETURN constants
RETURN_COLS = ["1wr", "1mr", "3mr", "6mr", "1yr", "3yr"]
MCAP_COL = "mcap"
TOP_MCAP_COUNT = 1000
PCT_RANK_LOW, PCT_RANK_HIGH = 0.45, 0.55
LEVEL_DAYS = {"3Y": 1095, "1Y": 365, "6M": 182, "3M": 91, "1M": 30, "1W": 7}
P_LOW, P_HIGH = 5, 95

# Final weights for T-score
tw1, tw2, tw3 = 0.6, 0.3, 0.1

# =====================================================================
# FIREBASE SETUP
# =====================================================================
def init_firebase():
    """Initializes the Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })

# =====================================================================
# PART 1: DMA-SCORE ENGINE
# =====================================================================
def normalize_prices(df):
    denom = df["52wh"] - df["52wl"]
    df["x1"] = 100 * (df["cmp"] - df["52wl"]) / denom
    df["x2"] = 100 * (df["50ma"] - df["52wl"]) / denom
    df["x3"] = 100 * (df["200ma"] - df["52wl"]) / denom
    return df

def raw_range(df):
    df["RawRange"] = 200 * (df["52wh"] - df["52wl"]) / (df["52wh"] + df["52wl"])
    return df

def robust_percentile_normalization(series):
    valid_data = series.dropna()
    if valid_data.empty:
        return series
    px1, px2 = np.nanpercentile(valid_data, [pl, ph])
    if px2 == px1:
        return pd.Series(50.0, index=series.index)
    norm = (series - px1) / (px2 - px1) * 100.0
    return norm.clip(0, 100)

def apply_range_normalization(df):
    df["R"] = robust_percentile_normalization(df["RawRange"])
    df["r"] = df["R"] / 100.0
    df["F"] = 1 - ke * np.exp(-n * df["r"])
    return df

def compute_psk(df):
    df["PSK"] = (w1 * df["x1"] + w2 * df["x2"] + w3 * df["x3"]) / (w1 + w2 + w3)
    return df

def compute_f1(df):
    df["F1"] = (df["PSK"] - 50) * df["F"]
    return df

def normalize_f1(df):
    f1_min, f1_max = df["F1"].min(), df["F1"].max()
    if pd.isna(f1_min) or pd.isna(f1_max) or f1_max == f1_min:
        df["P-SCORE"] = 50.0
    else:
        df["P-SCORE"] = ((df["F1"] - f1_min) / (f1_max - f1_min)) * 100.0
    return df

# =====================================================================
# PART 2: RETURN SCORE ENGINE
# =====================================================================
def compute_normalized_prices(df):
    mapping = {"1wr": "P1W", "1mr": "P1M", "3mr": "P3M", "6mr": "P6M", "1yr": "P1Y", "3yr": "P3Y"}
    for ret_col, price_col in mapping.items():
        if ret_col in df.columns:
            valid_returns = df[ret_col].where(df[ret_col] > -100.0, np.nan)
            df[price_col] = 100.0 / (1.0 + (valid_returns / 100.0))
        else:
            df[price_col] = np.nan
    return df

def compute_mcap_trimmed_benchmark(df, col_name):
    if MCAP_COL not in df.columns:
        valid_series = df[col_name].dropna()
    else:
        top_mcap_df = df.nlargest(TOP_MCAP_COUNT, MCAP_COL)
        valid_series = top_mcap_df[col_name].dropna()

    if valid_series.empty:
        return np.nan
    q_low = valid_series.quantile(PCT_RANK_LOW)
    q_high = valid_series.quantile(PCT_RANK_HIGH)
    mid_slice = valid_series[(valid_series >= q_low) & (valid_series <= q_high)]
    return float(mid_slice.mean()) if not mid_slice.empty else np.nan

def compute_level_differences(df):
    price_cols = ["P1W", "P1M", "P3M", "P6M", "P1Y", "P3Y"]
    for col in price_cols:
        bm = compute_mcap_trimmed_benchmark(df, col)
        df[f"LD_{col}"] = bm - df[col]
    return df

def normalize_weights_dict(w_dict):
    total = sum(w_dict.values())
    return {k: v / total for k, v in w_dict.items()} if total > 0 else w_dict

def get_base_level_weights():
    level_raw = {k: np.power(v, 0.25) for k, v in LEVEL_DAYS.items()}
    return normalize_weights_dict(level_raw)

def calculate_dynamic_weighted_score(df, base_weights):
    cols = list(base_weights.keys())
    vals = df[cols].values
    weights = np.array([base_weights[c] for c in cols])
    valid_mask = ~np.isnan(vals)
    effective_weights = valid_mask * weights
    weight_sum = effective_weights.sum(axis=1, keepdims=True)

    with np.errstate(divide="ignore", invalid="ignore"):
        normalized_row_weights = np.where(weight_sum > 0, effective_weights / weight_sum, 0.0)

    safe_vals = np.nan_to_num(vals, nan=0.0)
    scores = (safe_vals * normalized_row_weights).sum(axis=1)
    scores = np.where(weight_sum.squeeze(axis=1) > 0, scores, np.nan)
    return pd.Series(scores, index=df.index)

def compute_raw_scores(df, level_w):
    ot_ld_weights = {f"LD_P{k}": v for k, v in level_w.items()}
    df["OT_SCORE_RAW"] = calculate_dynamic_weighted_score(df, ot_ld_weights)

    level_keys = list(level_w.keys())
    level_vals_rev = list(level_w.values())[::-1]
    st_level_w = dict(zip(level_keys, level_vals_rev))
    st_ld_weights = {f"LD_P{k}": v for k, v in st_level_w.items()}
    df["ST_SCORE_RAW"] = calculate_dynamic_weighted_score(df, st_ld_weights)
    return df

def normalize_score_series(series, p_low=P_LOW, p_high=P_HIGH):
    valid_data = series.dropna()
    if valid_data.empty:
        return series
    p5 = np.percentile(valid_data, p_low)
    p95 = np.percentile(valid_data, p_high)
    if p95 == p5:
        return series.apply(lambda x: 50.0 if pd.notnull(x) else np.nan)
    normalized = ((series - p5) / (p95 - p5)) * 100.0
    return normalized.clip(lower=0.0, upper=100.0)

# =====================================================================
# PART 3: FINAL TECHNICAL SCORE
# =====================================================================
def compute_t_score(df):
    df["T-score"] = tw1 * df["P-SCORE"] + tw2 * df["ST-SCORE"] + tw3 * df["OT-SCORE"]
    return df

def update_technical_score():
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

    # Coerce numeric columns safely across the entire database
    num_cols = ["cmp", "50ma", "200ma", "52wh", "52wl", MCAP_COL] + RETURN_COLS
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = np.nan

    # Identify rows with sufficient data to calculate technical metrics
    valid_mask = (
        df[["cmp", "50ma", "200ma", "52wh", "52wl"]].notna().all(axis=1) &
        (df["52wh"] > df["52wl"]) &
        (df["cmp"] > 0)
    )

    if not valid_mask.any():
        print("[WARN] No records found with sufficient technical data to score.")
        return

    print(f"[INFO] Calculating T-score for {valid_mask.sum()} eligible records...")
    calc_df = df[valid_mask].copy()

    # --- DMA part ---
    calc_df = normalize_prices(calc_df)
    calc_df = raw_range(calc_df)
    calc_df = apply_range_normalization(calc_df)
    calc_df = compute_psk(calc_df)
    calc_df = compute_f1(calc_df)
    calc_df = normalize_f1(calc_df)
    calc_df.drop(columns=["x1", "x2", "x3", "RawRange", "R", "r", "F", "F1"],
                 inplace=True, errors="ignore")

    # --- RETURN part ---
    calc_df = compute_normalized_prices(calc_df)
    calc_df = compute_level_differences(calc_df)
    level_w = get_base_level_weights()
    calc_df = compute_raw_scores(calc_df, level_w)
    calc_df["OT-SCORE"] = normalize_score_series(calc_df["OT_SCORE_RAW"])
    calc_df["ST-SCORE"] = normalize_score_series(calc_df["ST_SCORE_RAW"])
    calc_df.drop(columns=["P1W", "P1M", "P3M", "P6M", "P1Y", "P3Y",
                          "LD_P1W", "LD_P1M", "LD_P3M", "LD_P6M", "LD_P1Y", "LD_P3Y",
                          "OT_SCORE_RAW", "ST_SCORE_RAW"],
                 inplace=True, errors="ignore")

    # --- TECHNICAL SCORE part ---
    calc_df = compute_t_score(calc_df)

    # Assign T-score back into main dataframe
    df["T-score"] = np.nan
    df.loc[valid_mask, "T-score"] = calc_df["T-score"].round(2)

    # Clean float/NaN/infinite values to None for proper JSON serialization
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    if "CODE" in cleaned_df.columns:
        payload = cleaned_df.set_index("CODE", drop=False).to_dict(orient="index")
    else:
        payload = cleaned_df.to_dict(orient="index")

    print(f"[INFO] Writing records with updated 'T-score' back to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(payload)
    print(f"[OK] Success! Updated {len(cleaned_df)} records in Firebase node '/{FIREBASE_TARGET_NODE}'.")

if __name__ == "__main__":
    try:
        update_technical_score()
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")