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

REFERENCE = 0.25  # Threshold factor for closeness in roe-c and roa-c

DIV_COLUMN = "advdp"
OUTPUT_COLUMN = "F-score"

# Dividend Model Parameters
IDEAL_DIV_MIN = 25.0  # when F1 = 0
IDEAL_DIV_MAX = 65.0  # when F1 = 100
LEFT_SIGMA = 24.0
RIGHT_SIGMA = 10.0
MAX_CORRECTION = 8.0
SIGMOID_MIDPOINT = 30.0
SIGMOID_STEEPNESS = 0.14
SIGMOID_POWER = 3.3


def init_firebase():
    """Initializes the Firebase Admin SDK if not already active."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY_FILE)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })


# =====================================================================
# PART 1: ROE-C & ROA-C CALCULATION LOGIC
# =====================================================================
def calculate_3pt_consensus(r0, r1, r3y):
    """
    Consensus logic shared by roe-c and roa-c:
    Evaluates 0-year, 1-year, and 3-year values against missing patterns
    and distance clustering.
    """
    v0, v1, v3y = pd.notna(r0), pd.notna(r1), pd.notna(r3y)

    # A: All three missing
    if not v0 and not v1 and not v3y:
        return 0.0

    # One missing
    if not v3y and v0 and v1:  # B: 3y missing
        return (0.55 * r0) + (0.45 * r1)
    if not v0 and v1 and v3y:  # C: 0 missing
        return 0.0
    if not v1 and v0 and v3y:  # D: 1 missing
        return 0.0

    # Two missing
    if v0 and not v1 and not v3y:  # E: only 0 available
        return r0
    if v1 and not v0 and not v3y:  # F: only 1 available
        return r1
    if v3y and not v0 and not v1:  # G: only 3y available
        return r3y

    # All three available
    if v0 and v1 and v3y:
        x1, x2, x3 = r0, r1, r3y
        x12, x23, x13 = abs(x1 - x2), abs(x2 - x3), abs(x1 - x3)

        k = np.median([x1, x2, x3]) * REFERENCE

        # Condition 1: all three close
        if (x12 < k) and (x23 < k) and (x13 < k):
            return (0.2 * x1) + (0.3 * x2) + (0.5 * x3)

        # Closest pair selection
        pairs = {"12": x12, "23": x23, "13": x13}
        closest = min(pairs, key=pairs.get)

        if closest == "12":
            return (0.55 * x1) + (0.45 * x2)
        elif closest == "23":
            return (0.75 * x3) + (0.25 * x2)
        elif closest == "13":
            return (0.75 * x3) + (0.25 * x1)

        return np.median([x1, x2, x3])

    return 0.0


# =====================================================================
# PART 2: 5X5 SUGENO FUZZY ENGINE
# =====================================================================
def sugeno_fuzzy_engine_5x5(X, Y):
    if pd.isna(X) or pd.isna(Y):
        return np.nan

    centers = {"LL": 0, "L": 25, "M": 50, "H": 75, "HH": 100}
    spreads = {"LL": 10.617, "L": 10.617, "M": 10.617, "H": 10.617, "HH": 21.233}

    Z_LL = min(20, max(0, (0 + 0.50 * (X + Y))))
    Z_L = min(40, max(20, (20 + 0.50 * (X + Y - 20))))
    Z_M = min(60, max(40, (40 + 0.50 * (X + Y - 40))))
    Z_H = min(80, max(60, (60 + 0.50 * (X + Y - 60))))
    Z_HH = min(100, max(80, (80 + 0.50 * (X + Y - 80))))

    def clip(val):
        return max(0.0, min(100.0, val))

    consequent_outputs = {
        "LL": clip(Z_LL),
        "L": clip(Z_L),
        "M": clip(Z_M),
        "H": clip(Z_H),
        "HH": clip(Z_HH),
    }

    rule_matrix = {
        ("LL", "LL"): "LL", ("LL", "L"): "LL", ("LL", "M"): "L",  ("LL", "H"): "M",  ("LL", "HH"): "M",
        ("L", "LL"): "LL",  ("L", "L"): "L",   ("L", "M"): "M",  ("L", "H"): "H",   ("L", "HH"): "H",
        ("M", "LL"): "L",   ("M", "L"): "L",   ("M", "M"): "M",  ("M", "H"): "H",   ("M", "HH"): "H",
        ("H", "LL"): "L",   ("H", "L"): "M",   ("H", "M"): "H",  ("H", "H"): "HH",  ("H", "HH"): "HH",
        ("HH", "LL"): "L",  ("HH", "L"): "M",  ("HH", "M"): "H", ("HH", "H"): "HH", ("HH", "HH"): "HH",
    }

    def gaussian(val, c, k):
        return math.exp(-((val - c) ** 2) / (2 * (k ** 2)))

    mu_X = {
        "LL": 1.0 if X <= 0 else gaussian(X, centers["LL"], spreads["LL"]),
        "L": gaussian(X, centers["L"], spreads["L"]),
        "M": gaussian(X, centers["M"], spreads["M"]),
        "H": gaussian(X, centers["H"], spreads["H"]),
        "HH": 1.0 if X >= 100 else gaussian(X, centers["HH"], spreads["HH"]),
    }

    mu_Y = {
        "LL": 1.0 if Y <= 0 else gaussian(Y, centers["LL"], spreads["LL"]),
        "L": gaussian(Y, centers["L"], spreads["L"]),
        "M": gaussian(Y, centers["M"], spreads["M"]),
        "H": gaussian(Y, centers["H"], spreads["H"]),
        "HH": 1.0 if Y >= 100 else gaussian(Y, centers["HH"], spreads["HH"]),
    }

    weighted_sum = 0.0
    total_weight = 0.0

    for x_class, x_mu in mu_X.items():
        for y_class, y_mu in mu_Y.items():
            weight = x_mu * y_mu
            rule_output_class = rule_matrix[(x_class, y_class)]
            z = consequent_outputs[rule_output_class]
            weighted_sum += weight * z
            total_weight += weight

    return 0.0 if total_weight == 0 else weighted_sum / total_weight


# ==========================================================
# PART 3: DIVIDEND CORRECTION FUNCTIONS
# ==========================================================
def clip_dividend(dividend):
    return np.clip(dividend, 0.0, 100.0)


def ideal_dividend(f1):
    return IDEAL_DIV_MIN + (IDEAL_DIV_MAX - IDEAL_DIV_MIN) * (f1 / 100.0)


def dividend_correction(dividend, ideal_div):
    sigma = np.where(dividend <= ideal_div, LEFT_SIGMA, RIGHT_SIGMA)
    gaussian = np.exp(-((dividend - ideal_div) ** 2) / (2 * sigma ** 2))
    return MAX_CORRECTION * (2 * gaussian - 1)


def dividend_weight(f1):
    sigmoid = 1.0 / (1.0 + np.exp(-SIGMOID_STEEPNESS * (f1 - SIGMOID_MIDPOINT)))
    return sigmoid ** SIGMOID_POWER


def calculate_f_score(f1, dividend):
    dividend = clip_dividend(dividend)
    ideal = ideal_dividend(f1)
    correction = dividend_correction(dividend, ideal)
    weight = dividend_weight(f1)
    f_score = f1 + weight * correction
    return np.clip(f_score, 0, 100)


# ==========================================================
# MAIN EXECUTION PIPELINE
# ==========================================================
def run_fundamental_scoring():
    print("Connecting to Firebase Realtime Database...")
    init_firebase()

    ref = db.reference(FIREBASE_TARGET_NODE)
    data = ref.get()

    if not data:
        print(f"Error: No data found at Firebase node '/{FIREBASE_TARGET_NODE}'.")
        return

    print("Fetched records from Firebase. Loading into DataFrame...")
    df = pd.DataFrame(data)

    # 1. Coerce input columns to numeric
    input_cols = [
        "roe-0", "roe-1", "roe-3y",
        "roa-0", "roa-1", "roa-3y",
        DIV_COLUMN
    ]
    for col in input_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    # 2. Derive roe-c and roa-c as internal series (not appended as columns)
    roe_c = df.apply(
        lambda r: calculate_3pt_consensus(r["roe-0"], r["roe-1"], r["roe-3y"]),
        axis=1
    ).round(2)

    roa_c = df.apply(
        lambda r: calculate_3pt_consensus(r["roa-0"], r["roa-1"], r["roa-3y"]),
        axis=1
    ).round(2)

    # 3. Piecewise mapping normalization
    x_A = [-1e9, 0, 5, 10, 15, 20, 25, 30, 1e9]
    y_A = [0, 0, 20, 35, 50, 85, 95, 100, 100]
    x_B = [-1e9, 0, 1, 2, 5, 10, 15, 20, 1e9]
    y_B = [0, 0, 20, 30, 50, 80, 90, 100, 100]

    a_norm = roe_c.apply(lambda v: np.interp(v, x_A, y_A))
    b_norm = roa_c.apply(lambda v: np.interp(v, x_B, y_B))

    # 4. Fuzzy inference engine for preliminary score F1
    f1_scores = [sugeno_fuzzy_engine_5x5(x, y) for x, y in zip(a_norm, b_norm)]
    f1_series = pd.Series(f1_scores, index=df.index).fillna(0)

    # 5. Dividend correction
    div_series = df[DIV_COLUMN].fillna(0)
    final_f_scores = calculate_f_score(f1_series, div_series)

    # 6. Save only the single final target column
    df[OUTPUT_COLUMN] = final_f_scores.round(2)

    # Strictly drop any intermediate consensus columns if present
    df.drop(columns=["roe-c", "roa-c"], inplace=True, errors="ignore")

    # 7. Clean and serialize to Firebase
    cleaned_df = df.replace([np.inf, -np.inf], np.nan)
    cleaned_df = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)

    records = cleaned_df.to_dict(orient="records")

    print(f"Writing records with only '{OUTPUT_COLUMN}' added back to Firebase node '/{FIREBASE_TARGET_NODE}'...")
    ref.set(records)
    print(f"[OK] Success! Single column '{OUTPUT_COLUMN}' updated in Firebase.")


if __name__ == "__main__":
    try:
        run_fundamental_scoring()
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")