import os
import subprocess
import sys

# =====================================================================
# CONFIGURATION BLOCK
# =====================================================================
# Pipeline sequence: Ingestion & PK derivation -> Metric derivations -> Scoring
scripts = [
    "BASIC.py",                # 1. Copies screener.csv, sets primary key CODE, saves to Firebase
    "DPE-DPB-MCAP.py",         # 2. Computes DPB%, DPE%, LGCAP, PCCAP
    "GROWTH-SCORE.py",         # 3. Computes and attaches G-score
    "FUNDAMENTAL-SCORE.py",    # 4. Computes and attaches F-score
    "TECHNICAL-SCORE.py"       # 5. Computes and attaches T-score
]
# =====================================================================

def run_script(script_name):
    print(f"\n{'='*60}")
    print(f">>> Executing: {script_name}")
    print(f"{'='*60}")

    if not os.path.exists(script_name):
        print(f"[ERROR] Script file '{script_name}' not found in current directory.")
        sys.exit(1)

    # Force child processes to execute with UTF-8 encoding to prevent Windows cp1252 errors
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    process = subprocess.Popen(
        [sys.executable, script_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
        errors="replace",
        env=env
    )

    for line in iter(process.stdout.readline, ''):
        print(line, end="")

    process.stdout.close()
    return_code = process.wait()

    if return_code != 0:
        print(f"\n[ERROR] Execution halted! Error occurred inside '{script_name}' (Exit Code: {return_code})")
        sys.exit(return_code)

    print(f"[OK] Completed successfully: {script_name}")

def main():
    print("Starting Screener ETL & Scoring Master Pipeline...")
    total_steps = len(scripts)

    for idx, script in enumerate(scripts, start=1):
        print(f"\n[Step {idx}/{total_steps}]")
        run_script(script)

    print(f"\n{'='*60}")
    print(f"[OK] All {total_steps} stages completed successfully! Firebase /SCREENER updated.")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()