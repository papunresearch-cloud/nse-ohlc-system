"""
force_param_update.py
Directly executes parameter calculation for all stocks and indices currently in /param.
Bypasses trading calendar and watchlist filters.
"""
from firebase_admin import db
from firebase_manager import init_firebase
from parameter import compute_script_parameters

def force_run():
    init_firebase()
    
    # 1. Fetch all script keys currently residing in Firebase /param
    param_ref = db.reference("param")
    param_scripts = param_ref.get()
    
    if not param_scripts:
        print("No scripts found in /param. Falling back to /stocks...")
        stocks_ref = db.reference("stocks")
        scripts_to_update = list((stocks_ref.get() or {}).keys())
    else:
        scripts_to_update = list(param_scripts.keys())

    print(f"Targeting {len(scripts_to_update)} scripts for forced parameter update:")
    print(scripts_to_update)

    # 2. Compute parameters directly for each script
    for script in scripts_to_update:
        print(f"\nProcessing [{script}]...")
        try:
            result = compute_script_parameters(script)
            if result:
                print(f"--> Successfully updated /param/{script} | 1yr: {result.get('1yr')} | 3yr: {result.get('3yr')}")
            else:
                print(f"--> Skipped / Failed for [{script}]: No candle records.")
        except Exception as e:
            print(f"--> ERROR updating [{script}]: {e}")

    print("\nForced calculation complete.")

if __name__ == "__main__":
    force_run()