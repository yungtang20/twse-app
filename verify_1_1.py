import sys
import os
import importlib.util

# Set encoding to utf-8
sys.stdout.reconfigure(encoding='utf-8')

file_path = r'd:\twse\最終修正.py'
module_name = '最終修正'

print(f"Loading {file_path}...")
spec = importlib.util.spec_from_file_location(module_name, file_path)
module = importlib.util.module_from_spec(spec)
sys.modules[module_name] = module
spec.loader.exec_module(module)

print("Starting _run_full_daily_update()...")
try:
    module._run_full_daily_update()
    print("\n_run_full_daily_update() completed successfully.")
except Exception as e:
    print(f"\n_run_full_daily_update() failed: {e}")
    import traceback
    traceback.print_exc()
