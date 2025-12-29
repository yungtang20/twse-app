import sys
import os
os.chdir('d:\\twse')
sys.path.insert(0, 'd:\\twse')

# 直接載入模組
import importlib.util
spec = importlib.util.spec_from_file_location("twse_module", "d:\\twse\\最終修正.py")
module = importlib.util.module_from_spec(spec)

print("Loading module...")
try:
    spec.loader.exec_module(module)
    print("Module loaded successfully.")
    
    # 測試 get_latest_market_date
    result = module.get_latest_market_date()
    print(f"get_latest_market_date() returned: {result}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
