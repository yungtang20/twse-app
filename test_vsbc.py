import importlib.util
import sys
from unittest.mock import patch
import os

# Set working directory to d:\twse to ensure DB is found
os.chdir("d:/twse")

# Load module
spec = importlib.util.spec_from_file_location("final_fix", "d:/twse/最終修正.py")
module = importlib.util.module_from_spec(spec)
sys.modules["final_fix"] = module
spec.loader.exec_module(module)

# Mock input to return limit=5, min_vol=100
print("Running VSBC Strategy Test...")
with patch('final_fix.get_user_scan_params', return_value=(5, 100)):
    module.scan_vsbc_strategy()
