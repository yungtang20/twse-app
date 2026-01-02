import requests
import json

# Test against the deployed URL if possible, but I can't from here easily without curl.
# I'll test the local backend logic by importing the function.

import sys
import os
sys.path.append(os.getcwd())

from backend.services.db import db_manager, get_stock_history

# Force cloud mode
db_manager.is_cloud_mode = True
db_manager.connect_supabase()

print("Testing get_stock_history('0000') in CLOUD mode...")
try:
    history = get_stock_history('0000', limit=10)
    print(f"Got {len(history)} records.")
    if history:
        print(f"Sample: {history[0]}")
    else:
        print("❌ Returned empty list!")
except Exception as e:
    print(f"❌ Error: {e}")
