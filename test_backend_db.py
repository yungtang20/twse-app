import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from backend.services.db import db_manager, get_all_stocks
    
    print(f"DB Path in Manager: {db_manager.db_path}")
    print(f"DB Exists: {db_manager.db_path.exists()}")
    
    stocks = get_all_stocks()
    print(f"Stocks found via backend service: {len(stocks)}")
    
    if len(stocks) > 0:
        print(f"First stock: {stocks[0]}")
        
except Exception as e:
    print(f"Error: {e}")
