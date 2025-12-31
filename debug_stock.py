import sys
import os

# Add backend to path
sys.path.append(os.getcwd())

from backend.services.db import get_stock_by_code

try:
    print("Calling get_stock_by_code('2412')...")
    stock = get_stock_by_code('2412')
    print(f"Result: {stock}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
