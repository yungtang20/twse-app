import sys
sys.path.insert(0, 'backend')
from services.db import get_stock_history, get_index_history_from_yfinance

print("Testing get_index_history_from_yfinance...")
result = get_index_history_from_yfinance(30)
print(f"Got {len(result)} records")
if result:
    print("Sample:", result[0])

print("\nTesting get_stock_history('0000')...")
result2 = get_stock_history('0000', 30)
print(f"Got {len(result2)} records")
if result2:
    print("Sample:", result2[0])
