import time
from backend.routers.scan_2560 import execute_2560_scan

t0 = time.time()
result = execute_2560_scan(limit=5, min_vol=500)
elapsed = time.time() - t0

print(f"Time: {elapsed:.2f}s")
print(f"Count: {result['count']}")
print(f"Stocks: {[r['code'] for r in result['results']]}")
print("Process Log:")
for log in result['process_log']:
    print(f"  {log['step']}: {log['count']}")
