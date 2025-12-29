import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.routers.scan import scan_kd_cross, scan_vsbc, scan_2560

async def test_scans():
    print("--- Testing 2560 ---")
    try:
        res = await scan_2560(limit=5)
        print(f"Success: {res['success']}, Count: {res['data']['count']}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- Testing KD Cross ---")
    try:
        res = await scan_kd_cross(limit=5)
        print(f"Success: {res['success']}, Count: {res['data']['count']}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- Testing VSBC ---")
    try:
        res = await scan_vsbc(limit=5)
        print(f"Success: {res['success']}, Count: {res['data']['count']}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_scans())
