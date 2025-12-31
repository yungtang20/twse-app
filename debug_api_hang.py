import sys
import os
import time
from pathlib import Path

# Add root directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.services.db import db_manager, get_system_status, get_cloud_status

def test_functions():
    print("Testing get_system_status()...")
    start = time.time()
    try:
        status = get_system_status()
        print(f"get_system_status result: {status}")
    except Exception as e:
        print(f"get_system_status failed: {e}")
    print(f"Time taken: {time.time() - start:.4f}s")
    
    print("\nTesting get_cloud_status()...")
    start = time.time()
    try:
        # Ensure we are NOT connected initially
        print(f"Initial supabase state: {db_manager.supabase}")
        status = get_cloud_status()
        print(f"get_cloud_status result: {status}")
    except Exception as e:
        print(f"get_cloud_status failed: {e}")
    print(f"Time taken: {time.time() - start:.4f}s")

if __name__ == "__main__":
    test_functions()
