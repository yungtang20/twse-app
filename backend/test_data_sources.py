import sys
import os
import logging
import pandas as pd

# Add current directory to path to import backend modules
sys.path.append(os.getcwd())

from backend.data_sources import FinMindDataSource, OfficialAPIDataSource, DataSourceManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestDataSource")

class MockProgressTracker:
    def info(self, msg, level=0):
        print(f"[INFO] {msg}")
    def warning(self, msg, level=0):
        print(f"[WARN] {msg}")
    def success(self, msg, level=0):
        print(f"[SUCCESS] {msg}")

def test_finmind():
    print("\n=== Testing FinMindDataSource ===")
    tracker = MockProgressTracker()
    source = FinMindDataSource(tracker)
    
    # Test with a known stock (e.g., 2330 TSMC)
    df = source.fetch_history("2330", start_date="2024-12-01", end_date="2024-12-20")
    
    if df is not None and not df.empty:
        print(f"Successfully fetched {len(df)} records")
        print(df.head())
        return True
    else:
        print("Failed to fetch data")
        return False

def test_official():
    print("\n=== Testing OfficialAPIDataSource ===")
    tracker = MockProgressTracker()
    source = OfficialAPIDataSource(tracker)
    
    # Test with a known stock (e.g., 2330 TSMC)
    # Note: Official API might be slower due to random delay
    df = source.fetch_history("2330", start_date="2024-12-01", end_date="2024-12-20")
    
    if df is not None and not df.empty:
        print(f"Successfully fetched {len(df)} records")
        print(df.head())
        return True
    else:
        print("Failed to fetch data")
        return False

def test_manager():
    print("\n=== Testing DataSourceManager ===")
    tracker = MockProgressTracker()
    manager = DataSourceManager(tracker)
    
    # Test with a known stock
    df = manager.fetch_history("2330", start_date="2024-12-01", end_date="2024-12-20")
    
    if df is not None and not df.empty:
        print(f"Successfully fetched {len(df)} records via Manager")
        print(df.head())
        return True
    else:
        print("Failed to fetch data via Manager")
        return False

if __name__ == "__main__":
    # Run tests
    finmind_ok = test_finmind()
    official_ok = test_official()
    manager_ok = test_manager()
    
    print("\n=== Test Summary ===")
    print(f"FinMind: {'PASS' if finmind_ok else 'FAIL'}")
    print(f"Official: {'PASS' if official_ok else 'FAIL'}")
    print(f"Manager: {'PASS' if manager_ok else 'FAIL'}")
