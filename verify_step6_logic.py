import sys
from unittest.mock import MagicMock

# Mock dependencies
sys.modules['twstock'] = MagicMock()
sys.modules['pandas'] = MagicMock()
sys.modules['requests'] = MagicMock()

# Mock global functions
def print_flush(msg):
    print(msg)

def _auto_fix_missing_amount(crawl=True):
    print(f"_auto_fix_missing_amount called with crawl={crawl}")

def step3_5_download_institutional(days=3):
    print(f"step3_5_download_institutional called")

def step3_6_download_major_holders():
    print(f"step3_6_download_major_holders called")

def step4_load_data():
    return {}

def load_progress():
    return {}

def save_progress(**kwargs):
    pass

class MockDBManager:
    def get_connection(self):
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.fetchall.return_value = []
        return mock_conn

db_manager = MockDBManager()

# Import the function to test (copy-paste modified version)
def step6_verify_and_backfill(data=None, resume=False, skip_downloads=False, skip_institutional=False):
    print(f"step6 called with skip_downloads={skip_downloads}, skip_institutional={skip_institutional}")
    
    _auto_fix_missing_amount(crawl=not skip_downloads)
    
    if not skip_downloads and not skip_institutional:
        step3_5_download_institutional(days=3)
        step3_6_download_major_holders()
    else:
        print("Skipping institutional downloads")

def main():
    print("Test 1: skip_downloads=True (Old behavior)")
    step6_verify_and_backfill(skip_downloads=True)
    print("-" * 20)
    
    print("Test 2: skip_downloads=False, skip_institutional=False (Normal behavior)")
    step6_verify_and_backfill(skip_downloads=False, skip_institutional=False)
    print("-" * 20)
    
    print("Test 3: skip_downloads=False, skip_institutional=True (New One-Click behavior)")
    step6_verify_and_backfill(skip_downloads=False, skip_institutional=True)
    print("-" * 20)

if __name__ == "__main__":
    main()
