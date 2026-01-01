import sys
import os
from pathlib import Path

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from services.db import db_manager, get_stock_history

def main():
    print("Testing get_stock_history('0000') in simulated cloud mode...")
    
    # Force cloud mode
    db_manager.is_cloud_mode = True
    db_manager.connect_supabase()
    
    if not db_manager.supabase:
        print("Failed to connect to Supabase")
        return

    print("Fetching history with limit=2000...")
    history = get_stock_history('0000', limit=2000)
    
    if history:
        print(f"Success! Found {len(history)} records.")
        print(f"First record: {history[0]}")
    else:
        print("Failed! Returned empty list.")

if __name__ == "__main__":
    main()
