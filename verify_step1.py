import sys
import os
import sqlite3
import pandas as pd

# Add current directory to path
sys.path.append(os.getcwd())

from 最終修正 import step1_fetch_stock_list, db_manager

def run_verification():
    print("Running Step 1 to update stock_meta with enhanced logic...")
    try:
        step1_fetch_stock_list()
    except Exception as e:
        print(f"Step 1 failed: {e}")
        return

    print("\nVerifying stock_meta coverage...")
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_meta")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM stock_meta WHERE list_date IS NOT NULL AND list_date != ''")
        with_date = cur.fetchone()[0]
        
        print(f"Total Stocks: {total}")
        print(f"With Listing Date: {with_date}")
        print(f"Missing Date: {total - with_date}")
        
        # Check specific stock 1240
        cur.execute("SELECT * FROM stock_meta WHERE code='1240'")
        row = cur.fetchone()
        print(f"Stock 1240: {row}")

if __name__ == "__main__":
    run_verification()
