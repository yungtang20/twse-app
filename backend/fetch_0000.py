import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime

# Add current directory to path
sys.path.append(os.getcwd())

from backend.data_sources import DataSourceManager
from backend.services.db import DB_PATH

def save_to_db(df, code):
    if df is None or df.empty:
        print("No data to save.")
        return

    print(f"Saving {len(df)} records to database...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Schema: code, name, list_date, delist_date, market_type
        cursor.execute(
            "INSERT OR IGNORE INTO stock_meta (code, name, market_type) VALUES (?, ?, ?)",
            (code, "加權指數", "TWSE")
        )
        
        # Upsert history
        for _, row in df.iterrows():
            date_int = int(row['date'].replace('-', ''))
            cursor.execute("""
                INSERT OR REPLACE INTO stock_history 
                (code, date_int, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code, 
                date_int,
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['amount']
            ))
            
        conn.commit()
        print("Save complete.")
        
    except Exception as e:
        print(f"Error saving to DB: {e}")
        conn.rollback()
    finally:
        conn.close()

class MockTracker:
    def info(self, msg, level=0): print(f"[INFO] {msg}")
    def warning(self, msg, level=0): print(f"[WARN] {msg}")
    def success(self, msg, level=0): print(f"[SUCCESS] {msg}")

def main():
    print("Fetching data for 0000 (Weighted Index)...")
    tracker = MockTracker()
    manager = DataSourceManager(progress_tracker=tracker)
    
    # Fetch last 3 years
    start_date = (datetime.now() - pd.Timedelta(days=1095)).strftime("%Y-%m-%d")
    
    # Try 0000 first
    df = manager.fetch_history("0000", start_date=start_date)
    
    if df is None or df.empty:
        print("0000 failed, trying TAIEX (FinMind code)...")
        df = manager.fetch_history("TAIEX", start_date=start_date)
    
    if df is not None and not df.empty:
        print("Data fetched successfully.")
        # Save as 0000 regardless of source code
        save_to_db(df, "0000")
    else:
        print("Failed to fetch data from all sources.")

if __name__ == "__main__":
    main()
