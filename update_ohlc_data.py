"""
Update Stock Snapshot with OHLC Data
"""
import sqlite3
import pandas as pd
from backend.services.db import db_manager

def add_ohlc_columns():
    """Add OHLC columns to stock_snapshot if they don't exist"""
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    
    columns = ['open', 'high', 'low']
    existing_cols = [row[1] for row in cursor.execute("PRAGMA table_info(stock_snapshot)")]
    
    for col in columns:
        if col not in existing_cols:
            print(f"Adding column {col}...")
            cursor.execute(f"ALTER TABLE stock_snapshot ADD COLUMN {col} REAL")
    
    conn.commit()
    conn.close()

def update_ohlc_data():
    """Update OHLC data for all stocks from history"""
    print("Fetching stocks...")
    stocks = db_manager.execute_query("SELECT code FROM stock_snapshot WHERE code GLOB '[0-9][0-9][0-9][0-9]'")
    
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    
    count = 0
    total = len(stocks)
    
    print(f"Updating OHLC data for {total} stocks...")
    
    for stock in stocks:
        code = stock['code']
        
        # Get latest history
        history = db_manager.execute_query(
            "SELECT open, high, low FROM stock_history WHERE code = ? ORDER BY date_int DESC LIMIT 1",
            (code,)
        )
        
        if history:
            latest = history[0]
            cursor.execute(
                "UPDATE stock_snapshot SET open = ?, high = ?, low = ? WHERE code = ?",
                (latest['open'], latest['high'], latest['low'], code)
            )
            count += 1
            
        if count % 100 == 0:
            print(f"Processed {count}/{total}...")
            conn.commit()
            
    conn.commit()
    conn.close()
    print(f"Completed! Updated {count} stocks.")

if __name__ == "__main__":
    add_ohlc_columns()
    update_ohlc_data()
