import sqlite3
import pandas as pd
from datetime import datetime

def check_8932():
    db_path = 'd:/twse/taiwan_stock.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Checking 8932...")
    
    # Check listing date
    cursor.execute("SELECT list_date FROM stock_meta WHERE code='8932'")
    row = cursor.fetchone()
    print(f"Listing Date: {row}")
    
    # Check missing amount dates
    cursor.execute("SELECT date_int, open, close, volume, amount FROM stock_history WHERE code='8932' AND (amount IS NULL OR amount = 0) ORDER BY date_int")
    rows = cursor.fetchall()
    print(f"Found {len(rows)} dates with missing amount:")
    for r in rows:
        print(r)
        
    # Check total count
    cursor.execute("SELECT COUNT(*) FROM stock_history WHERE code='8932'")
    count = cursor.fetchone()[0]
    print(f"Total history count: {count}")

    # Check recent data to see if amount is present
    cursor.execute("SELECT date_int, amount FROM stock_history WHERE code='8932' ORDER BY date_int DESC LIMIT 5")
    print("Recent 5 records:")
    for r in cursor.fetchall():
        print(r)

    conn.close()

if __name__ == "__main__":
    check_8932()
