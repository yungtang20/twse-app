import sys
import os
import sqlite3
import datetime

# Mock necessary components
class MockDBManager:
    def get_connection(self):
        return sqlite3.connect('taiwan_stock.db')

db_manager = MockDBManager()

def print_flush(msg):
    print(msg)
    sys.stdout.flush()

def get_latest_market_date():
    return "2024-12-20"

MIN_DATA_COUNT = 450

# Copy the modified function here to test it in isolation (or import if possible, but import might trigger other things)
# For safety, I will just run the logic against the DB directly in this script to verify the output format matches what I expect.

def test_logic():
    print("Testing logic...")
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    # Debug: Check total count first
    cur.execute("SELECT COUNT(*) FROM stock_history")
    print(f"Debug Total Count: {cur.fetchone()[0]}")

    sql = """
        SELECT code, 
               COUNT(*) as total_cnt,
               SUM(CASE WHEN volume > 0 AND (amount IS NULL OR amount = 0) THEN 1 ELSE 0 END) as missing_amount_cnt,
               MIN(date_int) as min_date
        FROM stock_history 
        GROUP BY code
    """
    cur.execute(sql)
    rows = cur.fetchall()
    
    total_records = 0
    total_stocks = len(rows)
    min_db_date = 99999999
    max_db_date = 0
    
    for r in rows:
        total = r[1]
        min_date_int = r[3]
        total_records += total
        if min_date_int and min_date_int < min_db_date:
            min_db_date = min_date_int
            
    print(f"Total Stocks: {total_stocks}")
    print(f"Total Records: {total_records}")
    if total_stocks > 0:
        print(f"Avg Days: {total_records // total_stocks}")
    print(f"Min Date: {min_db_date}")

if __name__ == "__main__":
    test_logic()
