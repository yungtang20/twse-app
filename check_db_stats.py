import sqlite3
import os

db_path = 'taiwan_stock.db'
if not os.path.exists(db_path):
    print("Database not found")
    exit()

conn = sqlite3.connect(db_path)
cur = conn.cursor()

try:
    cur.execute("SELECT COUNT(*) FROM stock_history")
    count_history = cur.fetchone()[0]
    print(f"stock_history count: {count_history}")

    cur.execute("SELECT COUNT(*) FROM stock_meta")
    count_meta = cur.fetchone()[0]
    print(f"stock_meta count: {count_meta}")

    cur.execute("SELECT COUNT(*) FROM stock_snapshot")
    count_snapshot = cur.fetchone()[0]
    print(f"stock_snapshot count: {count_snapshot}")
    
    # Check for duplicates in history
    # cur.execute("SELECT code, date_int, COUNT(*) FROM stock_history GROUP BY code, date_int HAVING COUNT(*) > 1")
    # dups = cur.fetchall()
    # print(f"Duplicate records: {len(dups)}")

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
