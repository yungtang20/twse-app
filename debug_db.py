import sqlite3
import os

db_path = 'd:/twse/taiwan_stock.db'
if not os.path.exists(db_path):
    print(f"Error: DB not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Check 1240 missing amount
print("Checking 1240 missing amount:")
cur.execute("SELECT code, date_int, amount, volume FROM stock_history WHERE code='1240' AND (amount IS NULL OR amount=0) ORDER BY date_int DESC LIMIT 10")
rows = cur.fetchall()
for r in rows:
    print(r)

conn.close()
