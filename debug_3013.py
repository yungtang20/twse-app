import sqlite3
import os

db_path = 'd:/twse/taiwan_stock.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Checking stock_history for 3013 on 20251224:")
cursor.execute("SELECT * FROM stock_history WHERE code='3013' AND date_int=20251224")
rows = cursor.fetchall()
if rows:
    print(f"Found {len(rows)} rows:")
    for row in rows:
        print(row)
else:
    print("No data found for 3013 on 20251224")

print("\nChecking stock_history for 2330 on 20251224:")
cursor.execute("SELECT * FROM stock_history WHERE code='2330' AND date_int=20251224")
rows = cursor.fetchall()
if rows:
    print(f"Found {len(rows)} rows:")
    for row in rows:
        print(row)
else:
    print("No data found for 2330 on 20251224")

print("\nChecking stock_meta for 3013:")
cursor.execute("SELECT * FROM stock_meta WHERE code='3013'")
rows = cursor.fetchall()
if rows:
    print(f"Found meta: {rows}")
else:
    print("No meta found for 3013")

conn.close()
