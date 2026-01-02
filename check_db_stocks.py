import sqlite3
import os

db_path = r"d:\twse\taiwan_stock.db"
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

codes = ['8093', '5515']
cursor.execute("SELECT code, name FROM stock_meta WHERE code IN (?, ?)", codes)
rows = cursor.fetchall()

print("Stock Meta in Database:")
for row in rows:
    print(f"Code: {row[0]}, Name: {row[1]}")

conn.close()
