import sqlite3
import os

db_path = 'taiwan_stock.db'
if not os.path.exists(db_path):
    print("DB not found")
    exit(1)

conn = sqlite3.connect(db_path)
try:
    print("Searching for Index codes...")
    cursor = conn.execute("SELECT code, name FROM stock_meta WHERE name LIKE '%指數%' OR code='0000' OR code='IX0001'")
    found = False
    for row in cursor:
        print(f"Found: {row}")
        found = True
        # Check history
        c2 = conn.execute("SELECT COUNT(*) FROM stock_history WHERE code=?", (row[0],))
        count = c2.fetchone()[0]
        print(f"  History records: {count}")
    
    if not found:
        print("No index codes found in stock_meta.")

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
