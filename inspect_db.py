import sqlite3
import sys

db_path = "d:/twse/taiwan_stock.db"
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Columns in stock_snapshot:")
    cursor.execute("PRAGMA table_info(stock_snapshot)")
    columns = cursor.fetchall()
    for col in columns:
        print(col[1])
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
