import sqlite3
import json

DB_PATH = "d:\\twse\\taiwan_stock.db"

def main():
    print(f"Connecting to local DB: {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        print("Checking columns in stock_snapshot...")
        cur.execute("PRAGMA table_info(stock_snapshot)")
        columns = [row[1] for row in cur.fetchall()]
        for col in columns:
            print(col)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
