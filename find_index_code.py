import sqlite3

DB_PATH = "d:\\twse\\taiwan_stock.db"

def main():
    print(f"Connecting to local DB: {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    print("\nSearching for '加權指數' in stock_meta...")
    cur.execute("SELECT code, name FROM stock_meta WHERE name LIKE '%加權指數%' OR name LIKE '%指數%'")
    rows = cur.fetchall()
    for row in rows:
        print(f"Found: {row}")
        
    print("\nSearching for '0000' in stock_meta...")
    cur.execute("SELECT code, name FROM stock_meta WHERE code = '0000'")
    rows = cur.fetchall()
    if not rows:
        print("Code '0000' not found in stock_meta.")
        
    print("\nChecking first 5 rows of stock_snapshot...")
    cur.execute("SELECT code FROM stock_snapshot LIMIT 5")
    print(cur.fetchall())

    conn.close()

if __name__ == "__main__":
    main()
