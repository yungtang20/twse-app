import sqlite3

DB_PATH = "d:\\twse\\taiwan_stock.db"

def main():
    print(f"Connecting to local DB: {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    print("\nChecking stock_history for '0000'...")
    cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = '0000'")
    count = cur.fetchone()[0]
    print(f"Found {count} records for 0000 in stock_history.")
    
    if count > 0:
        cur.execute("SELECT * FROM stock_history WHERE code = '0000' ORDER BY date_int DESC LIMIT 1")
        print(f"Latest record: {dict(zip([c[0] for c in cur.description], cur.fetchone()))}")

    conn.close()

if __name__ == "__main__":
    main()
