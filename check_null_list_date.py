import sqlite3

DB_PATH = 'd:/twse/taiwan_stock.db'

def check_null_list_date():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Checking for stocks with NULL list_date and < 450 records...")
    
    cursor.execute("""
        SELECT h.code, m.name, COUNT(*) as cnt 
        FROM stock_history h 
        LEFT JOIN stock_meta m ON h.code = m.code 
        WHERE (m.list_date IS NULL OR m.list_date = '') 
          AND LENGTH(h.code)=4 
          AND h.code GLOB '[0-9][0-9][0-9][0-9]' 
        GROUP BY h.code 
        HAVING cnt < 450
    """)
    rows = cursor.fetchall()
    
    if rows:
        print(f"Found {len(rows)} stocks with NULL list_date:")
        for r in rows:
            print(r)
    else:
        print("No stocks found with NULL list_date and < 450 records.")

    conn.close()

if __name__ == "__main__":
    check_null_list_date()
