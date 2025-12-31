import sqlite3
try:
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in cur.fetchall()]
    print(f"Tables: {tables}")
    
    if 'stock_meta' not in tables:
        print("MISSING: stock_meta table")
    else:
        print("FOUND: stock_meta table")
        
    if 'stock_snapshot' not in tables:
        print("MISSING: stock_snapshot table")
    else:
        print("FOUND: stock_snapshot table")

except Exception as e:
    print(f"Error: {e}")
