import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # 檢查 stock_snapshot 的 date 欄位
    cur.execute("SELECT MAX(date), MIN(date), COUNT(*) FROM stock_snapshot")
    max_date, min_date, count = cur.fetchone()
    print(f"stock_snapshot:")
    print(f"  MAX date: {max_date}")
    print(f"  MIN date: {min_date}")
    print(f"  Count: {count}")
    
    # 列出前幾筆
    cur.execute("SELECT code, date FROM stock_snapshot ORDER BY date DESC LIMIT 5")
    rows = cur.fetchall()
    print(f"\nTop 5 by date:")
    for r in rows:
        print(f"  {r[0]}: {r[1]}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
