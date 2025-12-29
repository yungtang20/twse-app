import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("SELECT MAX(date_int), MIN(date_int), COUNT(*) FROM stock_history")
    max_date, min_date, count = cur.fetchone()
    
    print(f"Max date_int: {max_date}")
    print(f"Min date_int: {min_date}")
    print(f"Total records: {count}")
    
    # 檢查是否有超過今天的資料
    today = 20251226
    cur.execute("SELECT code, date_int FROM stock_history WHERE date_int > ? ORDER BY date_int DESC LIMIT 10", (today,))
    future = cur.fetchall()
    if future:
        print(f"\nFuture data found ({len(future)} rows):")
        for r in future:
            print(f"  {r[0]}: {r[1]}")
    else:
        print("\nNo future data found.")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
