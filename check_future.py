
import sqlite3
import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
today_int = 20251226

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("SELECT MAX(date_int) FROM stock_history")
    max_date = cur.fetchone()[0]
    
    print(f"Max date in DB: {max_date}")
    print(f"Today: {today_int}")
    
    if max_date and max_date > today_int:
        print(f"Found future data! Max date: {max_date}")
        cur.execute("SELECT code, date_int FROM stock_history WHERE date_int > ?", (today_int,))
        rows = cur.fetchall()
        print(f"Future rows count: {len(rows)}")
        for r in rows[:5]:
            print(r)
    else:
        print("No future data found.")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
