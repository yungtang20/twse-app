import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # 刪除異常的 date_int (小於 20000000 表示民國年格式錯誤)
    cur.execute("SELECT COUNT(*) FROM stock_history WHERE date_int < 20000000")
    count = cur.fetchone()[0]
    print(f"Found {count} rows with anomalous date_int < 20000000")
    
    if count > 0:
        cur.execute("DELETE FROM stock_history WHERE date_int < 20000000")
        conn.commit()
        print(f"Deleted {count} rows.")
    
    conn.close()
    print("Done.")
except Exception as e:
    print(f"Error: {e}")
