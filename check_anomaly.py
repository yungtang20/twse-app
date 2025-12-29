import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # 檢查異常的 date_int (小於 20000000 或大於 20300000)
    cur.execute("SELECT code, date_int FROM stock_history WHERE date_int < 20000000 OR date_int > 20300000 ORDER BY date_int LIMIT 20")
    anomalies = cur.fetchall()
    
    if anomalies:
        print(f"Anomalies found ({len(anomalies)} rows):")
        for r in anomalies:
            print(f"  {r[0]}: {r[1]}")
    else:
        print("No anomalies found.")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
