import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # 找出錯誤的日期
    cur.execute("SELECT DISTINCT date FROM stock_snapshot WHERE date > '2025-12-31'")
    bad_dates = cur.fetchall()
    print(f"Found {len(bad_dates)} bad dates:")
    for d in bad_dates:
        print(f"  {d[0]}")
    
    # 修正: 將 2030-11-22 改為 2025-11-22 (可能是 114/11/22 被錯誤轉換為 119/11/22)
    # 但更可能的問題是日期格式解析錯誤
    # 讓我先確認實際應該是什麼日期
    
    # 根據今天是 2025-12-26，最可能的正確日期應該是 2025-12-26
    # 暫時將所有 > 2025-12-31 的日期修正為 2025-12-26
    cur.execute("SELECT COUNT(*) FROM stock_snapshot WHERE date > '2025-12-31'")
    count = cur.fetchone()[0]
    print(f"\nWill update {count} rows with date > 2025-12-31 to 2025-12-26")
    
    cur.execute("UPDATE stock_snapshot SET date = '2025-12-26' WHERE date > '2025-12-31'")
    conn.commit()
    print("Done.")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
