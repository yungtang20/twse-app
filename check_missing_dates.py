import sqlite3
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'

# 要檢查的日期
dates_to_check = [20251225, 20251222, 20251219, 20251218, 20251024]

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    print("=== 檢查 institutional_investors 表 ===")
    for d in dates_to_check:
        cur.execute("SELECT COUNT(*) FROM institutional_investors WHERE date_int = ?", (d,))
        count = cur.fetchone()[0]
        print(f"  {d}: {count} 筆")
    
    print("\n=== 檢查 stock_history 表 ===")
    for d in dates_to_check:
        cur.execute("SELECT COUNT(*) FROM stock_history WHERE date_int = ?", (d,))
        count = cur.fetchone()[0]
        print(f"  {d}: {count} 筆")
    
    print("\n=== 檢查 margin_data 表 ===")
    for d in dates_to_check:
        cur.execute("SELECT COUNT(*) FROM margin_data WHERE date_int = ?", (d,))
        count = cur.fetchone()[0]
        print(f"  {d}: {count} 筆")
    
    # 檢查這些日期是否為交易日 (週末檢查)
    print("\n=== 檢查是否為交易日 (週末檢查) ===")
    for d in dates_to_check:
        dt = datetime.strptime(str(d), "%Y%m%d")
        weekday = dt.weekday()
        day_name = ["一", "二", "三", "四", "五", "六", "日"][weekday]
        is_weekend = weekday >= 5
        print(f"  {d}: 週{day_name} {'(休市)' if is_weekend else ''}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
