import sqlite3
import datetime

def clean_future_dates():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    # 設定合理的未來界限 (例如: 明年年底)
    limit_date = 20251231
    
    print(f"正在檢查大於 {limit_date} 的異常日期...")
    cur.execute("SELECT COUNT(*) FROM stock_history WHERE date_int > ?", (limit_date,))
    count = cur.fetchone()[0]
    
    if count > 0:
        print(f"發現 {count} 筆異常未來資料，正在刪除...")
        cur.execute("DELETE FROM stock_history WHERE date_int > ?", (limit_date,))
        conn.commit()
        print(f"已刪除 {cur.rowcount} 筆資料")
    else:
        print("未發現異常未來資料")
        
    conn.close()

if __name__ == "__main__":
    clean_future_dates()
