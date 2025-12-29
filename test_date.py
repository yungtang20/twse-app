import sqlite3
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'

# 模擬 get_latest_market_date 函式
def get_latest_market_date():
    """取得市場最新交易日 (從資料庫)"""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT MAX(date_int) FROM stock_history")
        result = cur.fetchone()
        conn.close()
        
        if result and result[0]:
            date_int = result[0]
            # 轉換為 YYYY-MM-DD 格式
            date_str = str(date_int)
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        
        # Fallback: 今天
        return datetime.now().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error: {e}")
        return datetime.now().strftime("%Y-%m-%d")

# 測試
result = get_latest_market_date()
print(f"get_latest_market_date() returned: {result}")

# 再次檢查 DB
conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("SELECT MAX(date_int) as max_date, MIN(date_int) as min_date FROM stock_history")
max_date, min_date = cur.fetchone()
print(f"DB MAX date_int: {max_date}")
print(f"DB MIN date_int: {min_date}")
conn.close()
