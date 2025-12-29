"""
檢查 2025-10-24 是否為假日
"""
from datetime import datetime

# 2025-10-24
dt = datetime(2025, 10, 24)
print(f"日期: 2025-10-24")
print(f"星期: {['一', '二', '三', '四', '五', '六', '日'][dt.weekday()]}")

# 台股假日
# 2025/10/24 應該是正常交易日
# 但是讓我們從 TWSE API 實際查詢看看

import requests
import time

# 測試 TWSE API
date_str = "20251024"
url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
headers = {'User-Agent': 'Mozilla/5.0'}

print(f"\n嘗試從 TWSE 抓取 {date_str} 法人資料...")
try:
    resp = requests.get(url, headers=headers, timeout=20, verify=False)
    data = resp.json()
    
    print(f"stat: {data.get('stat')}")
    print(f"date: {data.get('date')}")
    
    if 'data' in data:
        print(f"data 筆數: {len(data['data'])}")
    else:
        print("無 data 欄位")
        
    # 印出前 100 字元
    import json
    print(f"回應內容: {json.dumps(data, ensure_ascii=False)[:200]}")
except Exception as e:
    print(f"Error: {e}")

# 也測試 stock_history 有沒有 2025-10-24 的資料
import sqlite3
db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 檢查 2025-10-24 附近的日期
print(f"\n檢查 stock_history 中 2025-10 月的資料:")
cur.execute("SELECT DISTINCT date_int FROM stock_history WHERE date_int BETWEEN 20251020 AND 20251030 ORDER BY date_int")
for row in cur.fetchall():
    print(f"  {row[0]}")

conn.close()
