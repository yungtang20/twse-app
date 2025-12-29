"""
使用 FinMind 補充 6949 沛爾生技的歷史資料
"""
import sqlite3
import requests
from datetime import datetime, timedelta

db_path = 'd:\\twse\\taiwan_stock.db'

# FinMind API
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN = ""  # 公開 API 不需要 token

def fetch_finmind_history(code, start_date, end_date):
    """從 FinMind 取得歷史資料"""
    params = {
        'dataset': 'TaiwanStockPrice',
        'data_id': code,
        'start_date': start_date,
        'end_date': end_date,
    }
    if FINMIND_TOKEN:
        params['token'] = FINMIND_TOKEN
    
    try:
        resp = requests.get(FINMIND_URL, params=params, timeout=30)
        data = resp.json()
        
        if data.get('status') != 200:
            print(f"  ⚠ FinMind 錯誤: {data.get('msg', 'Unknown')}")
            return []
        
        records = []
        for row in data.get('data', []):
            date_int = int(row['date'].replace('-', ''))
            records.append((
                code, date_int,
                row.get('open'), row.get('max'), row.get('min'), row.get('close'),
                row.get('Trading_Volume'), row.get('Trading_money')
            ))
        return records
    except Exception as e:
        print(f"  ⚠ FinMind 錯誤: {e}")
        return []

print("="*60)
print("使用 FinMind 補充 6949 沛爾生技歷史資料")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 取得 6949 目前的資料範圍
cur.execute("SELECT MIN(date_int), MAX(date_int), COUNT(*) FROM stock_history WHERE code = '6949'")
min_d, max_d, cnt = cur.fetchone()
print(f"目前資料: {min_d} ~ {max_d}, 共 {cnt} 筆")

# 補充資料：從上市日 2024-03-08 開始
start_date = "2024-03-08"
end_date = datetime.now().strftime("%Y-%m-%d")

print(f"\n補充範圍: {start_date} ~ {end_date}")

records = fetch_finmind_history("6949", start_date, end_date)
print(f"FinMind 取得: {len(records)} 筆")

if records:
    cur.executemany("""
        INSERT OR REPLACE INTO stock_history 
        (code, date_int, open, high, low, close, volume, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"✓ 已更新 {len(records)} 筆")

# 驗證
cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = '6949'")
new_cnt = cur.fetchone()[0]
print(f"\n更新後: {new_cnt} 筆 (新增 {new_cnt - cnt} 筆)")

conn.close()
print("\n完成！")
