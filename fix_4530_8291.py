"""
處理 4530 和 8291
- 4530 宏易（已更名為宏易新境）：使用 FinMind 補充
- 8291 尚茂：已於 2023-11-21 下市，標記為下市
"""
import sqlite3
import requests
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def fetch_finmind_history(code, start_date, end_date):
    """從 FinMind 取得歷史資料"""
    params = {
        'dataset': 'TaiwanStockPrice',
        'data_id': code,
        'start_date': start_date,
        'end_date': end_date,
    }
    
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
print("處理 4530 和 8291")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# ========== 1. 補充 4530 宏易新境 ==========
print("\n【1. 補充 4530 宏易新境】")

# 取得目前資料
cur.execute("SELECT MIN(date_int), MAX(date_int), COUNT(*) FROM stock_history WHERE code = '4530'")
min_d, max_d, cnt = cur.fetchone()
print(f"目前資料: {min_d} ~ {max_d}, 共 {cnt} 筆")

# 使用 FinMind 補充（從 2022 年開始）
start_date = "2022-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")

print(f"補充範圍: {start_date} ~ {end_date}")
records = fetch_finmind_history("4530", start_date, end_date)
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
    cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = '4530'")
    new_cnt = cur.fetchone()[0]
    print(f"更新後: {new_cnt} 筆")
else:
    print("  ⚠ 無法取得資料")

# ========== 2. 標記 8291 尚茂為下市 ==========
print("\n【2. 標記 8291 尚茂為下市】")

# 更新 stock_meta 的 delist_date
cur.execute("""
    UPDATE stock_meta 
    SET delist_date = '2023-11-21', status = 'delisted' 
    WHERE code = '8291'
""")
conn.commit()
print(f"✓ 已更新 stock_meta (delist_date = 2023-11-21)")

# 檢查 8291 的資料
cur.execute("SELECT MIN(date_int), MAX(date_int), COUNT(*) FROM stock_history WHERE code = '8291'")
row = cur.fetchone()
print(f"8291 資料: {row[0]} ~ {row[1]}, 共 {row[2]} 筆")

# 8291 已下市，保留歷史資料但不再補充
print("  ℹ 8291 已下市，保留現有歷史資料，不再補充")

# ========== 3. 更新 6949 判斷邏輯 ==========
print("\n【3. 修正 6949 白名單判斷】")

# 檢查 6949 上市日期
cur.execute("SELECT list_date FROM stock_meta WHERE code = '6949'")
row = cur.fetchone()
if row:
    list_date = row[0]
    print(f"6949 上市日期: {list_date}")
    
    # 計算上市天數
    if list_date:
        ld = datetime.strptime(list_date, "%Y-%m-%d")
        days = (datetime.now() - ld).days
        print(f"上市天數: {days} 天 (約 {int(days*5/7)} 交易日)")
        
        if days < 630:
            print("✓ 應歸類為新上市白名單")
        else:
            print("⚠ 已超過 630 天，應正常檢查")

conn.close()
print("\n完成！")
