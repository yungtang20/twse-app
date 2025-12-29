"""
手動補充法人資料
"""
import sys
sys.path.insert(0, 'd:\\twse')

import requests
import sqlite3
import time
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'

def safe_int(val):
    """安全轉整數"""
    if val is None:
        return 0
    try:
        return int(str(val).replace(',', '').replace('--', '0').strip())
    except:
        return 0

def fetch_twse_institutional(date_str):
    """從 TWSE 抓取法人資料"""
    url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        resp = requests.get(url, headers=headers, timeout=20, verify=False)
        data = resp.json()
        
        if data.get('stat') != 'OK' or 'data' not in data:
            return []
        
        results = []
        date_int = int(date_str)
        
        for row in data['data']:
            try:
                code = str(row[0]).strip().replace('=', '').replace('"', '')
                if not code.isdigit() or len(code) > 4:
                    continue
                
                results.append((
                    code, date_int,
                    safe_int(row[2]), safe_int(row[3]),  # 外資買/賣
                    safe_int(row[8]), safe_int(row[9]),  # 投信買/賣
                    safe_int(row[12]), safe_int(row[13])  # 自營商買/賣
                ))
            except:
                pass
        
        return results
    except Exception as e:
        print(f"TWSE Error: {e}")
        return []

def fetch_tpex_institutional(date_str):
    """從 TPEx 抓取法人資料"""
    # 轉換日期格式
    dt = datetime.strptime(date_str, "%Y%m%d")
    roc_date = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
    
    url = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d={roc_date}&o=json"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        resp = requests.get(url, headers=headers, timeout=20, verify=False)
        data = resp.json()
        
        tables = data.get('tables', [])
        if not tables:
            return []
        
        results = []
        date_int = int(date_str)
        
        table_data = tables[0].get('data', [])
        for row in table_data:
            try:
                code = str(row[0]).strip()
                if len(code) != 4:
                    continue
                
                # TPEx 格式: 外資買/賣在 8/9, 投信在 11/12, 自營商在 20/21
                results.append((
                    code, date_int,
                    safe_int(row[8]), safe_int(row[9]),
                    safe_int(row[11]), safe_int(row[12]),
                    safe_int(row[20]), safe_int(row[21])
                ))
            except:
                pass
        
        return results
    except Exception as e:
        print(f"TPEx Error: {e}")
        return []

# 要補充的日期 (排除 20251225 休市日)
dates = ['20251222', '20251219', '20251218', '20251024']

conn = sqlite3.connect(db_path)
cur = conn.cursor()

for date_str in dates:
    print(f"\n處理 {date_str}...")
    
    # 先檢查是否已有資料
    cur.execute("SELECT COUNT(*) FROM institutional_investors WHERE date_int = ?", (int(date_str),))
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"  已有 {existing} 筆資料，跳過")
        continue
    
    # 抓取 TWSE
    print(f"  抓取 TWSE...")
    twse_data = fetch_twse_institutional(date_str)
    print(f"  TWSE: {len(twse_data)} 筆")
    
    time.sleep(1)
    
    # 抓取 TPEx
    print(f"  抓取 TPEx...")
    tpex_data = fetch_tpex_institutional(date_str)
    print(f"  TPEx: {len(tpex_data)} 筆")
    
    # 合併並寫入
    all_data = twse_data + tpex_data
    if all_data:
        cur.executemany("""
            INSERT OR REPLACE INTO institutional_investors 
            (code, date_int, foreign_buy, foreign_sell, trust_buy, trust_sell, dealer_buy, dealer_sell)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, all_data)
        conn.commit()
        print(f"  ✓ 已寫入 {len(all_data)} 筆")
    else:
        print(f"  ⚠ 無資料")
    
    time.sleep(2)

conn.close()
print("\n完成！")
