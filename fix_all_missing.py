"""
補齊缺失資料
1. 3 支資料缺失股票 (4530, 8291, 6949)
2. Close 空值問題
3. 缺失日期的資料
"""
import sys
sys.path.insert(0, 'd:\\twse')

import sqlite3
import requests
import time
from datetime import datetime, timedelta
import twstock

db_path = 'd:\\twse\\taiwan_stock.db'

def safe_float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(',', ''))
    except:
        return None

def fetch_stock_history(code, year, month):
    """使用 twstock 抓取歷史資料"""
    try:
        stock = twstock.Stock(code)
        data = stock.fetch(year, month)
        
        if not data:
            return []
        
        records = []
        for row in data:
            date_int = int(row.date.strftime('%Y%m%d'))
            records.append((
                code, date_int,
                row.open, row.high, row.low, row.close,
                row.capacity, row.turnover
            ))
        return records
    except Exception as e:
        print(f"  ⚠ {code} {year}/{month}: {e}")
        return []

print("="*60)
print("補齊缺失資料")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# ========== 1. 補齊資料缺失股票 ==========
print("\n【1. 補齊資料缺失股票】")
missing_stocks = ['4530', '8291', '6949']

for code in missing_stocks:
    print(f"\n處理 {code}...")
    
    # 取得最近 2 年資料
    end_date = datetime.now()
    total_added = 0
    
    for year_offset in range(2, -1, -1):
        for month in range(1, 13):
            target_year = end_date.year - year_offset
            if target_year == end_date.year and month > end_date.month:
                continue
            
            records = fetch_stock_history(code, target_year, month)
            if records:
                cur.executemany("""
                    INSERT OR IGNORE INTO stock_history 
                    (code, date_int, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, records)
                conn.commit()
                total_added += len(records)
                print(f"  {target_year}/{month:02d}: {len(records)} 筆")
            
            time.sleep(1)
    
    print(f"  ✓ {code} 共新增 {total_added} 筆")

# ========== 2. 修復 Close 空值 ==========
print("\n【2. 修復 Close 空值】")

# 先看看有哪些空值
cur.execute("""
    SELECT code, date_int FROM stock_history 
    WHERE close IS NULL 
    ORDER BY date_int DESC 
    LIMIT 100
""")
null_records = cur.fetchall()
print(f"發現 {len(null_records)} 筆 close 空值")

# 按代碼分組
codes_to_fix = {}
for code, date_int in null_records:
    if code not in codes_to_fix:
        codes_to_fix[code] = []
    codes_to_fix[code].append(date_int)

for code, dates in codes_to_fix.items():
    print(f"\n修復 {code} ({len(dates)} 筆)...")
    
    # 取得這些日期的月份
    months = set()
    for d in dates:
        year = d // 10000
        month = (d % 10000) // 100
        months.add((year, month))
    
    for year, month in sorted(months):
        records = fetch_stock_history(code, year, month)
        if records:
            # 使用 REPLACE 更新
            cur.executemany("""
                INSERT OR REPLACE INTO stock_history 
                (code, date_int, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn.commit()
            print(f"  {year}/{month:02d}: 更新 {len(records)} 筆")
        
        time.sleep(1)

# ========== 3. 驗證結果 ==========
print("\n【3. 驗證結果】")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE close IS NULL")
remaining_nulls = cur.fetchone()[0]
print(f"剩餘 close 空值: {remaining_nulls}")

for code in missing_stocks:
    cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = ?", (code,))
    cnt = cur.fetchone()[0]
    print(f"  {code}: {cnt} 筆")

conn.close()
print("\n完成！")
