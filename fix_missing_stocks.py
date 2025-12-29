"""
補充上市時間足夠但資料不足的股票
"""
import sys
sys.path.insert(0, 'd:\\twse')

import sqlite3
import time
from datetime import datetime, timedelta

db_path = 'd:\\twse\\taiwan_stock.db'

# 需要補充的股票 (ETF 和 DR 需要特殊處理)
stocks_to_fix = [
    '0050', '0051', '0052', '0053', '0055', '0056', '0057', '0061',
    '4530', '8291', '9103', '9105', '9110', '9136', '6949'
]

print("需要補充的股票:")
for code in stocks_to_fix:
    print(f"  {code}")

print("\n" + "="*60)
print("開始補充資料...")
print("="*60)

# 導入 twstock
try:
    import twstock
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    for code in stocks_to_fix:
        print(f"\n處理 {code}...")
        
        try:
            stock = twstock.Stock(code)
            
            # 取得最近 2 年資料
            end_date = datetime.now()
            
            # twstock.fetch_from 需要 year, month
            for year_offset in range(2, -1, -1):  # 從 2 年前開始
                for month in range(1, 13):
                    target_year = end_date.year - year_offset
                    if target_year == end_date.year and month > end_date.month:
                        continue
                    
                    try:
                        data = stock.fetch(target_year, month)
                        if data:
                            for row in data:
                                date_int = int(row.date.strftime('%Y%m%d'))
                                cur.execute("""
                                    INSERT OR IGNORE INTO stock_history 
                                    (code, date_int, open, high, low, close, volume, amount)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    code, date_int,
                                    row.open, row.high, row.low, row.close,
                                    row.capacity, row.turnover
                                ))
                            conn.commit()
                            print(f"  {target_year}/{month:02d}: {len(data)} 筆")
                        
                        time.sleep(1)  # 避免太快
                    except Exception as e:
                        print(f"  {target_year}/{month:02d}: 錯誤 - {e}")
                        time.sleep(2)
        
        except Exception as e:
            print(f"  無法取得 {code}: {e}")
        
        time.sleep(2)
    
    conn.close()
    print("\n完成！")

except ImportError:
    print("twstock 未安裝，請先安裝: pip install twstock")
