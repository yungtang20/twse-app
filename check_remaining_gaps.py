import sqlite3
from datetime import datetime, timedelta

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('資料完整性檢查報告')
print('='*60)

# 1. Volume 空值
print('\n【Volume 空值】')
cur.execute('''
    SELECT code, date_int 
    FROM stock_history 
    WHERE volume IS NULL 
    ORDER BY date_int DESC
''')
vol_nulls = cur.fetchall()
if not vol_nulls:
    print("  ✓ 無 Volume 空值")
else:
    print(f"  ⚠ 發現 {len(vol_nulls)} 筆 Volume 空值:")
    for code, d in vol_nulls:
        print(f"    - {code} 日期:{d}")

# 2. 資料不足股票 (<450筆)
print('\n【資料不足股票 (<450筆)】')
cur.execute('''
    SELECT h.code, m.name, m.list_date, COUNT(*) as cnt
    FROM stock_history h
    LEFT JOIN stock_meta m ON h.code = m.code
    WHERE LENGTH(h.code) = 4 
      AND h.code GLOB '[0-9][0-9][0-9][0-9]'
      AND CAST(h.code AS INTEGER) >= 1101
      AND CAST(h.code AS INTEGER) < 9000
    GROUP BY h.code
    HAVING cnt < 450
    ORDER BY cnt ASC
''')
rows = cur.fetchall()

cutoff_date = (datetime.now() - timedelta(days=630)).strftime("%Y-%m-%d")
missing_stocks = []

for code, name, list_date, cnt in rows:
    is_new = False
    if list_date and list_date >= cutoff_date:
        is_new = True
    elif list_date:
        try:
            ld = datetime.strptime(list_date, "%Y-%m-%d")
            days = (datetime.now() - ld).days
            expected = int(days * 5 / 7)
            ratio = cnt / expected if expected > 0 else 0
            if ratio >= 0.9:
                is_new = True
        except:
            pass
    
    if not is_new:
        missing_stocks.append((code, name, list_date, cnt))

if not missing_stocks:
    print("  ✓ 無需補歷史的股票")
else:
    print(f"  ⚠ 發現 {len(missing_stocks)} 支需補歷史:")
    for code, name, ld, cnt in missing_stocks:
        print(f"    - {code} {name} (上市:{ld}) 現有:{cnt}筆")

conn.close()
