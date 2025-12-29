import sqlite3
from datetime import datetime, timedelta

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('最終資料完整性檢查報告')
print('='*60)

# 1. 空值檢查
print('\n【1. 空值檢查 (OHLCV)】')
has_nulls = False
for col in ['open', 'high', 'low', 'close', 'volume']:
    cur.execute(f'SELECT COUNT(*) FROM stock_history WHERE {col} IS NULL')
    cnt = cur.fetchone()[0]
    if cnt > 0:
        print(f'  ⚠ {col}: {cnt} 筆空值')
        has_nulls = True
        
        # 列出前 5 筆
        cur.execute(f'SELECT code, date_int FROM stock_history WHERE {col} IS NULL LIMIT 5')
        rows = cur.fetchall()
        for r in rows:
            print(f'    - {r[0]} {r[1]}')
    else:
        print(f'  ✓ {col}: 0 空值')

if not has_nulls:
    print('  ✓ 所有 OHLCV 資料完整')

# 2. 資料不足股票檢查
print('\n【2. 資料不足股票 (<450筆)】')
# 篩選條件: 
# 1. 代碼為 4 碼數字 (排除權證等)
# 2. 排除 2024 年以後上市的新股 (視為資料自然不足)
cutoff_date = (datetime.now() - timedelta(days=600)).strftime("%Y-%m-%d")

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

real_missing = []
for code, name, list_date, cnt in rows:
    # 如果沒有上市日期，或者上市日期早於 cutoff_date，則視為異常
    if not list_date or list_date < cutoff_date:
        real_missing.append((code, name, list_date, cnt))

if not real_missing:
    print('  ✓ 無需補歷史的股票 (所有不足 450 筆的皆為新上市)')
else:
    print(f'  ⚠ 發現 {len(real_missing)} 支需補歷史:')
    for code, name, ld, cnt in real_missing:
        print(f'    - {code} {name} (上市:{ld}) 現有:{cnt}筆')

# 3. 檢查 4530 和 6236 的狀態
print('\n【3. 重點股票狀態】')
for code in ['4530', '6236', '2740', '6904']:
    cur.execute('SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM stock_history WHERE code = ?', (code,))
    row = cur.fetchone()
    if row[0] > 0:
        print(f'  ✓ {code}: {row[0]} 筆 ({row[1]} ~ {row[2]})')
    else:
        print(f'  ⚠ {code}: 無資料')

conn.close()
