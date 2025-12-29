import sqlite3
from datetime import datetime, timedelta

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('最終資料完整性檢查')
print('='*60)

# 1. 空值檢查
print('\n【空值檢查】')
has_nulls = False
for col in ['open', 'high', 'low', 'close', 'volume']:
    cur.execute(f'SELECT COUNT(*) FROM stock_history WHERE {col} IS NULL')
    cnt = cur.fetchone()[0]
    if cnt > 0:
        print(f'  ⚠ {col}: {cnt} 筆空值')
        has_nulls = True
    else:
        print(f'  ✓ {col}: 0 空值')

if not has_nulls:
    print('  ✓ 所有 OHLCV 資料完整！')

# 2. 檢查近期交易日缺漏 (以 2330 為基準)
print('\n【近期交易日缺漏】')
cur.execute("SELECT MAX(date_int) FROM stock_history WHERE code = '2330'")
last_date = cur.fetchone()[0]
print(f'  最新交易日: {last_date}')

# 3. 檢查資料不足股票
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
real_missing = []

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
        real_missing.append((code, name, list_date, cnt))

if not real_missing:
    print('  ✓ 無需補歷史的股票')
else:
    print(f'  ⚠ 發現 {len(real_missing)} 支需補歷史:')
    for code, name, ld, cnt in real_missing:
        print(f'    - {code} {name} (上市:{ld}) 現有:{cnt}筆')

conn.close()
