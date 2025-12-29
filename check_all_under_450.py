import sqlite3
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('檢查所有資料不足 450 筆的股票 (與大盤比較)')
print('='*60)

# 1. 獲取大盤 (2330) 的交易日列表，作為基準
cur.execute("SELECT date_int FROM stock_history WHERE code = '2330' ORDER BY date_int")
dates_ref = [row[0] for row in cur.fetchall()]
ref_set = set(dates_ref)
print(f'基準 (2330) 總交易日: {len(dates_ref)} 筆 ({dates_ref[0]} ~ {dates_ref[-1]})')

# 2. 找出所有 < 450 筆的股票
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
candidates = cur.fetchall()

missing_list = []

for code, name, list_date, cnt in candidates:
    if not list_date:
        continue
        
    # 計算該股票上市後的「預期交易日」
    # 1. 找出 list_date 之後的第一個大盤交易日索引
    try:
        ld_int = int(list_date.replace('-', ''))
    except:
        continue
        
    # 計算從上市日開始，大盤有多少交易日
    expected_days = [d for d in dates_ref if d >= ld_int]
    expected_cnt = len(expected_days)
    
    if expected_cnt == 0:
        continue
        
    # 計算缺漏率
    # 如果實際筆數明顯少於預期 (例如少於 95%)
    if cnt < expected_cnt * 0.95:
        diff = expected_cnt - cnt
        missing_list.append({
            'code': code,
            'name': name,
            'list_date': list_date,
            'cnt': cnt,
            'expected': expected_cnt,
            'diff': diff
        })

print(f'\n發現 {len(missing_list)} 支股票有明顯缺漏:')
for item in missing_list:
    print(f"  - {item['code']} {item['name']} (上市:{item['list_date']}) 現有:{item['cnt']} / 預期:{item['expected']} (缺 {item['diff']} 筆)")

conn.close()
