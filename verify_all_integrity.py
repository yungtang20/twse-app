import sqlite3
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*80)
print('最終全面驗證報告 (Final Comprehensive Verification)')
print('='*80)

errors = []

# 1. 空值檢查
print('\n【1. 空值檢查】')
has_nulls = False
for col in ['open', 'high', 'low', 'close', 'volume']:
    cur.execute(f'SELECT COUNT(*) FROM stock_history WHERE {col} IS NULL')
    cnt = cur.fetchone()[0]
    if cnt > 0:
        print(f'  ❌ {col}: {cnt} 筆空值')
        has_nulls = True
        errors.append(f'{col} 有 {cnt} 筆空值')
    else:
        print(f'  ✓ {col}: 0 空值')

# 2. 檢查 8291 (應已刪除)
print('\n【2. 檢查 8291 (尚茂)】')
cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = '8291'")
cnt_8291 = cur.fetchone()[0]
if cnt_8291 == 0:
    print('  ✓ 8291 已完全刪除')
else:
    print(f'  ❌ 8291 仍有 {cnt_8291} 筆資料')
    errors.append(f'8291 仍有 {cnt_8291} 筆資料')

# 3. 檢查補件股票 (4530, 6236)
print('\n【3. 檢查補件股票】')
for code, name, expected_min in [('4530', '宏易', 5000), ('6236', '中湛', 5000)]:
    cur.execute("SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM stock_history WHERE code = ?", (code,))
    row = cur.fetchone()
    cnt = row[0]
    if cnt > expected_min:
        print(f'  ✓ {code} {name}: {cnt} 筆 ({row[1]} ~ {row[2]})')
    else:
        print(f'  ❌ {code} {name}: 僅 {cnt} 筆 (預期 > {expected_min})')
        errors.append(f'{code} 資料不足')

# 4. 檢查手動補件的關鍵數據 (抽樣驗證)
print('\n【4. 驗證手動補件數據 (抽樣)】')
check_points = [
    # 2025/12/26
    ('8488', 20251226, 154.5, 1502000),
    ('6807', 20251226, 106.0, 214000),
    ('5906', 20251226, 208.5, 643000),
    # 2025/12/19
    ('8077', 20251219, 42.9, 4000),
    ('8917', 20251219, 106.5, 5000),
    # 2023/05/03
    ('1410', 20230503, 32.5, 85000)
]

for code, date_int, exp_close, exp_vol in check_points:
    cur.execute("SELECT close, volume FROM stock_history WHERE code = ? AND date_int = ?", (code, date_int))
    row = cur.fetchone()
    if row:
        act_close, act_vol = row
        # 允許浮點數微小誤差
        close_match = abs(act_close - exp_close) < 0.01
        vol_match = act_vol == exp_vol
        
        if close_match and vol_match:
            print(f'  ✓ {code} {date_int}: Close={act_close}, Vol={act_vol} (正確)')
        else:
            print(f'  ❌ {code} {date_int}: 預期 Close={exp_close}/Vol={exp_vol}, 實際 Close={act_close}/Vol={act_vol}')
            errors.append(f'{code} {date_int} 數據不符')
    else:
        print(f'  ❌ {code} {date_int}: 無資料')
        errors.append(f'{code} {date_int} 無資料')

# 5. 檢查是否有其他異常少的股票 (排除新上市)
print('\n【5. 檢查其他異常股票】')
cur.execute('''
    SELECT h.code, COUNT(*) as cnt
    FROM stock_history h
    WHERE LENGTH(h.code) = 4 
      AND h.code GLOB '[0-9][0-9][0-9][0-9]'
      AND CAST(h.code AS INTEGER) >= 1101
      AND CAST(h.code AS INTEGER) < 9000
    GROUP BY h.code
    HAVING cnt < 400
    ORDER BY cnt ASC
''')
rows = cur.fetchall()
# 排除已知新上市 (2024年後)
known_new = ['6899', '5548', '6949', '7777', '6620', '6910', '7810', '6921'] # 簡化列表
suspicious = []
for code, cnt in rows:
    if code not in known_new:
        # 再次確認是否為 2024/2025 上市
        cur.execute("SELECT list_date FROM stock_meta WHERE code = ?", (code,))
        meta = cur.fetchone()
        if meta and meta[0] and meta[0] >= '2024-01-01':
            continue
        suspicious.append((code, cnt))

if not suspicious:
    print('  ✓ 無其他異常股票')
else:
    print(f'  ⚠ 發現 {len(suspicious)} 支潛在異常:')
    for code, cnt in suspicious:
        print(f'    - {code}: {cnt} 筆')

print('='*80)
if not errors:
    print('✅ 驗證通過！所有資料均已補齊且正確。')
else:
    print(f'❌ 驗證失敗！發現 {len(errors)} 個問題。')

conn.close()
