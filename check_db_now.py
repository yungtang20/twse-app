import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('資料庫現狀檢查')
print('='*60)

# 1. 檢查 8291
print('\n【8291 尚茂】')
cur.execute("SELECT * FROM stock_meta WHERE code = '8291'")
meta = cur.fetchone()
print(f'  Meta: {meta}')
cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = '8291'")
cnt = cur.fetchone()[0]
print(f'  History Count: {cnt}')

# 2. 檢查 Close 空值
print('\n【Close 空值】')
cur.execute('SELECT code, date_int FROM stock_history WHERE close IS NULL')
nulls = cur.fetchall()
print(f'  Count: {len(nulls)}')
if nulls:
    print(f'  Sample: {nulls[:5]}')

# 3. 檢查 2073 @ 20251226
print('\n【2073 @ 20251226】')
cur.execute("SELECT * FROM stock_history WHERE code = '2073' AND date_int = 20251226")
row = cur.fetchone()
print(f'  Row: {row}')

conn.close()
