import sqlite3
conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

# 檢查 2330 的資料
cur.execute("SELECT code, name, date, close FROM stock_snapshot WHERE code='2330'")
row = cur.fetchone()
print('stock_snapshot 中的 2330:')
if row:
    print(f'  code: {row[0]}')
    print(f'  name: {row[1]}')
    print(f'  date: {row[2]}')
    print(f'  close: {row[3]}')

# 檢查 stock_meta
cur.execute("SELECT code, name FROM stock_meta WHERE code='2330'")
row = cur.fetchone()
print(f'\nstock_meta 中的 2330:')
if row:
    print(f'  code: {row[0]}')
    print(f'  name: {row[1]}')
else:
    print('  (無資料)')

# 隨機抽查幾筆
print('\n隨機抽查 5 筆:')
cur.execute("SELECT code, name FROM stock_snapshot LIMIT 5")
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]}')

conn.close()
