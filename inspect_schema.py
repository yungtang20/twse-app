import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('Tables:')
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
for row in cur.fetchall():
    print(f'  - {row[0]}')

print('\nstock_history columns:')
cur.execute("PRAGMA table_info(stock_history)")
for row in cur.fetchall():
    print(f'  - {row[1]}')

print('\nCheck for valuation table:')
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%valuation%'")
print(cur.fetchall())

conn.close()
