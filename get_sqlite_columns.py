import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

tables = ['stock_history', 'institutional_investors']

for t in tables:
    print(f'Table: {t}')
    cur.execute(f"PRAGMA table_info({t})")
    for row in cur.fetchall():
        # cid, name, type, notnull, dflt_value, pk
        print(f'  {row[1]} ({row[2]})')

conn.close()
