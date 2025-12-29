import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('stock_snapshot 檢查')
print('='*60)

cur.execute("PRAGMA table_info(stock_snapshot)")
cols = [row[1] for row in cur.fetchall()]
print(f'Columns: {cols}')

cur.execute("SELECT COUNT(*) FROM stock_snapshot")
cnt = cur.fetchone()[0]
print(f'Total Rows: {cnt}')

# Check if PE/PB columns exist
print(f"Has PE: {'pe' in cols}")
print(f"Has PB: {'pb' in cols}")

conn.close()
