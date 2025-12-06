import sqlite3
conn = sqlite3.connect('d:/twse/taiwan_stock.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 模擬 step4_load_data 的讀取
cur.execute('SELECT * FROM stock_snapshot WHERE code="2330"')
row = cur.fetchone()
data = dict(row)

print('step4_load_data 讀取結果:')
print(f"  code: {data.get('code')}")
print(f"  name: {data.get('name')}")
print(f"  date: {data.get('date')}")
print(f"  close: {data.get('close')}")

conn.close()
