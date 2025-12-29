import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("SELECT * FROM stock_shareholding_all WHERE code='2330' ORDER BY level")
rows = cur.fetchall()
print("2330 Shareholding in DB:")
for row in rows:
    print(row)
conn.close()
