import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("SELECT date_int, volume FROM stock_history WHERE code='0000' ORDER BY date_int DESC LIMIT 5")
for row in cur.fetchall():
    print(row)
conn.close()
