import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("PRAGMA table_info(stock_history)")
cols = [c[1] for c in cur.fetchall()]
print('stock_history columns:', cols)
