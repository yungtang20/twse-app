import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='stock_history'")
print(cur.fetchone()[0])
conn.close()
