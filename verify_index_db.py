import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("SELECT * FROM stock_history WHERE code='0000' AND date_int=20251219")
rows = cur.fetchall()
for r in rows:
    print(f"Code: {r[0]}, Date: {r[1]}, Open: {r[2]}, High: {r[3]}, Low: {r[4]}, Close: {r[5]}, Vol: {r[6]}, Amt: {r[7]}")
conn.close()
