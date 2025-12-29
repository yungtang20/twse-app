import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("SELECT code, date_int, tdcc_count, large_shareholder_pct FROM stock_history WHERE tdcc_count IS NOT NULL LIMIT 10")
rows = cur.fetchall()
print("TDCC Data in stock_history:")
for row in rows:
    print(row)

cur.execute("SELECT COUNT(*) FROM stock_history WHERE tdcc_count IS NOT NULL")
print(f"Total rows with TDCC data: {cur.fetchone()[0]}")

conn.close()
