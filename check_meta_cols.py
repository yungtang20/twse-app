import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 檢查 stock_meta 的欄位
cur.execute("PRAGMA table_info(stock_meta)")
columns = cur.fetchall()
print("stock_meta 欄位:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

conn.close()
