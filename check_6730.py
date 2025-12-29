import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

print("=== 6730 股票詳情 ===")
cur.execute("SELECT name FROM stock_meta WHERE code='6730'")
r = cur.fetchone()
print(f"名稱: {r[0] if r else '無'}")

cur.execute("SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM stock_history WHERE code='6730'")
r = cur.fetchone()
print(f"歷史記錄: {r[0]} 筆, 日期: {r[1]} ~ {r[2]}")

cur.execute("SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM institutional_investors WHERE code='6730'")
r = cur.fetchone()
print(f"法人記錄: {r[0]} 筆, 日期: {r[1]} ~ {r[2]}")

# 查看最近幾筆
cur.execute("SELECT date_int FROM stock_history WHERE code='6730' ORDER BY date_int DESC LIMIT 5")
print(f"最近歷史日期: {[r[0] for r in cur.fetchall()]}")

conn.close()
