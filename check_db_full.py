import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

print("資料庫檢查:")
print("-" * 40)

# 基本
cur.execute("SELECT COUNT(*) FROM stock_meta")
print(f"stock_meta: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
print(f"stock_history: {cur.fetchone()[0]} 檔")
cur.execute("SELECT COUNT(DISTINCT code) FROM institutional_investors")
print(f"institutional_investors: {cur.fetchone()[0]} 檔")

# 缺法人
cur.execute("SELECT m.code, m.name FROM stock_meta m LEFT JOIN institutional_investors i ON m.code=i.code WHERE i.code IS NULL")
rows = cur.fetchall()
print(f"\n缺法人資料: {len(rows)} 檔")
for r in rows: print(f"  {r[0]} {r[1]}")

# 表格
print("\n所有表格:")
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
for r in cur.fetchall():
    cur.execute(f"SELECT COUNT(*) FROM {r[0]}")
    print(f"  {r[0]}: {cur.fetchone()[0]}")

conn.close()
