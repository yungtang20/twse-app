import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

print("=" * 60)
print("清理後驗證")
print("=" * 60)

cur.execute("SELECT COUNT(*) FROM stock_meta")
print(f"stock_meta: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
print(f"stock_history codes: {cur.fetchone()[0]}")

# 確認沒有孤兒代碼
cur.execute("""
    SELECT COUNT(DISTINCT h.code)
    FROM stock_history h
    LEFT JOIN stock_meta m ON h.code = m.code
    WHERE m.code IS NULL
""")
orphans = cur.fetchone()[0]
print(f"孤兒代碼: {orphans}")

if orphans == 0:
    print("\n✓ 資料庫一致性檢查通過！")

conn.close()
