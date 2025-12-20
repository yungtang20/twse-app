import sqlite3

conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

print("=" * 70)
print("檢查為什麼自動修復沒生效")
print("=" * 70)

# 1. 检查 volume > 0 但 amount = 0/NULL 且 close > 0 的记录
cur.execute("""
    SELECT code, date_int, close, volume, amount 
    FROM stock_history 
    WHERE volume > 0 AND (amount IS NULL OR amount = 0) AND close > 0
    LIMIT 20
""")
records1 = cur.fetchall()
print(f"\n1. volume > 0, amount = 0, close > 0 的記錄: {len(records1)} 筆")
for r in records1[:5]:
    print(f"   {r}")

# 2. 检查 volume > 0 但 amount = 0/NULL (不管 close)
cur.execute("""
    SELECT code, date_int, close, volume, amount 
    FROM stock_history 
    WHERE volume > 0 AND (amount IS NULL OR amount = 0)
    LIMIT 20
""")
records2 = cur.fetchall()
print(f"\n2. volume > 0, amount = 0 (不管 close): {len(records2)} 筆")
for r in records2[:10]:
    print(f"   {r}")

# 3. 检查 step6 用的查询条件
cutoff_int = 20230101
cur.execute(f"""
    SELECT code, 
           SUM(CASE WHEN (amount IS NULL OR (amount = 0 AND volume > 0)) AND date_int >= {cutoff_int} THEN 1 ELSE 0 END) as missing
    FROM stock_history 
    GROUP BY code
    HAVING missing > 0
    ORDER BY missing DESC
    LIMIT 20
""")
records3 = cur.fetchall()
print(f"\n3. Step6 檢測到的缺金額股票: {len(records3)} 檔")
for r in records3[:10]:
    print(f"   {r}")

conn.close()
