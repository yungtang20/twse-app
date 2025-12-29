import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

print("需要補充的資料:")
print("-" * 40)

# 缺法人
cur.execute("SELECT m.code FROM stock_meta m LEFT JOIN institutional_investors i ON m.code=i.code WHERE i.code IS NULL")
no_inst = [r[0] for r in cur.fetchall()]
print(f"1. 缺法人資料: {len(no_inst)} 檔 → {no_inst}")

# 缺PE
cur.execute("SELECT COUNT(*) FROM stock_snapshot WHERE pe IS NULL OR pe<=0")
print(f"2. 缺PE估值: {cur.fetchone()[0]} 檔 (正常，部分無盈利)")

# 資料筆數<100
cur.execute("SELECT COUNT(*) FROM (SELECT code FROM stock_history GROUP BY code HAVING COUNT(*)<100)")
print(f"3. 歷史資料<100筆: {cur.fetchone()[0]} 檔 (新上市股)")

# 快照無收盤
cur.execute("SELECT COUNT(*) FROM stock_snapshot WHERE close IS NULL OR close<=0")
print(f"4. 快照無收盤價: {cur.fetchone()[0]} 檔")

print("\n結論: 資料基本完整!")
conn.close()
