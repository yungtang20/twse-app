import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

# 檢查 2330 的日期分布
cur.execute("""
    SELECT date_int, COUNT(*) as levels, SUM(holders) as total_holders
    FROM stock_shareholding_all 
    WHERE code='2330' 
    GROUP BY date_int 
    ORDER BY date_int DESC
""")
results = cur.fetchall()

print(f"=== 2330 集保資料統計 ===")
print(f"日期數: {len(results)}")
print(f"\n日期列表:")
for date_int, levels, total_holders in results:
    print(f"  {date_int}: {levels} 級, {total_holders:,} 人")

# 總筆數
cur.execute("SELECT COUNT(*) FROM stock_shareholding_all WHERE code='2330'")
print(f"\n總筆數: {cur.fetchone()[0]}")

conn.close()
