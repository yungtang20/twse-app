import sqlite3

conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

# 检查 1410 的所有有问题的记录
print("1410 的缺金额记录（volume > 0 AND amount = 0 或 NULL）：")
cur.execute("""
    SELECT date_int, open, high, low, close, volume, amount 
    FROM stock_history 
    WHERE code = '1410' 
      AND ((amount IS NULL AND volume > 0) OR (amount = 0 AND volume > 0))
    ORDER BY date_int DESC
    LIMIT 10
""")

for row in cur.fetchall():
    print(f"日期: {row[0]}, OHLC: {row[1]}/{row[2]}/{row[3]}/{row[4]}, 量: {row[5]}, 額: {row[6]}")

print("\n" + "=" * 50)
print("1410 最近5天的全部记录：")
cur.execute("""
    SELECT date_int, open, high, low, close, volume, amount 
    FROM stock_history 
    WHERE code = '1410'
    ORDER BY date_int DESC
    LIMIT 5
""")

for row in cur.fetchall():
    print(f"日期: {row[0]}, OHLC: {row[1]}/{row[2]}/{row[3]}/{row[4]}, 量: {row[5]}, 額: {row[6]}")

conn.close()
