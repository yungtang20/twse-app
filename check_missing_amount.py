import sqlite3

# 检查缺金额的股票实际情况
conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

print("=" * 70)
print("检查「缺金额」股票的实际情况")
print("=" * 70)

# 找出被认为缺金额的股票
cutoff_int = 20230101  # 最近3年
cur.execute(f"""
    SELECT code, 
           SUM(CASE WHEN (amount IS NULL OR (amount = 0 AND volume > 0)) THEN 1 ELSE 0 END) as missing,
           SUM(CASE WHEN volume = 0 AND amount = 0 THEN 1 ELSE 0 END) as zero_trade,
           COUNT(*) as total
    FROM stock_history 
    WHERE date_int >= {cutoff_int}
    GROUP BY code
    HAVING missing > 0
    ORDER BY missing DESC
    LIMIT 20
""")

print(f"{'代號':<8} {'缺金額':<10} {'零成交(V=0,A=0)':<15} {'總筆數'}")
print("-" * 70)

for row in cur.fetchall():
    code, missing, zero_trade, total = row
    print(f"{code:<8} {missing:<10} {zero_trade:<15} {total}")

# 检查具体案例
print("\n" + "=" * 70)
print("具体案例：1410 的缺金额记录")
print("=" * 70)
cur.execute("""
    SELECT date_int, open, high, low, close, volume, amount 
    FROM stock_history 
    WHERE code = '1410' AND (amount IS NULL OR (amount = 0 AND volume > 0))
    ORDER BY date_int DESC
    LIMIT 10
""")

for row in cur.fetchall():
    print(f"日期: {row[0]}, OHLC: {row[1]}/{row[2]}/{row[3]}/{row[4]}, 量: {row[5]}, 額: {row[6]}")

conn.close()
