import sqlite3

conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

cutoff_int = 20230101

print("=" * 70)
print("深入檢查缺金額股票")
print("=" * 70)

# 检查 step6 用的 SQL 条件
cur.execute(f"""
    SELECT code, 
           SUM(CASE WHEN (amount IS NULL OR (amount = 0 AND volume > 0)) AND date_int >= {cutoff_int} THEN 1 ELSE 0 END) as missing
    FROM stock_history 
    GROUP BY code
    HAVING missing > 0
    ORDER BY missing DESC
""")

stocks = cur.fetchall()
print(f"共有 {len(stocks)} 檔股票符合「缺金額」條件\n")

for code, missing in stocks[:10]:
    # 查看具体缺失的记录详情
    cur.execute(f"""
        SELECT date_int, open, high, low, close, volume, amount
        FROM stock_history 
        WHERE code = ? AND (amount IS NULL OR (amount = 0 AND volume > 0)) AND date_int >= {cutoff_int}
        ORDER BY date_int DESC
        LIMIT 3
    """, (code,))
    details = cur.fetchall()
    
    print(f"\n{code} (缺 {missing} 筆):")
    for d in details:
        date, o, h, l, c, v, a = d
        print(f"  日期={date}, OHLC={o}/{h}/{l}/{c}, V={v}, A={a}")

conn.close()
