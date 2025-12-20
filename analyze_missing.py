import sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

# 计算截止日期 (最近 3 年)
cutoff_int = int((datetime.now() - timedelta(days=730)).strftime("%Y%m%d"))

print("=" * 70)
print("分析「缺金額」股票的實際情況")
print("=" * 70)

# 找出被认为缺金额的股票 (volume > 0 AND amount = 0/NULL)
cur.execute(f"""
    SELECT code, 
           COUNT(*) as total,
           SUM(CASE WHEN volume > 0 AND (amount IS NULL OR amount = 0) THEN 1 ELSE 0 END) as missing,
           MAX(date_int) as latest_date
    FROM stock_history 
    WHERE date_int >= {cutoff_int}
    GROUP BY code
    HAVING missing > 0
    ORDER BY missing DESC
""")

stocks = cur.fetchall()
print(f"共有 {len(stocks)} 檔股票缺金額\n")

print(f"{'代號':<8} {'總筆數':<8} {'缺金額':<8} {'最新日期':<12} {'問題詳情'}")
print("-" * 70)

for code, total, missing, latest_date in stocks[:30]:
    # 查看具体缺失的记录
    cur.execute(f"""
        SELECT date_int, volume, amount, close
        FROM stock_history 
        WHERE code = ? AND volume > 0 AND (amount IS NULL OR amount = 0) AND date_int >= {cutoff_int}
        ORDER BY date_int DESC
        LIMIT 3
    """, (code,))
    details = cur.fetchall()
    
    detail_str = ", ".join([f"{d[0]}(V={d[1]})" for d in details])
    print(f"{code:<8} {total:<8} {missing:<8} {latest_date:<12} {detail_str[:35]}")

conn.close()

print("\n" + "=" * 70)
print("建議：這些股票的 volume > 0 但 amount = 0，可能是 API 沒有提供金額資料")
print("解決方案：可以用 close * volume 估算金額")
print("=" * 70)
