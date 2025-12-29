"""
檢查資料不足 450 筆的股票是否因上市時間不足
"""
import sqlite3
from datetime import datetime, timedelta

db_path = 'd:\\twse\\taiwan_stock.db'

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 取得每支股票的歷史資料筆數
cur.execute("""
    SELECT h.code, m.name, m.list_date, COUNT(*) as cnt
    FROM stock_history h
    LEFT JOIN stock_meta m ON h.code = m.code
    GROUP BY h.code
    HAVING cnt < 450
    ORDER BY cnt ASC
""")

results = cur.fetchall()
print(f"共有 {len(results)} 支股票歷史資料不足 450 筆\n")

# 計算今天往前 450 個交易日的日期 (約 630 天)
today = datetime.now()
cutoff_date = today - timedelta(days=630)
cutoff_str = cutoff_date.strftime("%Y-%m-%d")
print(f"450 交易日前約為: {cutoff_str}\n")

# 分析
new_stocks = []  # 上市日期在 cutoff 之後
missing_data = []  # 上市日期在 cutoff 之前但資料不足

for code, name, list_date, cnt in results:
    if list_date:
        if list_date >= cutoff_str:
            new_stocks.append((code, name, list_date, cnt))
        else:
            missing_data.append((code, name, list_date, cnt))
    else:
        # 無上市日期資料
        missing_data.append((code, name or "未知", "無資料", cnt))

print("=" * 70)
print(f"1. 上市時間不足 (共 {len(new_stocks)} 支)")
print("=" * 70)
for code, name, list_date, cnt in new_stocks[:20]:
    print(f"  {code} {name:10} 上市日: {list_date}, 資料: {cnt} 筆")
if len(new_stocks) > 20:
    print(f"  ... 等共 {len(new_stocks)} 支")

print("\n" + "=" * 70)
print(f"2. 上市時間足夠但資料不足 (共 {len(missing_data)} 支) - 需要補資料")
print("=" * 70)
for code, name, list_date, cnt in missing_data[:30]:
    print(f"  {code} {name:10} 上市日: {list_date}, 資料: {cnt} 筆")
if len(missing_data) > 30:
    print(f"  ... 等共 {len(missing_data)} 支")

conn.close()
