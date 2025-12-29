"""
全面檢查資料庫缺漏 - 450 交易日
"""
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

db_path = 'd:\\twse\\taiwan_stock.db'

def get_trading_days(start_date, end_date):
    """產生交易日列表 (排除週末)"""
    days = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # 週一到週五
            days.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return days

# 連接資料庫
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 設定檢查範圍 (約 450 交易日 ≈ 630 天)
end_date = datetime.now()
start_date = end_date - timedelta(days=630)

trading_days = get_trading_days(start_date, end_date)
print(f"檢查範圍: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
print(f"預期交易日數量: {len(trading_days)}")

# 從資料庫取得實際有資料的交易日 (以 stock_history 為基準)
cur.execute("SELECT DISTINCT date_int FROM stock_history WHERE date_int >= ? ORDER BY date_int", (min(trading_days),))
actual_trading_days = set(row[0] for row in cur.fetchall())
print(f"stock_history 中實際交易日: {len(actual_trading_days)} 天")

# 使用實際交易日作為基準 (排除休市日)
trading_days = sorted(actual_trading_days)

# ========== 1. 檢查 stock_history ==========
print("\n" + "="*60)
print("1. 檢查 stock_history (歷史價格)")
print("="*60)

# 檢查每天的資料量
cur.execute("""
    SELECT date_int, COUNT(*) as cnt 
    FROM stock_history 
    WHERE date_int >= ? 
    GROUP BY date_int 
    ORDER BY date_int
""", (min(trading_days),))
history_counts = {row[0]: row[1] for row in cur.fetchall()}

# 找出資料量異常少的日期 (少於 500 筆)
low_count_days = [(d, c) for d, c in history_counts.items() if c < 500]
if low_count_days:
    print(f"⚠ {len(low_count_days)} 天資料量異常少 (< 500 筆):")
    for d, c in sorted(low_count_days)[:10]:
        print(f"   {d}: {c} 筆")
else:
    print("✓ 所有交易日資料量正常")

# ========== 2. 檢查 institutional_investors ==========
print("\n" + "="*60)
print("2. 檢查 institutional_investors (法人資料)")
print("="*60)

cur.execute("SELECT DISTINCT date_int FROM institutional_investors WHERE date_int >= ? ORDER BY date_int", (min(trading_days),))
inst_dates = set(row[0] for row in cur.fetchall())

missing_inst = sorted([d for d in trading_days if d not in inst_dates])
if missing_inst:
    print(f"⚠ 缺少 {len(missing_inst)} 天資料:")
    for d in missing_inst[:20]:
        print(f"   {d}")
    if len(missing_inst) > 20:
        print(f"   ... 等共 {len(missing_inst)} 天")
else:
    print("✓ 無缺漏")

# ========== 3. 檢查 margin_data ==========
print("\n" + "="*60)
print("3. 檢查 margin_data (融資融券)")
print("="*60)

cur.execute("SELECT DISTINCT date_int FROM margin_data WHERE date_int >= ? ORDER BY date_int", (min(trading_days),))
margin_dates = set(row[0] for row in cur.fetchall())

missing_margin = sorted([d for d in trading_days if d not in margin_dates])
if missing_margin:
    print(f"⚠ 缺少 {len(missing_margin)} 天資料:")
    for d in missing_margin[:20]:
        print(f"   {d}")
    if len(missing_margin) > 20:
        print(f"   ... 等共 {len(missing_margin)} 天")
else:
    print("✓ 無缺漏")

# ========== 4. 統計 ==========
print("\n" + "="*60)
print("4. 各表統計")
print("="*60)

tables = ['stock_history', 'institutional_investors', 'margin_data']
for table in tables:
    cur.execute(f"SELECT MIN(date_int), MAX(date_int), COUNT(DISTINCT date_int), COUNT(*) FROM {table}")
    min_d, max_d, days, total = cur.fetchone()
    print(f"  {table:25}: {min_d} ~ {max_d}, {days} 天, {total:,} 筆")

# ========== 5. 匯總 ==========
print("\n" + "="*60)
print("匯總")
print("="*60)

total_inst_gaps = len(missing_inst) if missing_inst else 0
total_margin_gaps = len(missing_margin) if missing_margin else 0
total_gaps = total_inst_gaps + total_margin_gaps

if total_gaps == 0:
    print("✓ 所有資料檢查完成，無缺漏！")
else:
    print(f"⚠ 發現缺漏:")
    print(f"   institutional_investors: {total_inst_gaps} 天")
    print(f"   margin_data: {total_margin_gaps} 天")

conn.close()
