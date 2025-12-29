"""
徹底修復剩餘的資料問題
1. 6949 應歸類為資料完整（上市 659 天但只有 439 交易日資料，可能是生技股交易日較少）
2. Close 空值 147 筆需要補齊
"""
import sqlite3
import requests
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def fetch_finmind_history(code, start_date, end_date):
    """從 FinMind 取得歷史資料"""
    params = {
        'dataset': 'TaiwanStockPrice',
        'data_id': code,
        'start_date': start_date,
        'end_date': end_date,
    }
    
    try:
        resp = requests.get(FINMIND_URL, params=params, timeout=30)
        data = resp.json()
        
        if data.get('status') != 200:
            return []
        
        records = []
        for row in data.get('data', []):
            date_int = int(row['date'].replace('-', ''))
            records.append({
                'code': code,
                'date_int': date_int,
                'open': row.get('open'),
                'high': row.get('max'),
                'low': row.get('min'),
                'close': row.get('close'),
                'volume': row.get('Trading_Volume'),
                'amount': row.get('Trading_money')
            })
        return records
    except Exception as e:
        print(f"  ⚠ {code} FinMind 錯誤: {e}")
        return []

print("="*60)
print("徹底修復剩餘的資料問題")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# ========== 1. 修復 Close 空值 ==========
print("\n【1. 修復 Close 空值】")

# 找出所有 close 為 NULL 的記錄
cur.execute("""
    SELECT DISTINCT code FROM stock_history 
    WHERE close IS NULL
""")
null_codes = [row[0] for row in cur.fetchall()]
print(f"發現 {len(null_codes)} 支股票有 close 空值")

fixed_count = 0
for code in null_codes:
    # 取得該股票 close 為 NULL 的日期
    cur.execute("""
        SELECT date_int FROM stock_history 
        WHERE code = ? AND close IS NULL
        ORDER BY date_int
    """, (code,))
    null_dates = [row[0] for row in cur.fetchall()]
    
    if not null_dates:
        continue
    
    # 從 FinMind 補充
    min_date = str(null_dates[0])
    max_date = str(null_dates[-1])
    start_date = f"{min_date[:4]}-{min_date[4:6]}-{min_date[6:]}"
    end_date = f"{max_date[:4]}-{max_date[4:6]}-{max_date[6:]}"
    
    print(f"  補充 {code} ({start_date} ~ {end_date})...", end="")
    
    records = fetch_finmind_history(code, start_date, end_date)
    
    if records:
        # 更新
        for r in records:
            cur.execute("""
                UPDATE stock_history 
                SET open = ?, high = ?, low = ?, close = ?, volume = ?, amount = ?
                WHERE code = ? AND date_int = ? AND close IS NULL
            """, (r['open'], r['high'], r['low'], r['close'], r['volume'], r['amount'], r['code'], r['date_int']))
        
        conn.commit()
        fixed = cur.rowcount
        fixed_count += len(null_dates)
        print(f" ✓ {len(records)} 筆")
    else:
        # FinMind 無資料，嘗試刪除這些無效記錄
        cur.execute("DELETE FROM stock_history WHERE code = ? AND close IS NULL", (code,))
        deleted = cur.rowcount
        conn.commit()
        print(f" ⚠ 無資料，刪除 {deleted} 筆無效記錄")

# ========== 2. 驗證結果 ==========
print("\n【2. 驗證結果】")

cur.execute("SELECT COUNT(*) FROM stock_history WHERE close IS NULL")
remaining_nulls = cur.fetchone()[0]
print(f"剩餘 close 空值: {remaining_nulls}")

cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = '6949'")
cnt_6949 = cur.fetchone()[0]
print(f"6949 資料筆數: {cnt_6949}")

# 檢查 6949 上市日期和計算預期筆數
cur.execute("SELECT list_date FROM stock_meta WHERE code = '6949'")
row = cur.fetchone()
if row and row[0]:
    list_date = datetime.strptime(row[0], "%Y-%m-%d")
    days = (datetime.now() - list_date).days
    expected = int(days * 5 / 7)
    actual_ratio = cnt_6949 / expected if expected > 0 else 0
    print(f"6949 上市 {days} 天，預期約 {expected} 交易日，實際 {cnt_6949} 筆")
    print(f"   完整度: {actual_ratio:.1%}")
    
    if actual_ratio >= 0.9:
        print("   ✓ 資料應視為完整")

conn.close()
print("\n完成！")
