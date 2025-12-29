"""
補齊所有缺失資料並報告無法補齊的項目
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
        return []

print("="*60)
print("補齊所有缺失資料")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

unfixable = []  # 無法補齊的項目

# ========== 1. 處理資料缺失股票 ==========
print("\n【1. 處理資料缺失股票】")

missing_stocks = [
    ('4530', '宏易', '2001-04-30'),
    ('8291', '尚茂', '2011-11-29'),  # 已下市
    ('6236', '中湛', '2003-03-31'),
    ('2740', '天蔥', '2015-12-24'),
    ('6904', '伯鑫', '2023-12-05'),
]

for code, name, list_date in missing_stocks:
    print(f"\n處理 {code} {name}...")
    
    # 檢查是否已下市
    if code == '8291':
        # 8291 尚茂已下市，刪除資料
        cur.execute("DELETE FROM stock_history WHERE code = ?", (code,))
        cur.execute("DELETE FROM stock_meta WHERE code = ?", (code,))
        conn.commit()
        print(f"  ✓ {code} 已下市 (2023-11-21)，已刪除")
        continue
    
    # 使用 FinMind 補充
    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    records = fetch_finmind_history(code, start_date, end_date)
    
    if records:
        for r in records:
            cur.execute("""
                INSERT OR REPLACE INTO stock_history 
                (code, date_int, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (r['code'], r['date_int'], r['open'], r['high'], r['low'], 
                  r['close'], r['volume'], r['amount']))
        conn.commit()
        print(f"  ✓ FinMind 補充 {len(records)} 筆")
    else:
        # 無法補齊
        cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = ?", (code,))
        cnt = cur.fetchone()[0]
        unfixable.append({
            'type': '股票資料不足',
            'code': code,
            'name': name,
            'list_date': list_date,
            'current_count': cnt,
            'reason': 'FinMind 無資料'
        })
        print(f"  ⚠ FinMind 無資料")

# ========== 2. 修復 Close 空值 ==========
print("\n【2. 修復 Close 空值】")

cur.execute("""
    SELECT code, date_int FROM stock_history 
    WHERE close IS NULL 
    ORDER BY date_int DESC
""")
null_records = cur.fetchall()
print(f"發現 {len(null_records)} 筆 close 空值")

codes_to_fix = {}
for code, date_int in null_records:
    if code not in codes_to_fix:
        codes_to_fix[code] = []
    codes_to_fix[code].append(date_int)

for code, dates in codes_to_fix.items():
    print(f"\n補充 {code} ({len(dates)} 筆)...", end="")
    
    # 取得日期範圍
    min_date = str(min(dates))
    max_date = str(max(dates))
    start_date = f"{min_date[:4]}-{min_date[4:6]}-{min_date[6:]}"
    end_date = f"{max_date[:4]}-{max_date[4:6]}-{max_date[6:]}"
    
    records = fetch_finmind_history(code, start_date, end_date)
    
    if records:
        for r in records:
            cur.execute("""
                UPDATE stock_history 
                SET open = ?, high = ?, low = ?, close = ?, volume = ?, amount = ?
                WHERE code = ? AND date_int = ? AND close IS NULL
            """, (r['open'], r['high'], r['low'], r['close'], r['volume'], 
                  r['amount'], r['code'], r['date_int']))
        conn.commit()
        print(f" ✓")
    else:
        # 無法補齊，記錄並刪除
        for d in dates:
            unfixable.append({
                'type': 'Close 空值',
                'code': code,
                'date_int': d,
                'reason': 'FinMind 無資料'
            })
        # 刪除無效記錄
        cur.execute("DELETE FROM stock_history WHERE code = ? AND close IS NULL", (code,))
        conn.commit()
        print(f" ⚠ 無資料，已刪除 {len(dates)} 筆")

# ========== 3. 驗證結果 ==========
print("\n" + "="*60)
print("驗證結果")
print("="*60)

cur.execute("SELECT COUNT(*) FROM stock_history WHERE close IS NULL")
remaining_nulls = cur.fetchone()[0]
print(f"剩餘 close 空值: {remaining_nulls}")

# ========== 4. 無法補齊的項目報告 ==========
if unfixable:
    print("\n" + "="*60)
    print("📋 無法補齊的項目 (需人工處理)")
    print("="*60)
    
    # 按日期分組
    by_date = {}
    by_stock = {}
    
    for item in unfixable:
        if item['type'] == 'Close 空值':
            d = item['date_int']
            if d not in by_date:
                by_date[d] = []
            by_date[d].append(item['code'])
        else:
            code = item['code']
            by_stock[code] = item
    
    if by_date:
        print("\n【按日期】缺失的 Close 資料:")
        for date_int, codes in sorted(by_date.items(), reverse=True):
            date_str = f"{date_int // 10000}/{(date_int % 10000) // 100:02d}/{date_int % 100:02d}"
            print(f"  {date_str}: {', '.join(codes)}")
    
    if by_stock:
        print("\n【按股票】資料不足:")
        for code, item in by_stock.items():
            print(f"  {code} {item['name']}: 上市{item['list_date']}, 現有{item['current_count']}筆, {item['reason']}")
else:
    print("\n✓ 所有資料已補齊！")

conn.close()
print("\n完成！")
