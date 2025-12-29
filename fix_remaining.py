"""
補齊剩餘資料缺漏並報告無法補齊的項目
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
        return data.get('data', [])
    except:
        return []

print("="*60)
print("補齊剩餘資料缺漏")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

unfixable = []

# ========== 1. 補齊 open/high/low/volume 空值 ==========
print("\n【1. 補齊 open/high/low/volume 空值】")

cur.execute("""
    SELECT DISTINCT code, date_int FROM stock_history 
    WHERE open IS NULL OR high IS NULL OR low IS NULL OR volume IS NULL
    ORDER BY date_int DESC
""")
null_records = cur.fetchall()
print(f"發現 {len(null_records)} 筆需要補齊")

# 按代碼分組
codes_dates = {}
for code, date_int in null_records:
    if code not in codes_dates:
        codes_dates[code] = []
    codes_dates[code].append(date_int)

fixed = 0
for code, dates in codes_dates.items():
    min_d = str(min(dates))
    max_d = str(max(dates))
    start = f"{min_d[:4]}-{min_d[4:6]}-{min_d[6:]}"
    end = f"{max_d[:4]}-{max_d[4:6]}-{max_d[6:]}"
    
    records = fetch_finmind_history(code, start, end)
    
    if records:
        for r in records:
            date_int = int(r['date'].replace('-', ''))
            cur.execute("""
                UPDATE stock_history 
                SET open = COALESCE(open, ?), 
                    high = COALESCE(high, ?), 
                    low = COALESCE(low, ?), 
                    volume = COALESCE(volume, ?)
                WHERE code = ? AND date_int = ?
            """, (r.get('open'), r.get('max'), r.get('min'), 
                  r.get('Trading_Volume'), code, date_int))
        conn.commit()
        fixed += len(dates)
    else:
        # 無法補齊
        for d in dates:
            cur.execute("""
                SELECT open, high, low, close, volume 
                FROM stock_history WHERE code = ? AND date_int = ?
            """, (code, d))
            row = cur.fetchone()
            unfixable.append({
                'code': code,
                'date_int': d,
                'missing': [],
                'has_close': row[3] if row else None
            })
            if row:
                if row[0] is None: unfixable[-1]['missing'].append('open')
                if row[1] is None: unfixable[-1]['missing'].append('high')
                if row[2] is None: unfixable[-1]['missing'].append('low')
                if row[4] is None: unfixable[-1]['missing'].append('volume')

print(f"✓ 已補齊 {fixed} 筆")

# ========== 2. 補齊 4 支資料不足股票 ==========
print("\n【2. 補齊資料不足股票】")

missing_stocks = [
    ('4530', '宏易'),
    ('6236', '中湛'),
    ('2740', '天蔥'),
    ('6904', '伯鑫'),
]

for code, name in missing_stocks:
    print(f"\n處理 {code} {name}...")
    
    # 取得目前資料範圍
    cur.execute("SELECT MIN(date_int), MAX(date_int), COUNT(*) FROM stock_history WHERE code = ?", (code,))
    row = cur.fetchone()
    current_min, current_max, current_cnt = row
    
    # 嘗試從 2022 年開始補齊
    records = fetch_finmind_history(code, "2022-01-01", datetime.now().strftime("%Y-%m-%d"))
    
    if records:
        for r in records:
            date_int = int(r['date'].replace('-', ''))
            cur.execute("""
                INSERT OR IGNORE INTO stock_history 
                (code, date_int, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, date_int, r.get('open'), r.get('max'), r.get('min'), 
                  r.get('close'), r.get('Trading_Volume'), r.get('Trading_money')))
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM stock_history WHERE code = ?", (code,))
        new_cnt = cur.fetchone()[0]
        print(f"  ✓ 從 {current_cnt} 筆增加到 {new_cnt} 筆")
    else:
        print(f"  ⚠ FinMind 無資料")
        unfixable.append({
            'code': code,
            'name': name,
            'type': '股票資料不足',
            'current_count': current_cnt
        })

# ========== 3. 輸出無法補齊的項目 ==========
print("\n" + "="*60)
print("📋 驗證結果")
print("="*60)

cur.execute("SELECT COUNT(*) FROM stock_history WHERE open IS NULL")
print(f"open 空值: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE high IS NULL")
print(f"high 空值: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE low IS NULL")
print(f"low 空值: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE volume IS NULL")
print(f"volume 空值: {cur.fetchone()[0]}")

if unfixable:
    print("\n" + "="*60)
    print("📋 無法補齊的項目")
    print("="*60)
    
    # 按日期分組輸出 OHLV 缺失
    by_date = {}
    stocks = []
    
    for item in unfixable:
        if 'type' in item and item['type'] == '股票資料不足':
            stocks.append(item)
        elif 'missing' in item:
            d = item['date_int']
            if d not in by_date:
                by_date[d] = []
            by_date[d].append(item)
    
    if by_date:
        print("\n【按日期】缺失明細:")
        for date_int in sorted(by_date.keys(), reverse=True)[:20]:
            date_str = f"{date_int // 10000}/{(date_int % 10000) // 100:02d}/{date_int % 100:02d}"
            print(f"\n  {date_str}:")
            for item in by_date[date_int]:
                missing_str = ', '.join(item['missing'])
                print(f"    {item['code']}: 缺 {missing_str} (close={item['has_close']})")
    
    if stocks:
        print("\n【股票資料不足】無法從 FinMind 取得:")
        for s in stocks:
            print(f"  {s['code']} {s['name']}: 現有 {s['current_count']} 筆")

conn.close()
print("\n完成！")
