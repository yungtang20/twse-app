"""
手動補入使用者提供的缺失收盤價資料
"""
import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

# 使用者提供的缺失資料
missing_data = [
    # 2025/12/26
    {'code': '8488', 'date_int': 20251226, 'close': 154.5},
    {'code': '6807', 'date_int': 20251226, 'close': 106.0},
    {'code': '5906', 'date_int': 20251226, 'close': 208.5},
    {'code': '8921', 'date_int': 20251226, 'close': 21.80},
    {'code': '8342', 'date_int': 20251226, 'close': 53.50},
    {'code': '8077', 'date_int': 20251226, 'close': 42.90},
    {'code': '6856', 'date_int': 20251226, 'close': 188.0},
    {'code': '6597', 'date_int': 20251226, 'close': 689.0},
    {'code': '5205', 'date_int': 20251226, 'close': 10.60},
    {'code': '4305', 'date_int': 20251226, 'close': 38.95},
    {'code': '3629', 'date_int': 20251226, 'close': 17.90},
    {'code': '2924', 'date_int': 20251226, 'close': 30.15},
    {'code': '2073', 'date_int': 20251226, 'close': 31.35},
    
    # 2025/12/19
    {'code': '8077', 'date_int': 20251219, 'close': 42.90},
    {'code': '8917', 'date_int': 20251219, 'close': 106.5},
    {'code': '8455', 'date_int': 20251219, 'close': 34.75},
    {'code': '7810', 'date_int': 20251219, 'close': 101.5},
    {'code': '6921', 'date_int': 20251219, 'close': 489.0},
    {'code': '3593', 'date_int': 20251219, 'close': 11.20},
    {'code': '6910', 'date_int': 20251219, 'close': 489.0},
    {'code': '6904', 'date_int': 20251219, 'close': 205.0},
    {'code': '6620', 'date_int': 20251219, 'close': 134.0},
    {'code': '6236', 'date_int': 20251219, 'close': 13.50},
    {'code': '4530', 'date_int': 20251219, 'close': 12.30},
    
    # 2023/05/03
    {'code': '1410', 'date_int': 20230503, 'close': 32.50},
]

print("="*60)
print("手動補入缺失收盤價資料")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

inserted = 0
updated = 0

for item in missing_data:
    code = item['code']
    date_int = item['date_int']
    close = item['close']
    
    # 檢查記錄是否存在
    cur.execute("SELECT close FROM stock_history WHERE code = ? AND date_int = ?", (code, date_int))
    row = cur.fetchone()
    
    if row is None:
        # 不存在，插入新記錄
        cur.execute("""
            INSERT INTO stock_history (code, date_int, close)
            VALUES (?, ?, ?)
        """, (code, date_int, close))
        inserted += 1
        print(f"  插入: {code} {date_int} close={close}")
    elif row[0] is None:
        # 存在但 close 為 NULL，更新
        cur.execute("""
            UPDATE stock_history SET close = ? 
            WHERE code = ? AND date_int = ?
        """, (close, code, date_int))
        updated += 1
        print(f"  更新: {code} {date_int} close={close}")
    else:
        print(f"  跳過: {code} {date_int} 已有 close={row[0]}")

conn.commit()

print(f"\n✓ 插入 {inserted} 筆，更新 {updated} 筆")

# 驗證
cur.execute("SELECT COUNT(*) FROM stock_history WHERE close IS NULL")
remaining = cur.fetchone()[0]
print(f"剩餘 close 空值: {remaining}")

conn.close()
print("\n完成！")
