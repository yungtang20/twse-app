"""
手動補入使用者提供的 open/high/low/volume 資料
"""
import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

# 2025/12/26 補件
data_20251226 = [
    {'code': '8488', 'open': 154.5, 'high': 155.5, 'low': 154.0},
    {'code': '6807', 'open': 105.0, 'high': 106.5, 'low': 105.0},
    {'code': '5906', 'open': 208.0, 'high': 209.5, 'low': 207.5},
    {'code': '8921', 'open': 21.80, 'high': 21.80, 'low': 21.80},
    {'code': '8342', 'open': 53.50, 'high': 53.50, 'low': 53.50},
    {'code': '8077', 'open': 43.10, 'high': 43.10, 'low': 42.90},
    {'code': '6856', 'open': 187.0, 'high': 188.5, 'low': 186.0},
    {'code': '6597', 'open': 685.0, 'high': 694.0, 'low': 684.0},
    {'code': '5205', 'open': 10.60, 'high': 10.60, 'low': 10.60},
    {'code': '4305', 'open': 39.00, 'high': 39.00, 'low': 38.95},
    {'code': '3629', 'open': 17.90, 'high': 17.90, 'low': 17.90},
    {'code': '2924', 'open': 30.15, 'high': 30.15, 'low': 30.15},
    {'code': '2073', 'open': 31.35, 'high': 31.35, 'low': 31.35},
]

# 2025/12/19 補件
data_20251219 = [
    {'code': '8917', 'open': 106.5, 'high': 106.5, 'low': 106.5, 'volume': 5000},
    {'code': '8455', 'open': 34.60, 'high': 35.00, 'low': 34.50, 'volume': 12000},
    {'code': '7810', 'open': 100.5, 'high': 102.5, 'low': 100.5, 'volume': 88000},
    {'code': '6921', 'open': 485.0, 'high': 492.0, 'low': 485.0, 'volume': 156000},
    {'code': '6910', 'open': 485.0, 'high': 492.0, 'low': 485.0, 'volume': 156000},  # 同 6921
    {'code': '6904', 'open': 204.0, 'high': 206.0, 'low': 203.0, 'volume': 42000},
    {'code': '6620', 'open': 133.5, 'high': 135.0, 'low': 133.0, 'volume': 215000},
    {'code': '6236', 'open': 13.50, 'high': 13.50, 'low': 13.50, 'volume': 2000},
    {'code': '4530', 'open': 12.30, 'high': 12.35, 'low': 12.25, 'volume': 18000},
    {'code': '3593', 'open': 11.20, 'high': 11.25, 'low': 11.15, 'volume': 31000},
]

print("="*60)
print("手動補入 open/high/low/volume 資料")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

updated = 0

# 處理 2025/12/26
print("\n【2025/12/26】")
for item in data_20251226:
    cur.execute("""
        UPDATE stock_history 
        SET open = ?, high = ?, low = ?
        WHERE code = ? AND date_int = 20251226
    """, (item['open'], item['high'], item['low'], item['code']))
    if cur.rowcount > 0:
        print(f"  ✓ {item['code']}")
        updated += 1
    else:
        # 如果不存在，插入
        cur.execute("""
            INSERT OR IGNORE INTO stock_history (code, date_int, open, high, low)
            VALUES (?, 20251226, ?, ?, ?)
        """, (item['code'], item['open'], item['high'], item['low']))

# 處理 2025/12/19
print("\n【2025/12/19】")
for item in data_20251219:
    volume = item.get('volume', 0)
    cur.execute("""
        UPDATE stock_history 
        SET open = ?, high = ?, low = ?, volume = ?
        WHERE code = ? AND date_int = 20251219
    """, (item['open'], item['high'], item['low'], volume, item['code']))
    if cur.rowcount > 0:
        print(f"  ✓ {item['code']}")
        updated += 1
    else:
        cur.execute("""
            INSERT OR IGNORE INTO stock_history (code, date_int, open, high, low, volume)
            VALUES (?, 20251219, ?, ?, ?, ?)
        """, (item['code'], item['open'], item['high'], item['low'], volume))

conn.commit()

# 對於 2025/12/18 等其他日期，如果 open/high/low 為空但 close 存在，設為與 close 相同
print("\n【補齊其他日期：open=high=low=close】")
cur.execute("""
    UPDATE stock_history 
    SET open = close, high = close, low = close
    WHERE (open IS NULL OR high IS NULL OR low IS NULL) AND close IS NOT NULL
""")
auto_fixed = cur.rowcount
conn.commit()
print(f"  ✓ 自動補齊 {auto_fixed} 筆")

# 驗證
print("\n" + "="*60)
print("驗證結果")
print("="*60)
cur.execute("SELECT COUNT(*) FROM stock_history WHERE open IS NULL")
print(f"open 空值: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE high IS NULL")
print(f"high 空值: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE low IS NULL")
print(f"low 空值: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM stock_history WHERE volume IS NULL")
print(f"volume 空值: {cur.fetchone()[0]}")

conn.close()
print(f"\n✓ 手動更新 {updated} 筆")
print("完成！")
