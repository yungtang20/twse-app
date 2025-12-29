"""
手動補入使用者提供的成交量 (Volume) 資料
"""
import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

# 成交量資料 (單位: 張 -> 需轉換為股，即 * 1000)
# 注意：有些資料來源可能是股數，但用戶註明單位為「張」，故需乘 1000
# 若為 0 則維持 0
volume_data = [
    # 2025/12/26
    {'code': '8488', 'date_int': 20251226, 'volume': 1502 * 1000},
    {'code': '6807', 'date_int': 20251226, 'volume': 214 * 1000},
    {'code': '5906', 'date_int': 20251226, 'volume': 643 * 1000},
    {'code': '6597', 'date_int': 20251226, 'volume': 358 * 1000},
    {'code': '6856', 'date_int': 20251226, 'volume': 1280 * 1000},
    {'code': '8342', 'date_int': 20251226, 'volume': 15 * 1000},
    {'code': '8077', 'date_int': 20251226, 'volume': 2 * 1000},
    {'code': '8921', 'date_int': 20251226, 'volume': 0},
    {'code': '5205', 'date_int': 20251226, 'volume': 0},
    {'code': '4305', 'date_int': 20251226, 'volume': 3 * 1000},
    {'code': '3629', 'date_int': 20251226, 'volume': 0},
    {'code': '2924', 'date_int': 20251226, 'volume': 0},
    {'code': '2073', 'date_int': 20251226, 'volume': 0},
    
    # 2025/12/19
    {'code': '8077', 'date_int': 20251219, 'volume': 4 * 1000},
    
    # 2023/05/03
    {'code': '1410', 'date_int': 20230503, 'volume': 85 * 1000},
]

print("="*60)
print("手動補入成交量資料")
print("="*60)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

updated = 0

for item in volume_data:
    code = item['code']
    date_int = item['date_int']
    volume = item['volume']
    
    cur.execute("""
        UPDATE stock_history 
        SET volume = ?
        WHERE code = ? AND date_int = ?
    """, (volume, code, date_int))
    
    if cur.rowcount > 0:
        print(f"  ✓ {code} {date_int} volume={volume}")
        updated += 1
    else:
        print(f"  ⚠ {code} {date_int} 找不到記錄")

conn.commit()

# 驗證 Volume 空值
print("\n" + "="*60)
print("驗證 Volume 空值")
print("="*60)
cur.execute("SELECT COUNT(*) FROM stock_history WHERE volume IS NULL")
cnt = cur.fetchone()[0]
print(f"剩餘 Volume 空值: {cnt}")

if cnt > 0:
    cur.execute("SELECT code, date_int FROM stock_history WHERE volume IS NULL")
    for row in cur.fetchall():
        print(f"  - {row[0]} {row[1]}")

# 關於資料不足股票
print("\n" + "="*60)
print("資料不足股票說明")
print("="*60)
print("4530 宏易: 建議使用 Goodinfo 補齊 (需人工下載)")
print("6236 中湛: 建議使用 鉅亨網 補齊 (需人工下載)")
print("2740 天蔥 & 6904 伯鑫: 已接近 450 筆，可視為資料完整")

conn.close()
print(f"\n✓ 已更新 {updated} 筆成交量")
print("完成！")
