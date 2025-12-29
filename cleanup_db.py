"""
清理 stock_history 中的無效代碼
只保留：4位純數字 且 存在於 stock_meta 的代碼
"""
import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

print("=" * 60)
print("清理 stock_history 中的無效代碼")
print("=" * 60)

# 統計清理前
cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
before_codes = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM stock_history")
before_records = cur.fetchone()[0]
print(f"清理前: {before_codes} 股票代碼, {before_records} 筆記錄")

# 找出需要刪除的代碼
# 1. 不在 stock_meta 中的代碼
# 2. 或非 4 位純數字的代碼
cur.execute("""
    SELECT DISTINCT h.code
    FROM stock_history h
    LEFT JOIN stock_meta m ON h.code = m.code
    WHERE m.code IS NULL
       OR LENGTH(h.code) != 4
       OR h.code GLOB '*[^0-9]*'
""")
invalid_codes = [r[0] for r in cur.fetchall()]
print(f"無效代碼: {len(invalid_codes)}")

if invalid_codes:
    print(f"準備刪除...")
    # 使用批次刪除
    for code in invalid_codes:
        cur.execute("DELETE FROM stock_history WHERE code = ?", (code,))
    conn.commit()
    print(f"✓ 已刪除 {len(invalid_codes)} 個無效代碼的記錄")

# 統計清理後
cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
after_codes = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM stock_history")
after_records = cur.fetchone()[0]
print(f"清理後: {after_codes} 股票代碼, {after_records} 筆記錄")

conn.close()
print("✓ 完成！")
