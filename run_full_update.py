"""
執行完整一鍵更新並追蹤每個步驟的孤兒代碼變化
"""
import sqlite3
import sys
sys.path.insert(0, '.')

def get_orphans():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_meta")
    m = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
    h = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT h.code) FROM stock_history h LEFT JOIN stock_meta m ON h.code=m.code WHERE m.code IS NULL")
    o = cur.fetchone()[0]
    conn.close()
    return m, h, o

print("開始追蹤...")
print(f"初始: {get_orphans()}")

from 最終修正 import _run_full_daily_update

# 執行一鍵更新
_run_full_daily_update()

print(f"\n完成後: {get_orphans()}")
