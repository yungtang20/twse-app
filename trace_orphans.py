"""
追蹤一鍵更新每步驟的 stock_history 變化
"""
import sqlite3

def count_orphans():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_meta")
    meta = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
    history = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT h.code) FROM stock_history h LEFT JOIN stock_meta m ON h.code=m.code WHERE m.code IS NULL")
    orphans = cur.fetchone()[0]
    conn.close()
    return meta, history, orphans

print("=" * 60)
print("追蹤一鍵更新 - 孤兒代碼來源")
print("=" * 60)

print(f"\n初始狀態: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

# 先清理一次
print("\n[清理] 刪除孤兒代碼...")
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("""
    DELETE FROM stock_history
    WHERE code NOT IN (SELECT code FROM stock_meta)
""")
deleted = cur.rowcount
conn.commit()
conn.close()
print(f"已刪除 {deleted} 筆記錄")

print(f"清理後: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

# 匯入模組
import sys
sys.path.insert(0, '.')
from 最終修正 import (
    step1_fetch_stock_list,
    step2_download_tpex_daily,
    step3_download_twse_daily,
    step3_5_download_institutional,
    step4_check_data_gaps,
    step5_clean_delisted,
)

print("\n" + "-" * 60)
print("[Step 1] 更新股票清單")
step1_fetch_stock_list(silent_header=True)
print(f"Step1後: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

print("\n" + "-" * 60)
print("[Step 2] 下載 TPEx")
step2_download_tpex_daily(silent_header=True)
print(f"Step2後: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

print("\n" + "-" * 60)
print("[Step 3] 下載 TWSE")
step3_download_twse_daily(silent_header=True)
print(f"Step3後: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

print("\n" + "-" * 60)
print("[Step 3.5] 下載法人資料")
step3_5_download_institutional(days=3, silent_header=True)
print(f"Step3.5後: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

print("\n" + "-" * 60)
print("[Step 5] 清理下市股票")
step5_clean_delisted()
print(f"Step5後: meta={count_orphans()[0]}, history={count_orphans()[1]}, orphans={count_orphans()[2]}")

print("\n" + "=" * 60)
print("追蹤完成！如果 orphans > 0，查看上面哪個步驟增加了孤兒代碼")
