"""
測試腳本：執行一鍵更新並檢查資料庫狀態
"""
import sys
sys.path.insert(0, '.')

# 導入必要模組
from 最終修正 import (
    step1_fetch_stock_list,
    step2_download_tpex_daily,
    step3_download_twse_daily,
    step5_clean_delisted,
    db_manager,
    print_flush
)

print("=" * 60)
print("【測試】執行一鍵更新 Step 1-5")
print("=" * 60)

# Step 1: 更新股票清單
print("\n[Step 1] 更新股票清單...")
step1_fetch_stock_list()

# Step 2: 下載 TPEx
print("\n[Step 2] 下載 TPEx (上櫃)...")
step2_download_tpex_daily()

# Step 3: 下載 TWSE
print("\n[Step 3] 下載 TWSE (上市)...")
step3_download_twse_daily()

# Step 5: 清理下市股票
print("\n[Step 5] 清理下市股票...")
step5_clean_delisted()

# 檢查資料庫狀態
print("\n" + "=" * 60)
print("【資料庫狀態檢查】")
print("=" * 60)

with db_manager.get_connection() as conn:
    cur = conn.cursor()
    
    # stock_meta 統計
    cur.execute("SELECT COUNT(*) FROM stock_meta")
    meta_count = cur.fetchone()[0]
    print(f"stock_meta 記錄數: {meta_count}")
    
    # stock_history 統計
    cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
    history_codes = cur.fetchone()[0]
    print(f"stock_history 股票數: {history_codes}")
    
    cur.execute("SELECT COUNT(*) FROM stock_history")
    history_total = cur.fetchone()[0]
    print(f"stock_history 總記錄數: {history_total}")
    
    # 找出在 stock_history 但不在 stock_meta 的股票
    cur.execute("""
        SELECT DISTINCT h.code 
        FROM stock_history h
        LEFT JOIN stock_meta m ON h.code = m.code
        WHERE m.code IS NULL
        LIMIT 20
    """)
    orphan_codes = cur.fetchall()
    print(f"\n在 stock_history 但不在 stock_meta 的股票 (前20): {len(orphan_codes)} 筆")
    for row in orphan_codes:
        print(f"  - {row[0]}")
    
    # 計算總數
    cur.execute("""
        SELECT COUNT(DISTINCT h.code)
        FROM stock_history h
        LEFT JOIN stock_meta m ON h.code = m.code
        WHERE m.code IS NULL
    """)
    orphan_total = cur.fetchone()[0]
    print(f"\n孤兒股票代碼總數: {orphan_total}")
    
    # 檢查這些孤兒代碼的格式
    cur.execute("""
        SELECT h.code, LENGTH(h.code), COUNT(*) as cnt
        FROM stock_history h
        LEFT JOIN stock_meta m ON h.code = m.code
        WHERE m.code IS NULL
        GROUP BY h.code
        ORDER BY LENGTH(h.code), h.code
        LIMIT 30
    """)
    print("\n孤兒代碼分析 (代碼, 長度, 筆數):")
    for row in cur.fetchall():
        print(f"  {row[0]:10} | 長度={row[1]} | {row[2]} 筆")

print("\n" + "=" * 60)
print("【完成】")
print("=" * 60)
