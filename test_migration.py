"""測試資料庫遷移腳本 - 獨立版"""
import sqlite3
from pathlib import Path
import sys
import os

# 禁用主程式自動執行
os.environ['TEST_MODE'] = '1'

DB_FILE = Path('d:/twse/taiwan_stock.db')

def print_flush(s="", end="\n"):
    print(s, end=end)
    sys.stdout.flush()

print("正在建立新表格...")

conn = sqlite3.connect(DB_FILE, timeout=30)
cur = conn.cursor()

# ========== 新架構：三表結構 ==========
# 1. 建立股票名冊表
cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_meta (
        code TEXT PRIMARY KEY,
        name TEXT,
        list_date TEXT,
        delist_date TEXT,
        market_type TEXT
    )
""")

# 2. 建立歷史表（純K線原料倉庫）
cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_history (
        code TEXT,
        date_int INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        amount INTEGER,
        PRIMARY KEY (code, date_int)
    )
""")

# 3. 建立快照表（今日展示架）
cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_snapshot (
        code TEXT PRIMARY KEY,
        name TEXT,
        date TEXT,
        close REAL,
        volume INTEGER,
        close_prev REAL,
        vol_prev INTEGER,
        ma3 REAL, ma20 REAL, ma60 REAL, ma120 REAL, ma200 REAL,
        wma3 REAL, wma20 REAL, wma60 REAL, wma120 REAL, wma200 REAL,
        mfi14 REAL, vwap20 REAL, chg14_pct REAL,
        rsi REAL, macd REAL, signal REAL,
        vp_poc REAL, vp_upper REAL, vp_lower REAL,
        month_k REAL, month_d REAL,
        daily_k REAL, daily_d REAL,
        week_k REAL, week_d REAL,
        ma3_prev REAL, ma20_prev REAL, ma60_prev REAL, ma120_prev REAL, ma200_prev REAL,
        wma3_prev REAL, wma20_prev REAL, wma60_prev REAL, wma120_prev REAL, wma200_prev REAL,
        mfi14_prev REAL, vwap20_prev REAL, chg14_pct_prev REAL,
        month_k_prev REAL, month_d_prev REAL,
        daily_k_prev REAL, daily_d_prev REAL,
        week_k_prev REAL, week_d_prev REAL,
        smi REAL, svi REAL, nvi REAL, pvi REAL, clv REAL,
        smi_signal INTEGER, svi_signal INTEGER,
        nvi_signal INTEGER, vsa_signal INTEGER,
        smart_score INTEGER,
        smi_prev REAL, svi_prev REAL, nvi_prev REAL,
        smart_score_prev INTEGER
    )
""")

# 4. 建立新架構索引
cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_meta_code ON stock_meta(code)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_code ON stock_history(code)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_date ON stock_history(date_int)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_code_date ON stock_history(code, date_int DESC)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshot_date ON stock_snapshot(date)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshot_smart_score ON stock_snapshot(smart_score)")

conn.commit()
print("✓ 新表格建立完成")

# Check tables
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
table_names = [t[0] for t in tables]
print('\n=== 現有表格 ===')
for t in table_names:
    print(f'  - {t}')

# Check counts before migration
print('\n=== 遷移前資料量 ===')
for table in ['stock_data', 'stock_meta', 'stock_history', 'stock_snapshot']:
    if table in table_names:
        cur.execute(f'SELECT COUNT(*) FROM {table}')
        count = cur.fetchone()[0]
        print(f'{table}: {count:,} 筆')

# Run migration
print('\n=== 開始遷移資料 ===')

# Check if already migrated
cur.execute("SELECT COUNT(*) FROM stock_history")
if cur.fetchone()[0] > 0:
    print("✓ 偵測到已有遷移資料，跳過遷移")
else:
    # 1. Migrate to stock_meta
    print("  -> 遷移股票名冊...")
    cur.execute("""
        INSERT OR IGNORE INTO stock_meta (code, name, market_type)
        SELECT DISTINCT code, name, 
               CASE WHEN code GLOB '[0-9][0-9][0-9][0-9]' AND code < '6000' 
                    THEN 'TWSE' ELSE 'TPEx' END
        FROM stock_data
        WHERE code IS NOT NULL
    """)
    meta_count = cur.rowcount
    print(f"     ✓ 新增 {meta_count} 筆股票名冊")
    
    # 2. Migrate to stock_history
    print("  -> 遷移歷史K線...")
    cur.execute("""
        INSERT OR IGNORE INTO stock_history 
        (code, date_int, open, high, low, close, volume)
        SELECT code,
               CAST(REPLACE(date, '-', '') AS INTEGER),
               open, high, low, close, volume
        FROM stock_data
        WHERE code IS NOT NULL AND date IS NOT NULL
    """)
    history_count = cur.rowcount
    print(f"     ✓ 新增 {history_count} 筆歷史K線")
    
    # 3. Migrate to stock_snapshot
    print("  -> 遷移最新快照...")
    cur.execute("""
        INSERT OR REPLACE INTO stock_snapshot (
            code, name, date, close, volume, close_prev, vol_prev,
            ma3, ma20, ma60, ma120, ma200,
            wma3, wma20, wma60, wma120, wma200,
            mfi14, vwap20, chg14_pct, rsi, macd, signal,
            vp_poc, vp_upper, vp_lower,
            month_k, month_d, daily_k, daily_d, week_k, week_d,
            ma3_prev, ma20_prev, ma60_prev, ma120_prev, ma200_prev,
            wma3_prev, wma20_prev, wma60_prev, wma120_prev, wma200_prev,
            mfi14_prev, vwap20_prev, chg14_pct_prev,
            month_k_prev, month_d_prev,
            daily_k_prev, daily_d_prev, week_k_prev, week_d_prev,
            smi, svi, nvi, pvi, clv,
            smi_signal, svi_signal, nvi_signal, vsa_signal, smart_score,
            smi_prev, svi_prev, nvi_prev, smart_score_prev
        )
        SELECT 
            code, name, date, close, volume, close_prev, vol_prev,
            ma3, ma20, ma60, ma120, ma200,
            wma3, wma20, wma60, wma120, wma200,
            mfi14, vwap20, chg14_pct, rsi, macd, signal,
            vp_poc, vp_upper, vp_lower,
            month_k, month_d, daily_k, daily_d, week_k, week_d,
            ma3_prev, ma20_prev, ma60_prev, ma120_prev, ma200_prev,
            wma3_prev, wma20_prev, wma60_prev, wma120_prev, wma200_prev,
            mfi14_prev, vwap20_prev, chg14_pct_prev,
            month_k_prev, month_d_prev,
            daily_k_prev, daily_d_prev, week_k_prev, week_d_prev,
            smi, svi, nvi, pvi, clv,
            smi_signal, svi_signal, nvi_signal, vsa_signal, smart_score,
            smi_prev, svi_prev, nvi_prev, smart_score_prev
        FROM stock_data T1
        WHERE date = (SELECT MAX(date) FROM stock_data T2 WHERE T2.code = T1.code)
    """)
    snapshot_count = cur.rowcount
    print(f"     ✓ 新增 {snapshot_count} 筆快照")
    
    conn.commit()
    print("✓ 資料遷移完成")

# Check counts after migration
print('\n=== 遷移後資料量 ===')
for table in ['stock_data', 'stock_meta', 'stock_history', 'stock_snapshot']:
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    count = cur.fetchone()[0]
    print(f'{table}: {count:,} 筆')

# Calculate unique codes
cur.execute('SELECT COUNT(DISTINCT code) FROM stock_data')
unique_codes = cur.fetchone()[0]
print(f'\n唯一股票代碼: {unique_codes:,} 檔')

conn.close()
print('\n✓ 測試完成')
