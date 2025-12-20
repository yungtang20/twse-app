import sqlite3
import os

db_path = "/sdcard/Download/stock_app/taiwan_stock.db"

# 刪除鎖定檔案
for ext in ['-wal', '-shm', '-journal']:
    lock_file = db_path + ext
    if os.path.exists(lock_file):
        os.remove(lock_file)
        print(f"已刪除: {lock_file}")

# 嘗試連接並修復
try:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=DELETE;")
    conn.execute("VACUUM;")
    conn.close()
    print("✓ 資料庫已解鎖並修復")
except Exception as e:
    print(f"❌ 錯誤: {e}")