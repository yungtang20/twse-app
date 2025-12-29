import sqlite3
import os
import sys
from pathlib import Path

def prepare_db_for_mobile():
    db_path = Path("taiwan_stock.db")
    
    if not db_path.exists():
        print(f"❌ 找不到資料庫檔案: {db_path}")
        return

    print(f"📦 正在準備資料庫: {db_path}")
    print("這將會合併 WAL 檔案並將資料庫轉換為單一檔案模式...")

    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            
            # 1. 強制寫入 WAL 資料
            print("1. 合併 WAL 資料 (Checkpoint)...")
            cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            
            # 2. 切換回單一檔案模式
            print("2. 切換為 DELETE 模式 (移除 .wal/.shm)...")
            cursor.execute("PRAGMA journal_mode=DELETE")
            
            # 3. 整理資料庫
            print("3. 整理資料庫 (VACUUM)...")
            cursor.execute("VACUUM")
            
            # 4. 檢查完整性
            print("4. 檢查資料庫完整性...")
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            
            if result and result[0] == "ok":
                print("\n✅ 資料庫準備完成！")
                print("=" * 50)
                print(f"檔案位置: {db_path.absolute()}")
                print(f"檔案大小: {db_path.stat().st_size / 1024 / 1024:.2f} MB")
                print("=" * 50)
                print("👉 現在您可以安全地將 'taiwan_stock.db' 複製到手機了。")
                print("   (不需要複製 .wal 或 .shm 檔)")
            else:
                print(f"\n❌ 資料庫完整性檢查失敗: {result}")

    except Exception as e:
        print(f"\n❌ 發生錯誤: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    prepare_db_for_mobile()
    input("\n按 Enter 鍵結束...")
