#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SQLite 單一寫入員模式驗證測試 v2"""
import sys
import time
sys.path.insert(0, 'd:/twse')

print("=" * 50)
print("SQLite 單一寫入員模式驗證測試 v2")
print("=" * 50)

# 測試 1: 匯入模組
try:
    from 最終修正 import db_manager, ensure_db, SingleWriterDBManager, ProxyConnection
    print("✓ 測試 1: 模組匯入成功")
    print(f"  - db_manager 類型: {type(db_manager).__name__}")
except Exception as e:
    print(f"✗ 測試 1 失敗: {e}")
    sys.exit(1)

# 測試 2: 檢查寫入線程
try:
    writer_alive = db_manager._writer._writer_thread.is_alive()
    print(f"✓ 測試 2: 寫入線程運行中: {writer_alive}")
except Exception as e:
    print(f"✗ 測試 2 失敗: {e}")

# 測試 3: 初始化資料庫
try:
    ensure_db()
    print("✓ 測試 3: ensure_db() 執行成功")
except Exception as e:
    print(f"✗ 測試 3 失敗: {e}")

# 測試 4: 讀取操作
try:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_meta")
        count = cur.fetchone()[0]
        print(f"✓ 測試 4: 讀取操作成功 (stock_meta: {count} 筆)")
except Exception as e:
    print(f"✗ 測試 4 失敗: {e}")

# 測試 5-6: 寫入與讀取同一筆資料 (在同一連線中)
try:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        # 先清理可能存在的測試資料
        cur.execute("DELETE FROM stock_meta WHERE code='TEST99'")
        # 寫入新資料
        cur.execute("INSERT INTO stock_meta (code, name) VALUES ('TEST99', 'Test Stock 99')")
        # commit 會觸發寫入並重新開啟讀取連線
        conn.commit()
        
        # 在同一個 with 區塊內讀取資料
        cur = conn.cursor()
        cur.execute("SELECT name FROM stock_meta WHERE code='TEST99'")
        result = cur.fetchone()
        
        if result and result[0] == 'Test Stock 99':
            print("✓ 測試 5-6: 寫入與即時讀取成功")
        else:
            print(f"✗ 測試 5-6: 寫入驗證失敗 - 結果: {result}")
except Exception as e:
    print(f"✗ 測試 5-6 失敗: {e}")
    import traceback
    traceback.print_exc()

# 測試 7: 跨連線讀取
try:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM stock_meta WHERE code='TEST99'")
        result = cur.fetchone()
        if result and result[0] == 'Test Stock 99':
            print("✓ 測試 7: 跨連線讀取成功")
        else:
            print(f"✗ 測試 7: 跨連線讀取失敗 - 結果: {result}")
except Exception as e:
    print(f"✗ 測試 7 失敗: {e}")

# 測試 8: 清理測試資料
try:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM stock_meta WHERE code='TEST99'")
    print("✓ 測試 8: 測試資料清理成功")
except Exception as e:
    print(f"✗ 測試 8 失敗: {e}")

# 測試 9: 直接寫入 API
try:
    db_manager.execute_write("INSERT OR IGNORE INTO stock_meta (code, name) VALUES ('TEST100', 'Direct Write Test')")
    # 等待寫入完成
    time.sleep(0.2)
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM stock_meta WHERE code='TEST100'")
        result = cur.fetchone()
        if result:
            print(f"✓ 測試 9: 直接寫入 API 成功 - {result[0]}")
            # 清理
            db_manager.execute_write("DELETE FROM stock_meta WHERE code='TEST100'")
        else:
            print(f"✗ 測試 9: 直接寫入 API 失敗")
except Exception as e:
    print(f"✗ 測試 9 失敗: {e}")

print("=" * 50)
print("驗證完成")
print("=" * 50)
