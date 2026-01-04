#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
從本地 SQLite 資料庫上傳資料到 Supabase
"""
import sqlite3
import json
import sys
import ssl
import math

# Disable SSL verification
ssl._create_default_https_context = ssl._create_unverified_context

try:
    from supabase import create_client
except ImportError:
    print("❌ 請先安裝 supabase: pip install supabase")
    sys.exit(1)

# Supabase 設定
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

# 本地資料庫路徑
DB_PATH = "taiwan_stock.db"

def upload_to_supabase(supabase, table: str, data: list, batch_size: int = 500):
    """批次上傳到 Supabase"""
    if not data:
        print(f"  ⚠ {table}: 無資料")
        return 0
    
    print(f"📤 上傳 {table} ({len(data)} 筆)...")
    
    total_batches = math.ceil(len(data) / batch_size)
    success_count = 0
    
    for i in range(total_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, len(data))
        batch = data[start:end]
        
        try:
            supabase.table(table).upsert(batch).execute()
            success_count += len(batch)
            
            if (i + 1) % 5 == 0 or (i + 1) == total_batches:
                print(f"  進度: {i + 1}/{total_batches} ({success_count}/{len(data)})")
        except Exception as e:
            print(f"  ❌ Batch {i + 1} 失敗: {e}")
    
    print(f"  ✓ {table}: {success_count}/{len(data)} 筆")
    return success_count

def main():
    print("=" * 50)
    print("📤 從本地 SQLite 上傳到 Supabase")
    print("=" * 50)
    
    # 連接 Supabase
    print("\n[Step 1] 連接 Supabase...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✓ Supabase 連線成功")
    except Exception as e:
        print(f"❌ Supabase 連線失敗: {e}")
        return
    
    # 連接本地資料庫
    print("\n[Step 2] 連接本地資料庫...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        print(f"✓ 已連接 {DB_PATH}")
    except Exception as e:
        print(f"❌ 無法連接資料庫: {e}")
        return
    
    # 上傳 institutional_investors
    print("\n[Step 3] 上傳法人買賣超 (institutional_investors)...")
    try:
        cursor.execute("""
            SELECT code, date_int, 
                   COALESCE(foreign_buy, 0) - COALESCE(foreign_sell, 0) as foreign_net,
                   COALESCE(trust_buy, 0) - COALESCE(trust_sell, 0) as trust_net,
                   COALESCE(dealer_buy, 0) - COALESCE(dealer_sell, 0) as dealer_net
            FROM institutional_investors
            ORDER BY date_int DESC
            LIMIT 50000
        """)
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            data.append({
                "code": row["code"],
                "date_int": row["date_int"],
                "foreign_net": row["foreign_net"],
                "trust_net": row["trust_net"],
                "dealer_net": row["dealer_net"]
            })
        
        if data:
            upload_to_supabase(supabase, "institutional_investors", data)
        else:
            print("  ⚠ 本地資料庫無 institutional_investors 資料")
    except Exception as e:
        print(f"  ❌ 讀取 institutional_investors 失敗: {e}")
    
    # 上傳 stock_snapshot
    print("\n[Step 4] 上傳股票快照 (stock_snapshot)...")
    try:
        cursor.execute("""
            SELECT * FROM stock_snapshot LIMIT 5000
        """)
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            record = dict(row)
            # 清理 None 和 infinity 值
            for key in record:
                val = record[key]
                if val is None:
                    record[key] = 0 if key not in ['code', 'name', 'date'] else record[key]
                elif isinstance(val, float):
                    # 處理 infinity 和 NaN
                    import math
                    if math.isinf(val) or math.isnan(val):
                        record[key] = 0
            data.append(record)
        
        if data:
            upload_to_supabase(supabase, "stock_snapshot", data)
        else:
            print("  ⚠ 本地資料庫無 stock_snapshot 資料")
    except Exception as e:
        print(f"  ❌ 讀取 stock_snapshot 失敗: {e}")
    
    # 上傳 stock_history (最近 30 天)
    print("\n[Step 5] 上傳股票歷史 (stock_history, 最近30天)...")
    try:
        cursor.execute("""
            SELECT code, date_int, open, high, low, close, volume, amount
            FROM stock_history
            WHERE date_int >= (SELECT MAX(date_int) - 300 FROM stock_history)
            ORDER BY date_int DESC
        """)
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            data.append({
                "code": row["code"],
                "date_int": row["date_int"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "amount": row["amount"] if row["amount"] else 0
            })
        
        if data:
            upload_to_supabase(supabase, "stock_history", data, batch_size=1000)
        else:
            print("  ⚠ 本地資料庫無 stock_history 資料")
    except Exception as e:
        print(f"  ❌ 讀取 stock_history 失敗: {e}")
    
    conn.close()
    
    print("\n" + "=" * 50)
    print("✅ 上傳完成!")
    print("=" * 50)

if __name__ == "__main__":
    main()
