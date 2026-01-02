"""
修復 Supabase stock_meta schema：新增 total_shares 欄位
"""
import os
import sys
import sqlite3
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
try:
    from backend.services.db import SUPABASE_KEY
except:
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

from supabase import create_client, Client

DB_PATH = Path("taiwan_stock.db")

def main():
    print("="*50)
    print("🔧 修復 stock_meta schema")
    print("="*50)
    
    if not SUPABASE_KEY:
        print("❌ Missing SUPABASE_KEY")
        return
    
    print("🔄 Connecting to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. Add missing column via RPC or direct SQL (if service role key)
    print("📝 Adding 'total_shares' column to stock_meta...")
    try:
        # Try using rpc to execute raw SQL (requires service role key with permissions)
        result = supabase.rpc('exec_sql', {'query': 'ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS total_shares bigint;'}).execute()
        print(f"   ✓ Column added via RPC: {result}")
    except Exception as e:
        print(f"   ⚠ RPC failed: {e}")
        print("   ℹ Trying alternative approach...")
        
        # Alternative: just upload with columns that exist
        # Let's check what columns exist in stock_meta
        try:
            # Get one row to see columns
            result = supabase.table('stock_meta').select('*').limit(1).execute()
            if result.data:
                existing_cols = set(result.data[0].keys())
                print(f"   ℹ Existing columns: {existing_cols}")
            else:
                print("   ℹ No existing rows, will try direct insert")
        except Exception as e2:
            print(f"   ⚠ Cannot check schema: {e2}")
    
    # 2. Re-upload stock_meta with only existing columns
    print("\n📤 Re-uploading stock_meta...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM stock_meta")
    rows = cursor.fetchall()
    
    # Get column names from SQLite
    local_cols = set(rows[0].keys()) if rows else set()
    print(f"   ℹ Local columns: {local_cols}")
    
    # Prepare records, excluding total_shares if it causes issues
    records = []
    for row in rows:
        record = dict(row)
        # Remove problematic columns if needed
        # record.pop('total_shares', None)
        records.append(record)
    
    print(f"   ✓ Found {len(records)} meta records.")
    
    # Upload in batches
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            supabase.table('stock_meta').upsert(batch).execute()
            print(f"   ✓ Progress: {min(i+batch_size, len(records))}/{len(records)}")
        except Exception as e:
            print(f"   ❌ Batch {i}-{i+batch_size} failed: {e}")
            # Try without total_shares
            try:
                clean_batch = [{k: v for k, v in r.items() if k != 'total_shares'} for r in batch]
                supabase.table('stock_meta').upsert(clean_batch).execute()
                print(f"   ✓ Progress (without total_shares): {min(i+batch_size, len(records))}/{len(records)}")
            except Exception as e2:
                print(f"   ❌ Clean batch also failed: {e2}")
    
    conn.close()
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
