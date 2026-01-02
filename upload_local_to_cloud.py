import os
import sys
import time
import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from supabase import create_client, Client

# Add root directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load config or env
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
# Try to get key from backend/services/db.py
try:
    from backend.services.db import SUPABASE_KEY
except:
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

DB_PATH = Path("taiwan_stock.db")

def get_sqlite_connection():
    return sqlite3.connect(DB_PATH)

# Columns that should be integers in Supabase
INT_COLUMNS = {'volume', 'foreign_buy', 'trust_buy', 'dealer_buy', 'foreign_net', 'trust_net', 'dealer_net', 'date_int', 'tdcc_count'}

def sanitize_record(record: Dict) -> Dict:
    """Clean record data for Supabase"""
    cleaned = {}
    for k, v in record.items():
        if v is None:
            cleaned[k] = None
        elif isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                cleaned[k] = None
            elif k in INT_COLUMNS:
                # Convert float to int for bigint columns
                cleaned[k] = int(v)
            else:
                cleaned[k] = v
        else:
            cleaned[k] = v
    return cleaned

def upload_batch(supabase: Client, table: str, data: List[Dict], batch_size: int = 1000):
    if not data:
        return
    
    # Sanitize data
    data = [sanitize_record(r) for r in data]
    
    total = len(data)
    print(f"📤 Uploading {total} records to '{table}'...")
    
    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        try:
            # Using upsert to be safe
            supabase.table(table).upsert(batch).execute()
            print(f"  ✓ Progress: {min(i+batch_size, total)}/{total}")
        except Exception as e:
            print(f"  ❌ Batch {i}-{i+batch_size} failed: {e}")
            # Try row by row for this batch to identify error?
            # For now just skip to save time, or maybe retry smaller chunks
            time.sleep(1)

def main():
    if not SUPABASE_KEY:
        print("❌ Missing SUPABASE_KEY")
        return
    
    if not DB_PATH.exists():
        print("❌ Local database 'stocks.db' not found.")
        return

    print("🔄 Connecting to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    conn = get_sqlite_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Upload Stock History (Limit to last 1 year to save time)
    print("\n[1] Reading local 'stock_history' (last 1 year)...")
    # Get latest date
    cursor.execute("SELECT MAX(date_int) FROM stock_history")
    max_date = cursor.fetchone()[0]
    if not max_date:
        print("  ⚠ No local history found.")
    else:
        # Calculate cutoff (roughly 1 year)
        # 20251229 -> 20241229 (diff 10000)
        cutoff_date = max_date - 10000 
        
        cursor.execute("""
            SELECT code, date_int, open, high, low, close, volume, amount,
                   foreign_buy, trust_buy, dealer_buy,
                   tdcc_count, large_shareholder_pct
            FROM stock_history
            WHERE date_int > ?
        """, (cutoff_date,))
        
        rows = cursor.fetchall()
        records = []
        for row in rows:
            records.append(dict(row))
            
        print(f"  ✓ Found {len(records)} records.")
        upload_batch(supabase, "stock_history", records)

    # 2. Upload Institutional Investors
    print("\n[2] Reconstructing 'institutional_investors' from history...")
    inst_records = []
    for r in records:
        # Only if there is non-zero data
        if r['foreign_buy'] != 0 or r['trust_buy'] != 0 or r['dealer_buy'] != 0:
            inst_records.append({
                "code": r['code'],
                "date_int": r['date_int'],
                "foreign_net": r['foreign_buy'],
                "trust_net": r['trust_buy'],
                "dealer_net": r['dealer_buy']
            })
            
    print(f"  ✓ Found {len(inst_records)} institutional records.")
    upload_batch(supabase, "institutional_investors", inst_records)

    # 3. Upload Stock Snapshot (Indicators)
    print("\n[3] Uploading 'stock_snapshot' (Real-time & Indicators)...")
    cursor.execute("SELECT * FROM stock_snapshot")
    rows = cursor.fetchall()
    snapshot_records = [dict(row) for row in rows]
    
    print(f"  ✓ Found {len(snapshot_records)} snapshot records.")
    upload_batch(supabase, "stock_snapshot", snapshot_records)

    # 4. Upload Stock Meta (Basic Info)
    print("\n[4] Uploading 'stock_meta'...")
    cursor.execute("SELECT * FROM stock_meta")
    rows = cursor.fetchall()
    meta_records = [dict(row) for row in rows]
    print(f"  ✓ Found {len(meta_records)} meta records.")
    upload_batch(supabase, "stock_meta", meta_records)

    conn.close()
    print("\n✅ Restore complete!")

if __name__ == "__main__":
    main()
