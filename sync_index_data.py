import sqlite3
import math
from supabase import create_client
import os

# Supabase Credentials
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

DB_PATH = "d:\\twse\\taiwan_stock.db"

def safe_int(val):
    if val is None: return None
    try: return int(float(val))
    except: return None

def main():
    print(f"Connecting to Supabase: {SUPABASE_URL}...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")
        return

    if not os.path.exists(DB_PATH):
        print(f"Local database not found at {DB_PATH}")
        return

    print(f"Connecting to local DB: {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Sync stock_meta for 0000
    print("\nSyncing stock_meta for 0000...")
    cur.execute("SELECT * FROM stock_meta WHERE code = '0000'")
    row = cur.fetchone()
    if row:
        data = dict(row)
        # Ensure required fields
        if 'is_normal' not in data: data['is_normal'] = 1
        
        # Filter columns to match Supabase schema
        valid_cols = ['code', 'name', 'market_type', 'industry', 'list_date', 'delist_date', 'status', 'is_normal']
        filtered_data = {k: v for k, v in data.items() if k in valid_cols}
        
        try:
            supabase.table("stock_meta").upsert([filtered_data]).execute()
            print("Successfully synced stock_meta for 0000")
        except Exception as e:
            print(f"Error syncing stock_meta: {e}")
    else:
        print("Local stock_meta for 0000 not found!")

    # 2. Sync stock_snapshot for 0000
    print("\nSyncing stock_snapshot for 0000...")
    cur.execute("SELECT * FROM stock_snapshot WHERE code = '0000'")
    row = cur.fetchone()
    if row:
        data = dict(row)
        if 'volume' in data: data['volume'] = safe_int(data['volume'])
        
        try:
            supabase.table("stock_snapshot").upsert([data]).execute()
            print("Successfully synced stock_snapshot for 0000")
        except Exception as e:
            print(f"Error syncing stock_snapshot: {e}")
    else:
        print("Local stock_snapshot for 0000 not found!")

    conn.close()

if __name__ == "__main__":
    main()
