import sqlite3
import math
from supabase import create_client
import os
import time

# Supabase Credentials
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

DB_PATH = "d:\\twse\\taiwan_stock.db"

def safe_int(val):
    if val is None:
        return None
    try:
        return int(float(val))
    except:
        return None

def sync_table(supabase, cur, table_name, columns=None, batch_size=500, limit=None, order_by=None, int_cols=None):
    print(f"\nSyncing {table_name}...")
    try:
        # Get total count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cur.fetchone()[0]
        print(f"Found {total} records in local {table_name}.")
        
        if limit:
            print(f"Syncing last {limit} records...")
            total = min(total, limit)

        # Build query
        col_str = "*"
        if columns:
            col_str = ", ".join(columns)
            
        query = f"SELECT {col_str} FROM {table_name}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"
            
        cur.execute(query)
        
        total_batches = math.ceil(total / batch_size)
        success_count = 0
        
        for i in range(total_batches):
            rows = cur.fetchmany(batch_size)
            if not rows: break
            
            data = []
            for row in rows:
                item = dict(row)
                # Type casting
                if int_cols:
                    for col in int_cols:
                        if col in item:
                            item[col] = safe_int(item[col])
                data.append(item)
            
            try:
                supabase.table(table_name).upsert(data).execute()
                success_count += len(data)
                print(f"\r  Progress: {i+1}/{total_batches} ({success_count}/{total})", end="")
            except Exception as e:
                print(f"\n  Error in batch {i+1}: {e}")
                
        print(f"\n{table_name} sync complete. {success_count}/{total} records synced.")
        
    except Exception as e:
        print(f"Error syncing {table_name}: {e}")

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

    # 1. Sync stock_snapshot
    # No int casting needed usually, but let's be safe for volume
    sync_table(supabase, cur, "stock_snapshot", batch_size=500, int_cols=['volume'])

    # 2. Sync institutional_investors
    print("\nFetching latest date from local DB for institutional_investors...")
    cur.execute("SELECT MAX(date_int) FROM institutional_investors")
    max_date = cur.fetchone()[0]
    if max_date:
        cutoff_date = max_date - 200
        print(f"Syncing institutional_investors since {cutoff_date}...")
        
        cur.execute(f"SELECT COUNT(*) FROM institutional_investors WHERE date_int >= {cutoff_date}")
        total = cur.fetchone()[0]
        print(f"Records to sync: {total}")
        
        cur.execute(f"SELECT * FROM institutional_investors WHERE date_int >= {cutoff_date}")
        
        batch_size = 1000
        total_batches = math.ceil(total / batch_size)
        success_count = 0
        
        int_cols = [
            'foreign_buy', 'foreign_sell', 'foreign_net',
            'trust_buy', 'trust_sell', 'trust_net',
            'dealer_buy', 'dealer_sell', 'dealer_net'
        ]

        for i in range(total_batches):
            rows = cur.fetchmany(batch_size)
            if not rows: break
            data = []
            for row in rows:
                item = dict(row)
                for col in int_cols:
                    if col in item:
                        item[col] = safe_int(item[col])
                data.append(item)
                
            try:
                supabase.table("institutional_investors").upsert(data).execute()
                success_count += len(data)
                print(f"\r  Progress: {i+1}/{total_batches} ({success_count}/{total})", end="")
            except Exception as e:
                print(f"\n  Error in batch {i+1}: {e}")
        print(f"\nInstitutional sync complete.")

    # 3. Sync stock_history
    cur.execute("SELECT MAX(date_int) FROM stock_history")
    max_hist_date = cur.fetchone()[0]
    if max_hist_date:
        cutoff_hist = max_hist_date - 200
        print(f"\nSyncing stock_history since {cutoff_hist}...")
        
        cur.execute(f"SELECT COUNT(*) FROM stock_history WHERE date_int >= {cutoff_hist}")
        total = cur.fetchone()[0]
        print(f"Records to sync: {total}")
        
        cur.execute(f"SELECT * FROM stock_history WHERE date_int >= {cutoff_hist}")
        
        batch_size = 1000
        total_batches = math.ceil(total / batch_size)
        success_count = 0
        
        hist_int_cols = ['volume', 'foreign_buy', 'trust_buy', 'dealer_buy', 'tdcc_count']

        for i in range(total_batches):
            rows = cur.fetchmany(batch_size)
            if not rows: break
            data = []
            for row in rows:
                item = dict(row)
                for col in hist_int_cols:
                    if col in item:
                        item[col] = safe_int(item[col])
                data.append(item)
            try:
                supabase.table("stock_history").upsert(data).execute()
                success_count += len(data)
                print(f"\r  Progress: {i+1}/{total_batches} ({success_count}/{total})", end="")
            except Exception as e:
                print(f"\n  Error in batch {i+1}: {e}")
        print(f"\nHistory sync complete.")

    conn.close()

if __name__ == "__main__":
    main()
