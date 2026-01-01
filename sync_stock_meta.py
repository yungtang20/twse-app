import sqlite3
import math
from supabase import create_client
import os

# Supabase Credentials
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

DB_PATH = "d:\\twse\\taiwan_stock.db"

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

    # Sync stock_meta
    print("\nSyncing stock_meta...")
    try:
        cur.execute("SELECT COUNT(*) FROM stock_meta")
        total = cur.fetchone()[0]
        print(f"Found {total} records in local stock_meta.")

        BATCH_SIZE = 100
        total_batches = math.ceil(total / BATCH_SIZE)

        # Select available columns
        cur.execute("SELECT code, name, market_type, industry, list_date, delist_date, status FROM stock_meta")
        
        success_count = 0
        for i in range(total_batches):
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows: break

            data = []
            for row in rows:
                item = dict(row)
                # Manually add is_normal since it's missing in local DB but required/default in Supabase
                item['is_normal'] = 1 
                data.append(item)

            try:
                supabase.table("stock_meta").upsert(data).execute()
                success_count += len(data)
                print(f"\r  Progress: {i+1}/{total_batches} ({success_count}/{total})", end="")
            except Exception as e:
                print(f"\n  Error in batch {i+1}: {e}")

        print(f"\nSync complete. {success_count}/{total} records synced.")

    except Exception as e:
        print(f"Error syncing stock_meta: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
