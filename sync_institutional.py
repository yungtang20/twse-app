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

def safe_float(val):
    if val is None: return None
    try: return float(val)
    except: return None

def main():
    print(f"Connecting to Supabase: {SUPABASE_URL}...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")
        return

    print(f"Connecting to local DB: {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get latest date that HAS VALID HOLDINGS from local institutional_investors
    # (not just the absolute latest date, which may have NULL holdings)
    cur.execute("""
        SELECT MAX(date_int) as max_date FROM institutional_investors 
        WHERE foreign_holding_shares IS NOT NULL AND foreign_holding_shares != 0
    """)
    max_date = cur.fetchone()['max_date']
    print(f"Latest local institutional date with holdings: {max_date}")

    if not max_date:
        print("No local data found.")
        return

    # Fetch data for this date
    print(f"Fetching local data for {max_date}...")
    cur.execute("SELECT * FROM institutional_investors WHERE date_int = ?", (max_date,))
    rows = cur.fetchall()
    
    print(f"Found {len(rows)} records. Syncing to Supabase...")
    
    batch_size = 100
    batch = []
    
    for row in rows:
        # Filter for Supabase schema
        # We only need code, date_int, and the holding columns we just added
        # Supabase seems to lack dealer_buy etc, or uses different names.
        # To be safe, we only sync what we need for the rankings.
        item = dict(row)
        allowed_keys = {
            'code', 'date_int', 
            'foreign_holding_shares', 'foreign_holding_pct',
            'trust_holding_shares', 'trust_holding_pct',
            'dealer_holding_shares', 'dealer_holding_pct'
        }
        
        filtered_item = {k: v for k, v in item.items() if k in allowed_keys}
        
        batch.append(filtered_item)
        
        if len(batch) >= batch_size:
            try:
                supabase.table("institutional_investors").upsert(batch).execute()
                print(f"Synced {len(batch)} records...")
            except Exception as e:
                with open("sync_error.txt", "w") as f:
                    f.write(str(e))
                print(f"Error syncing batch: {e}")
                break
            batch = []

    if batch:
        try:
            supabase.table("institutional_investors").upsert(batch).execute()
            print(f"Synced remaining {len(batch)} records.")
        except Exception as e:
            with open("sync_error.txt", "w") as f:
                f.write(str(e))
            print(f"Error syncing final batch: {e}")

    conn.close()

if __name__ == "__main__":
    main()
