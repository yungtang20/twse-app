import os
import sys
import sqlite3
from supabase import create_client

# Load config or env
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
try:
    from backend.services.db import SUPABASE_KEY
except:
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_KEY:
    print("❌ Missing SUPABASE_KEY")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
DB_PATH = "taiwan_stock.db"

def sanitize_record(record):
    cleaned = {}
    int_cols = ['volume', 'amount', 'foreign_buy', 'trust_buy', 'dealer_buy', 'tdcc_count']
    for k, v in record.items():
        if v is None:
            cleaned[k] = None
        elif k in int_cols:
            try:
                cleaned[k] = int(float(v))
            except:
                cleaned[k] = 0
        else:
            cleaned[k] = v
    return cleaned

try:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("Fetching 0000 history from local DB...")
    cursor.execute("SELECT * FROM stock_history WHERE code='0000'")
    rows = cursor.fetchall()
    
    if rows:
        print(f"Found {len(rows)} records for 0000")
        
        # Prepare batch
        batch = []
        for row in rows:
            batch.append(sanitize_record(dict(row)))
            
        # Upload in chunks
        chunk_size = 100
        total = len(batch)
        print(f"Uploading {total} records to Supabase...")
        
        for i in range(0, total, chunk_size):
            chunk = batch[i:i+chunk_size]
            supabase.table("stock_history").upsert(chunk).execute()
            print(f"Uploaded {i+len(chunk)}/{total}")
            
        print("✅ Upload success!")
    else:
        print("❌ Local 0000 history not found!")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    conn.close()
