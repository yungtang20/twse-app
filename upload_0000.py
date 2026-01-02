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
    for k, v in record.items():
        if v is None:
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

try:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("Fetching 0000 from local DB...")
    cursor.execute("SELECT * FROM stock_snapshot WHERE code='0000'")
    row = cursor.fetchone()
    
    if row:
        record = dict(row)
        print(f"Found 0000: {record['name']}")
        
        # Upload to Supabase
        print("Uploading to Supabase...")
        data = sanitize_record(record)
        supabase.table("stock_snapshot").upsert(data).execute()
        print("✅ Upload success!")
    else:
        print("❌ Local 0000 not found!")

    # Also check stock_meta for 0000
    cursor.execute("SELECT * FROM stock_meta WHERE code='0000'")
    row = cursor.fetchone()
    if row:
        print("Uploading stock_meta for 0000...")
        record = dict(row)
        if 'total_shares' in record:
            del record['total_shares']
        supabase.table("stock_meta").upsert(record).execute()
        print("✅ Meta upload success!")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    conn.close()
