import os
import sys
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

try:
    # Check stock_snapshot count
    print("Checking 'stock_snapshot' count...")
    res = supabase.table("stock_snapshot").select("code", count="exact").limit(1).execute()
    print(f"stock_snapshot count: {res.count}")

    # Check stock_history count
    print("Checking 'stock_history' count...")
    res = supabase.table("stock_history").select("code", count="exact").limit(1).execute()
    print(f"stock_history count: {res.count}")
    
    # Check specific stock 0000
    print("Checking '0000' in stock_snapshot...")
    res = supabase.table("stock_snapshot").select("*").eq("code", "0000").execute()
    if res.data:
        print(f"Found 0000: {res.data[0]}")
    else:
        print("❌ 0000 NOT FOUND in stock_snapshot")

except Exception as e:
    print(f"❌ Error: {e}")
