import os
import sys
from supabase import create_client

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
    print("Checking 'stock_history' for 0000...")
    res = supabase.table("stock_history").select("date_int", count="exact").eq("code", "0000").execute()
    print(f"0000 history count: {res.count}")
    
    if res.count == 0:
        print("❌ 0000 history is MISSING!")
    else:
        print("✅ 0000 history exists.")

except Exception as e:
    print(f"❌ Error: {e}")
