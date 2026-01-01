import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\n1. Checking 3715 institutional data...")
    res = supabase.table("institutional_investors") \
        .select("*") \
        .eq("code", "3715") \
        .order("date_int", desc=True) \
        .limit(1) \
        .execute()
    
    if res.data:
        print("Record for 3715:")
        print(json.dumps(res.data[0], indent=2, ensure_ascii=False))
    else:
        print("No record found for 3715!")
        
    print("\n2. Latest institutional date:")
    res2 = supabase.table("institutional_investors") \
        .select("date_int") \
        .order("date_int", desc=True) \
        .limit(1) \
        .execute()
    if res2.data:
        print(f"Latest date: {res2.data[0]['date_int']}")

if __name__ == "__main__":
    main()
