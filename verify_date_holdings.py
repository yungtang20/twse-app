import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\n1. Checking 3715 institutional data at date 20251226...")
    res = supabase.table("institutional_investors") \
        .select("*") \
        .eq("code", "3715") \
        .eq("date_int", 20251226) \
        .execute()
    
    if res.data:
        print("Record for 3715 at 20251226:")
        print(json.dumps(res.data[0], indent=2, ensure_ascii=False))
    else:
        print("No record found for 3715 at 20251226!")
        
    print("\n2. Checking ANY stock with valid holdings at 20251226...")
    res2 = supabase.table("institutional_investors") \
        .select("code, date_int, foreign_holding_shares, foreign_holding_pct") \
        .eq("date_int", 20251226) \
        .not_.is_("foreign_holding_shares", "null") \
        .limit(5) \
        .execute()
    if res2.data:
        for d in res2.data:
            print(d)
    else:
        print("No records with holdings at 20251226!")

if __name__ == "__main__":
    main()
