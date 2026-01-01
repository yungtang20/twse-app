import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\n1. Checking latest dates...")
    try:
        h_res = supabase.table("stock_history").select("date_int").order("date_int", desc=True).limit(1).execute()
        i_res = supabase.table("institutional_investors").select("date_int").order("date_int", desc=True).limit(1).execute()
        
        h_date = h_res.data[0]['date_int'] if h_res.data else None
        i_date = i_res.data[0]['date_int'] if i_res.data else None
        
        print(f"Latest Stock History Date: {h_date}")
        print(f"Latest Institutional Date: {i_date}")
        
    except Exception as e:
        print(f"Error checking dates: {e}")

    print("\n2. Checking data for 3715 (定穎投控)...")
    try:
        # Check specific stock
        res = supabase.table("institutional_investors") \
            .select("*") \
            .eq("code", "3715") \
            .order("date_int", desc=True) \
            .limit(1) \
            .execute()
            
        if res.data:
            print("Found record for 3715:")
            print(json.dumps(res.data[0], indent=2, ensure_ascii=False))
        else:
            print("No institutional record found for 3715.")

    except Exception as e:
        print(f"Error checking 3715: {e}")

if __name__ == "__main__":
    main()
