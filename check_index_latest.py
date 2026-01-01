import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\nChecking latest date for code '0000'...")
    try:
        res = supabase.table("stock_history") \
            .select("date_int") \
            .eq("code", "0000") \
            .order("date_int", desc=True) \
            .limit(1) \
            .execute()
            
        if res.data:
            print(f"Latest date for 0000: {res.data[0]['date_int']}")
        else:
            print("No records found for code '0000'.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
