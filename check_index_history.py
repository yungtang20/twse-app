import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\nChecking stock_history for code '0000'...")
    try:
        res = supabase.table("stock_history") \
            .select("*") \
            .eq("code", "0000") \
            .limit(5) \
            .execute()
            
        if res.data:
            print(f"Found {len(res.data)} records for 0000:")
            print(json.dumps(res.data, indent=2, ensure_ascii=False))
        else:
            print("No records found for code '0000'.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
