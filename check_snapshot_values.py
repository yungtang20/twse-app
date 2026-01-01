import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\nChecking foreign_buy in stock_snapshot...")
    try:
        # Select rows where foreign_buy is not null and not 0
        res = supabase.table("stock_snapshot") \
            .select("code, name, foreign_buy, trust_buy, dealer_buy") \
            .neq("foreign_buy", 0) \
            .limit(5) \
            .execute()
            
        if res.data:
            print("Found records with non-zero foreign_buy:")
            print(json.dumps(res.data, indent=2, ensure_ascii=False))
        else:
            print("No records found with non-zero foreign_buy.")
            
            # Check if column exists and has any data
            print("Checking raw sample...")
            res = supabase.table("stock_snapshot").select("foreign_buy").limit(5).execute()
            print(json.dumps(res.data, indent=2))

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
