import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\nChecking stock_snapshot columns for holdings...")
    try:
        # Fetch one record to see keys
        res = supabase.table("stock_snapshot").select("*").limit(1).execute()
        
        if res.data:
            keys = res.data[0].keys()
            print("Columns found in Supabase stock_snapshot:")
            holdings_cols = [k for k in keys if 'holding' in k or 'share' in k]
            print(holdings_cols)
            
            print("\nSample data for holdings:")
            print({k: res.data[0][k] for k in holdings_cols})
        else:
            print("No records found in stock_snapshot.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
