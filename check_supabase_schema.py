import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("Fetching one record from stock_data...")
    try:
        res = supabase.table("stock_data").select("*").limit(1).execute()
        if res.data:
            print("Columns in stock_data:")
            print(json.dumps(res.data[0], indent=2, ensure_ascii=False))
        else:
            print("stock_data is empty.")
    except Exception as e:
        print(f"Error fetching stock_data: {e}")

    print("\nFetching one record from stock_meta...")
    try:
        res = supabase.table("stock_meta").select("*").limit(1).execute()
        if res.data:
            print("Columns in stock_meta:")
            print(json.dumps(res.data[0], indent=2, ensure_ascii=False))
        else:
            print("stock_meta is empty or does not exist.")
    except Exception as e:
        print(f"Error fetching stock_meta: {e}")

if __name__ == "__main__":
    main()
