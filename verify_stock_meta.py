import os
from supabase import create_client
import json

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\nChecking stock_meta table...")
    try:
        # Try to insert a dummy record to verify write access and schema, or just select
        # Since we just want to verify existence, select is safer.
        # But to verify columns, we might want to see if we can select specific columns.
        res = supabase.table("stock_meta").select("*").limit(1).execute()
        print("Successfully queried stock_meta.")
        if res.data:
            print("Table is not empty. Sample data:")
            print(json.dumps(res.data[0], indent=2, ensure_ascii=False))
        else:
            print("Table is empty but exists.")
            
        # Verify columns by trying to select them
        print("Verifying columns...")
        try:
            res = supabase.table("stock_meta").select("code,name,market_type,industry,status").limit(1).execute()
            print("Columns verification passed.")
        except Exception as e:
            print(f"Column verification failed: {e}")

    except Exception as e:
        print(f"Error accessing stock_meta: {e}")

if __name__ == "__main__":
    main()
