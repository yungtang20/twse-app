import os
from supabase import create_client

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\nChecking stock_snapshot count...")
    try:
        res = supabase.table("stock_snapshot").select("code", count="exact").execute()
        print(f"Total records in stock_snapshot: {res.count}")
        
        if res.count < 10:
            print("Warning: Low record count! Listing all codes:")
            for item in res.data:
                print(item['code'])
                
    except Exception as e:
        with open("snapshot_count.txt", "w") as f:
            f.write(f"Error: {e}")
            
    with open("snapshot_count.txt", "w") as f:
        if 'res' in locals():
            f.write(f"Total records in stock_snapshot: {res.count}\n")
            if res.count < 10:
                f.write("Warning: Low record count! Listing all codes:\n")
                for item in res.data:
                    f.write(f"{item['code']}\n")

if __name__ == "__main__":
    main()
