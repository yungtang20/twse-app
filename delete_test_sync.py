import os
from supabase import create_client

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    print(f"Connecting to {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Delete TEST_SYNC from all relevant tables
    tables = ['stock_snapshot', 'stock_meta', 'institutional_investors', 'stock_history']
    
    for table in tables:
        try:
            print(f"Deleting TEST_SYNC from {table}...")
            res = supabase.table(table).delete().eq('code', 'TEST_SYNC').execute()
            print(f"  Deleted from {table}")
        except Exception as e:
            print(f"  Error or no record in {table}: {e}")
    
    print("\nDone! TEST_SYNC removed from all tables.")

if __name__ == "__main__":
    main()
