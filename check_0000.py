
import os
from supabase import create_client

# Supabase configuration
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_0000():
    print("Checking stock_history for 0000 (TAIEX)...")
    res = supabase.table('stock_history').select('count', count='exact').eq('code', '0000').execute()
    print(f"Total records for 0000: {res.count}")
    
    if res.count == 0:
        print("No data for 0000. This explains the mock data warning.")
    else:
        print("Data exists for 0000.")

if __name__ == "__main__":
    check_0000()
