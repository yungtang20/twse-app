
import os
from supabase import create_client
import json

# Supabase configuration
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_history():
    code = '2330'
    print(f"Checking stock_history for {code}...")
    
    # Get count
    res = supabase.table('stock_history').select('count', count='exact').eq('code', code).execute()
    print(f"Total records for {code}: {res.count}")
    
    # Get latest 5 records
    res = supabase.table('stock_history').select('*').eq('code', code).order('date_int', desc=True).limit(5).execute()
    
    if not res.data:
        print("No data found.")
    else:
        print("Latest 5 records:")
        for row in res.data:
            print(f"Date: {row['date_int']}, Close: {row['close']}, Volume: {row['volume']}")

if __name__ == "__main__":
    check_history()
