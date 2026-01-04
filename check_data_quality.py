
import os
from supabase import create_client
import json

# Supabase configuration
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_data():
    print("Checking institutional_investors data...")
    # Get latest date
    res = supabase.table('institutional_investors').select('date_int').order('date_int', desc=True).limit(1).execute()
    if not res.data:
        print("No institutional data found.")
        return
    
    latest_date = res.data[0]['date_int']
    print(f"Latest date: {latest_date}")

    # Check a few records for net buy/sell
    res = supabase.table('institutional_investors').select('*').eq('date_int', latest_date).limit(5).execute()
    print("\nSample Institutional Data:")
    for row in res.data:
        print(f"Code: {row['code']}, Foreign Net: {row.get('foreign_net')}, Trust Net: {row.get('trust_net')}")

    print("\nChecking stock_snapshot data...")
    # Check a few records for streaks
    codes = [row['code'] for row in res.data]
    res = supabase.table('stock_snapshot').select('*').in_('code', codes).execute()
    print("\nSample Snapshot Data:")
    for row in res.data:
        print(f"Code: {row['code']}, Foreign Streak: {row.get('foreign_streak')}, Trust Streak: {row.get('trust_streak')}")

if __name__ == "__main__":
    check_data()
