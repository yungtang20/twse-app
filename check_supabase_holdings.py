import os
from supabase import create_client

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# 1. Check for holding data in stock_snapshot
print("Checking stock_snapshot for holding data...")
res = supabase.table('stock_snapshot').select('code, foreign_holding_shares, trust_holding_shares').limit(5).execute()
print(res.data)

# 2. Delete TEST_SYNC record
print("\nDeleting TEST_SYNC record...")
try:
    res = supabase.table('stock_snapshot').delete().eq('code', 'TEST_SYNC').execute()
    print(f"Deleted from stock_snapshot: {res.data}")
    
    res = supabase.table('stock_meta').delete().eq('code', 'TEST_SYNC').execute()
    print(f"Deleted from stock_meta: {res.data}")
except Exception as e:
    print(f"Error deleting: {e}")
