import sqlite3
from supabase import create_client

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    # Check local schema
    print("=== LOCAL stock_snapshot columns ===")
    conn = sqlite3.connect('d:\\twse\\taiwan_stock.db')
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(stock_snapshot)")
    local_cols = set()
    for row in cur.fetchall():
        col_name = row[1]
        local_cols.add(col_name)
        print(f"  {col_name}")
    conn.close()
    
    # Check Supabase schema
    print("\n=== SUPABASE stock_snapshot columns ===")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table('stock_snapshot').select('*').limit(1).execute()
    cloud_cols = set()
    if res.data:
        cloud_cols = set(res.data[0].keys())
        for col in sorted(cloud_cols):
            print(f"  {col}")
    
    # Find missing columns
    print("\n=== MISSING in Supabase ===")
    missing = local_cols - cloud_cols
    for col in sorted(missing):
        print(f"  {col}")
    
    # Key columns for six-dim scan
    print("\n=== SIX-DIM REQUIRED COLUMNS ===")
    six_dim_cols = ['macd', 'signal', 'daily_k', 'daily_d', 'lwr', 'bbi', 'mtm']
    for col in six_dim_cols:
        status = "✓" if col in cloud_cols else "✗ MISSING"
        print(f"  {col}: {status}")

if __name__ == "__main__":
    main()
