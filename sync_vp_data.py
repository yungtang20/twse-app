import sqlite3
from supabase import create_client

SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

def main():
    # 1. Check local VP data
    print("=== 檢查本地資料庫 VP 資料 ===")
    conn = sqlite3.connect('d:/twse/taiwan_stock.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Check 7810
    cur.execute("SELECT code, name, vp_poc, vp_high, vp_low FROM stock_snapshot WHERE code = '7810'")
    row = cur.fetchone()
    if row:
        print(f"  7810: vp_poc={row['vp_poc']}, vp_high={row['vp_high']}, vp_low={row['vp_low']}")
    
    # Count VP data availability
    cur.execute("SELECT COUNT(*) FROM stock_snapshot WHERE vp_poc IS NOT NULL AND vp_poc != 0")
    local_count = cur.fetchone()[0]
    print(f"  本地有 VP 資料的股票數: {local_count}")
    
    # 2. Check Supabase VP data
    print("\n=== 檢查 Supabase VP 資料 ===")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    res = supabase.table('stock_snapshot').select('code, vp_poc', count='exact').not_.is_('vp_poc', 'null').execute()
    print(f"  Supabase 有 VP 資料的股票數: {res.count}")
    
    # 3. Sync VP data from local to Supabase
    print("\n=== 同步 VP 資料到 Supabase ===")
    cur.execute("""
        SELECT code, vp_poc, vp_high, vp_low 
        FROM stock_snapshot 
        WHERE vp_poc IS NOT NULL AND vp_poc != 0
    """)
    rows = cur.fetchall()
    
    batch_size = 100
    synced = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        updates = []
        for row in batch:
            updates.append({
                'code': row['code'],
                'vp_poc': row['vp_poc'],
                'vp_high': row['vp_high'],
                'vp_low': row['vp_low']
            })
        try:
            # Upsert VP data
            supabase.table('stock_snapshot').upsert(updates, on_conflict='code').execute()
            synced += len(batch)
            print(f"  已同步: {synced}/{len(rows)}")
        except Exception as e:
            print(f"  同步錯誤: {e}")
    
    print(f"\n✅ 完成！共同步 {synced} 筆 VP 資料")
    conn.close()

if __name__ == "__main__":
    main()
