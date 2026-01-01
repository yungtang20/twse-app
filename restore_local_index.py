import sqlite3
from supabase import create_client
import os

# Supabase Credentials
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

DB_PATH = "d:\\twse\\taiwan_stock.db"

def main():
    print(f"Connecting to Supabase: {SUPABASE_URL}...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")
        return

    print(f"Connecting to local DB: {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Restore stock_meta
    print("\nRestoring stock_meta for 0000...")
    meta = {
        'code': '0000',
        'name': '加權指數',
        'market_type': 'TWSE',
        'status': 1
    }
    try:
        cur.execute("INSERT OR REPLACE INTO stock_meta (code, name, market_type, status) VALUES (?, ?, ?, ?)",
                    (meta['code'], meta['name'], meta['market_type'], meta['status']))
        print("Inserted stock_meta for 0000")
    except Exception as e:
        print(f"Error inserting stock_meta: {e}")

    # 2. Restore stock_snapshot
    print("\nRestoring stock_snapshot for 0000...")
    try:
        res = supabase.table("stock_history").select("*").eq("code", "0000").order("date_int", desc=True).limit(1).execute()
        if res.data:
            latest = res.data[0]
            # Convert date_int to date string YYYY-MM-DD
            d_str = str(latest['date_int'])
            date_fmt = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
            
            snap = {
                'code': '0000',
                'name': '加權指數',
                'date': date_fmt,
                'open': latest['open'],
                'high': latest['high'],
                'low': latest['low'],
                'close': latest['close'],
                'volume': latest['volume'],
                'amount': latest['amount'],
                'close_prev': 0
            }
            # Try to get prev close
            res_prev = supabase.table("stock_history").select("close").eq("code", "0000").lt("date_int", latest['date_int']).order("date_int", desc=True).limit(1).execute()
            if res_prev.data:
                snap['close_prev'] = res_prev.data[0]['close']
            
            # Insert
            cols = ', '.join(snap.keys())
            placeholders = ', '.join(['?'] * len(snap))
            sql = f"INSERT OR REPLACE INTO stock_snapshot ({cols}) VALUES ({placeholders})"
            cur.execute(sql, list(snap.values()))
            print(f"Inserted stock_snapshot for 0000 from history {latest['date_int']}")
        else:
            print("No history found in Supabase for 0000 to build snapshot")
    except Exception as e:
        print(f"Error restoring snapshot: {e}")

    # 3. Restore stock_history
    print("\nRestoring stock_history for 0000 (limit 2000)...")
    try:
        res = supabase.table("stock_history").select("*").eq("code", "0000").order("date_int", desc=True).limit(2000).execute()
        if res.data:
            print(f"Downloaded {len(res.data)} records from Supabase")
            data_to_insert = []
            for row in res.data:
                # Filter fields that match local schema
                # Local schema: date_int, open, high, low, close, volume, amount, foreign_buy, trust_buy, dealer_buy, tdcc_count, large_shareholder_pct, code
                # Supabase has these.
                data_to_insert.append((
                    row['code'], row['date_int'], row['open'], row['high'], row['low'], row['close'], 
                    row['volume'], row['amount'], row.get('foreign_buy'), row.get('trust_buy'), row.get('dealer_buy'),
                    row.get('tdcc_count'), row.get('large_shareholder_pct')
                ))
            
            cur.executemany("""
                INSERT OR REPLACE INTO stock_history 
                (code, date_int, open, high, low, close, volume, amount, foreign_buy, trust_buy, dealer_buy, tdcc_count, large_shareholder_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data_to_insert)
            print("Inserted stock_history records")
        else:
            print("No history found in Supabase")
    except Exception as e:
        print(f"Error restoring history: {e}")

    conn.commit()
    conn.close()
    print("Restore complete.")

if __name__ == "__main__":
    main()
