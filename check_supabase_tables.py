import os
from supabase import create_client

# Supabase Config
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_secret_XSeaHx_76CRxA6j8nZ3qDg_nzgFgTAN"

def check_tables():
    print(f"Connecting to Supabase: {SUPABASE_URL} ...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        tables = ["stock_list", "stock_snapshot", "daily_quotes", "stocks", "institutional_investors"]
        
        for t in tables:
            print(f"Checking table '{t}'...")
            try:
                res = supabase.table(t).select("*").limit(1).execute()
                if res.data:
                    print(f"  ✓ Found records. Sample keys: {list(res.data[0].keys())}")
                else:
                    print(f"  ✓ Table '{t}' exists but is empty.")
            except Exception as e:
                print(f"  ✗ Error checking '{t}': {e}")
                
    except Exception as e:
        print(f"Error connecting: {e}")

if __name__ == "__main__":
    check_tables()
