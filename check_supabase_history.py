import os
from supabase import create_client

# Supabase Config
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_secret_XSeaHx_76CRxA6j8nZ3qDg_nzgFgTAN"

def check_history():
    print(f"Connecting to Supabase: {SUPABASE_URL} ...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        print("Checking 'stock_history' table...")
        response = supabase.table("stock_history") \
            .select("*") \
            .limit(5) \
            .execute()
            
        data = response.data
        if not data:
            print("Table 'stock_history' is empty or does not exist (or RLS policy blocks it).")
        else:
            print(f"Found {len(data)} records in 'stock_history'. Sample:")
            print(data[0])
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_history()
