
import os
import sys
from backend.services.db import db_manager

def check_stock_history_columns():
    if not db_manager.supabase:
        print("Supabase not connected")
        return

    print("Fetching one row from stock_history in Supabase...")
    try:
        res = db_manager.supabase.table("stock_history").select("*").limit(1).execute()
        if res.data:
            print("Columns found:", list(res.data[0].keys()))
        else:
            print("Table is empty.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_stock_history_columns()
