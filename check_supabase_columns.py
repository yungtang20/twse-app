
import os
import sys
from backend.services.db import db_manager

def check_supabase_columns():
    if not db_manager.supabase:
        print("Supabase not connected")
        return

    print("Fetching one row from institutional_investors in Supabase...")
    try:
        res = db_manager.supabase.table("institutional_investors").select("*").limit(1).execute()
        if res.data:
            print("Columns found:", list(res.data[0].keys()))
        else:
            print("Table is empty. Cannot determine columns from data.")
            # Try to insert a dummy record to see what fails, or just rely on error?
            # Actually, if empty, we might need to check migration files or assume standard schema.
            # Let's try to see if we can get schema info another way? 
            # Supabase-py doesn't have a direct 'get schema' method easily accessible without admin rights usually.
            pass
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_supabase_columns()
