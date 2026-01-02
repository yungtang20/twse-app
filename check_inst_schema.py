
import os
import sys
from backend.services.db import db_manager

def check_schema():
    if db_manager.is_cloud_mode:
        print("Cloud mode detected. Checking Supabase schema...")
        # In cloud mode, we might not be able to easily check schema via SQL, 
        # but we can fetch one row and see keys.
        res = db_manager.supabase.table("institutional_investors").select("*").limit(1).execute()
        if res.data:
            print("Columns:", res.data[0].keys())
        else:
            print("Table is empty, cannot infer schema from data.")
    else:
        print("Local mode detected. Checking SQLite schema...")
        schema = db_manager.execute_query("PRAGMA table_info(institutional_investors)")
        for col in schema:
            print(col)

if __name__ == "__main__":
    check_schema()
