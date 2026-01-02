
import os
import sys
from pathlib import Path
from supabase import create_client

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.services.db import db_manager

def check_0000_data():
    if not db_manager.supabase:
        print("Supabase not connected")
        return

    print("Checking stock_history for 0000...")
    res_hist = db_manager.supabase.table("stock_history") \
        .select("date_int, close, foreign_buy, trust_buy, dealer_buy") \
        .eq("code", "0000") \
        .order("date_int", desc=True) \
        .limit(5) \
        .execute()
    
    if res_hist.data:
        print("stock_history data (first 5):")
        for row in res_hist.data:
            print(row)
    else:
        print("No data in stock_history for 0000")

    print("\nChecking institutional_investors for 0000...")
    res_inst = db_manager.supabase.table("institutional_investors") \
        .select("*") \
        .eq("code", "0000") \
        .order("date_int", desc=True) \
        .limit(5) \
        .execute()

    if res_inst.data:
        print("institutional_investors data (first 5):")
        for row in res_inst.data:
            print(row)
    else:
        print("No data in institutional_investors for 0000")

if __name__ == "__main__":
    check_0000_data()
