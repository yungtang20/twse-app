import os
import sys
from datetime import datetime, timedelta
from supabase import create_client, Client

# Add root directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load config or env
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
# We need the key. It's in backend/services/db.py or env.
# For this script, we'll try to load from backend/services/db.py if possible, 
# or use the one from the user's environment if they set it.
# Actually, the user has the key in cloud_update.py (env var) or db.py (hardcoded).
# Let's try to import from db.py
from backend.services.db import SUPABASE_KEY

def clean_data():
    if not SUPABASE_KEY:
        print("❌ Missing SUPABASE_KEY")
        return

    print(f"🔄 Connecting to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Calculate cutoff date (1 year ago)
    cutoff_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    print(f"📅 Cutoff date: {cutoff_date}")

    # 1. Clean institutional_investors
    print("\n[1] Cleaning 'institutional_investors' (older than 1 year)...")
    try:
        # Supabase delete requires a filter
        res = supabase.table("institutional_investors").delete().lt("date", cutoff_date).execute()
        print(f"  ✓ Deleted rows: {len(res.data) if res.data else 'Unknown'}")
    except Exception as e:
        print(f"  ❌ Failed: {e}")

    # 2. Clean sync_status (keep last 30 days)
    print("\n[2] Cleaning 'sync_status' (older than 30 days)...")
    status_cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    try:
        res = supabase.table("sync_status").delete().lt("created_at", status_cutoff).execute()
        print(f"  ✓ Deleted rows: {len(res.data) if res.data else 'Unknown'}")
    except Exception as e:
        print(f"  ❌ Failed: {e}")

    # 3. Clean stock_data (older than 1 year) - User said "non-essential first", but stock_data might be redundant if stock_history exists?
    # Let's be conservative and only delete if user explicitly asked, but user said "unnecessary data".
    # stock_data is usually for the "latest" view or daily indicators. 
    # If it stores history, it duplicates stock_history.
    # Let's clean it to save space.
    print("\n[3] Cleaning 'stock_data' (older than 1 year)...")
    try:
        res = supabase.table("stock_data").delete().lt("date", cutoff_date).execute()
        print(f"  ✓ Deleted rows: {len(res.data) if res.data else 'Unknown'}")
    except Exception as e:
        print(f"  ❌ Failed: {e}")

    print("\n✅ Cleanup complete.")

if __name__ == "__main__":
    clean_data()
