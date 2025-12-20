import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

# Import the function
from 最終修正 import step8_sync_supabase, ensure_db, Config, DB_FILE

# Ensure DB exists (just in case)
if not os.path.exists(DB_FILE):
    print(f"Error: Database not found at {DB_FILE}")
    sys.exit(1)

print("Starting Supabase Sync...")
try:
    step8_sync_supabase()
except Exception as e:
    print(f"Error running sync: {e}")
