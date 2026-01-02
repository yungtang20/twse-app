import os
import sys
import time
from pathlib import Path
from supabase import create_client, Client, ClientOptions

# Add root directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load config or env
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
try:
    from backend.services.db import SUPABASE_KEY
except:
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

DB_PATH = Path("taiwan_stock.db")
BUCKET_NAME = "databases"
REMOTE_PATH = "taiwan_stock.db"

def main():
    print("="*50)
    print("🚀 Uploading taiwan_stock.db to Supabase Storage")
    print("="*50)

    if not SUPABASE_KEY:
        print("❌ Missing SUPABASE_KEY")
        return

    if not DB_PATH.exists():
        print(f"❌ Local database '{DB_PATH}' not found.")
        return

    print("🔄 Connecting to Supabase...")
    try:
        # Increase timeout
        options = ClientOptions(postgrest_client_timeout=300, storage_client_timeout=600)
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=options)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    # 1. Ensure bucket exists
    print(f"🔍 Checking bucket '{BUCKET_NAME}'...")
    try:
        buckets = supabase.storage.list_buckets()
        bucket_names = [b.name for b in buckets]
        if BUCKET_NAME not in bucket_names:
            print(f"   ⚠ Bucket '{BUCKET_NAME}' not found. Creating...")
            supabase.storage.create_bucket(BUCKET_NAME, options={"public": False})
            print(f"   ✓ Bucket created.")
        else:
            print(f"   ✓ Bucket exists.")
    except Exception as e:
        print(f"   ⚠ Failed to list/create bucket: {e}")
        # Continue anyway, maybe it exists but we can't list
    
    # 2. Upload
    print(f"📤 Uploading '{DB_PATH}' ({DB_PATH.stat().st_size / 1024 / 1024:.2f} MB)...")
    
    max_retries = 3
    for i in range(max_retries):
        try:
            with open(DB_PATH, 'rb') as f:
                response = supabase.storage.from_(BUCKET_NAME).upload(
                    path=REMOTE_PATH,
                    file=f,
                    file_options={"upsert": "true", "content-type": "application/x-sqlite3"}
                )
            print(f"✅ Upload successful!")
            break
        except Exception as e:
            print(f"❌ Upload failed (Attempt {i+1}/{max_retries}): {e}")
            if i < max_retries - 1:
                print("   ⏳ Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print("❌ Max retries reached.")

if __name__ == "__main__":
    main()
