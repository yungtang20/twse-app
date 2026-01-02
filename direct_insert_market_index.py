import sys
import os
import ssl
import requests

# Add current directory to path
sys.path.append(os.getcwd())

# SSL Patch
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass
requests.packages.urllib3.disable_warnings()

from core.fetchers.market_index import MarketIndexFetcher
import sqlite3

db_path = r"d:\twse\taiwan_stock.db"
date_str = "20260102"

print(f"=== Testing MarketIndexFetcher for {date_str} ===")
fetcher = MarketIndexFetcher()
records = fetcher.fetch_all(date_str)
print(f"Fetched {len(records)} records: {records}")

if records:
    print(f"\n=== Inserting into database ===")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Check existing data
    first_date = records[0][0]
    cur.execute("SELECT COUNT(*) FROM market_index WHERE date_int = ?", (first_date,))
    existing = cur.fetchone()[0]
    print(f"Existing records for {first_date}: {existing}")
    
    if existing == 0:
        cur.executemany("""
            INSERT OR REPLACE INTO market_index (
                date_int, index_id, close, open, high, low, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, records)
        conn.commit()
        print(f"✓ Inserted {len(records)} records.")
    else:
        print(f"Data already exists for {first_date}.")
    
    conn.close()
else:
    print("No records to insert.")

print(f"\n=== Verifying database ===")
conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("SELECT * FROM market_index WHERE date_int = ?", (int(date_str),))
rows = cur.fetchall()
for row in rows:
    print(f"  {row}")
conn.close()
