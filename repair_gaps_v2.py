import sys
import os
import time
from datetime import datetime, timedelta

# Ensure we are in the right directory
os.chdir(r'd:\twse')
sys.path.append(r'd:\twse')

from 最終修正 import db_manager, ensure_db
from core.fetchers import TwstockFetcher

# Target stocks
targets = ['5310', '6212', '6236', '8291', '8921']

print("Starting targeted repair (V3 - TwstockFetcher) for:", targets)

ensure_db()
fetcher = TwstockFetcher()

def update_stock(code):
    print(f"Fetching data for {code}...")
    try:
        # Fetch last 30 days
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # TwstockFetcher.fetch_price returns list of dicts or objects
        data = fetcher.fetch_price(code, start_date, end_date)
        
        if not data:
            print(f"  No data found for {code}")
            return

        print(f"  Got {len(data)} records. Updating DB...")
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            for d in data:
                # TwstockFetcher returns dict with: date, open, high, low, close, volume, amount
                # date is string "YYYY-MM-DD"
                date_int = int(d['date'].replace('-', ''))
                
                # Upsert
                cur.execute("""
                    INSERT OR REPLACE INTO stock_history 
                    (code, date_int, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (code, date_int, d['open'], d['high'], d['low'], d['close'], d['volume'], d['amount']))
            
            conn.commit()
        print(f"  {code} updated successfully.")
        
    except Exception as e:
        print(f"  Error updating {code}: {e}")

for code in targets:
    update_stock(code)
    time.sleep(3) # Be nice to API

print("Repair V3 complete.")
