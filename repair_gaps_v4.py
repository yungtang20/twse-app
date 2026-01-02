import sys
import os
import time
from datetime import datetime

# Ensure we are in the right directory
os.chdir(r'd:\twse')
sys.path.append(r'd:\twse')

# Import init_twstock to apply patches
from 最終修正 import db_manager, ensure_db, init_twstock, safe_num, safe_int

# Apply patches
print("Applying twstock patches...")
init_twstock()

import twstock

# Target stocks
targets = ['5310', '6212', '6236', '8291', '8921']

print("Starting targeted repair (V4 - Patched Twstock) for:", targets)

ensure_db()

def update_stock(code):
    print(f"Fetching data for {code}...")
    try:
        stock = twstock.Stock(code)
        # Fetch recent data (last 31 days)
        # The patch should make this work for TPEx
        data = stock.fetch_31()
        
        if not data:
            print(f"  No data found for {code}")
            return

        print(f"  Got {len(data)} records. Updating DB...")
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            for d in data:
                # twstock data: date, capacity, turnover, open, high, low, close, change, transaction
                # date is datetime
                date_int = int(d.date.strftime('%Y%m%d'))
                
                # Convert values
                volume = int(d.capacity) # shares
                amount = int(d.turnover) # value
                open_p = d.open
                high_p = d.high
                low_p = d.low
                close_p = d.close
                
                if close_p is None: continue

                # Upsert
                cur.execute("""
                    INSERT OR REPLACE INTO stock_history 
                    (code, date_int, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (code, date_int, open_p, high_p, low_p, close_p, volume, amount))
            
            conn.commit()
        print(f"  {code} updated successfully.")
        
    except Exception as e:
        print(f"  Error updating {code}: {e}")
        import traceback
        traceback.print_exc()

for code in targets:
    update_stock(code)
    time.sleep(3) # Be nice to API

print("Repair V4 complete.")
