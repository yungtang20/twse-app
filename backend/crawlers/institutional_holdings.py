import requests
import sqlite3
import datetime
import time
import sys
import os
import urllib3

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add parent directory to path to import database config if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = r"d:\twse\taiwan_stock.db"

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def fetch_foreign_holdings(date_str=None):
    """
    Fetch Foreign Investors Shareholding (MI_QFIIS) from TWSE.
    date_str: YYYYMMDD
    """
    if not date_str:
        # Default to yesterday if not provided (data usually available after 15:00)
        date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    url = f"https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?date={date_str}&selectType=ALLBUT0999&response=json"
    print(f"Fetching MI_QFIIS for {date_str} from {url}...")

    try:
        # verify=False to bypass potential SSL issues in this environment
        res = requests.get(url, verify=False, timeout=10)
        data = res.json()
        
        if data.get('stat') != 'OK' or not data.get('data'):
            print(f"Error or no data: {data.get('stat')}")
            print(f"Raw response: {str(data)[:500]}") # Print first 500 chars
            return

        print(f"Fetched {len(data.get('data', []))} records.")
        if data.get('data'):
            for row in data['data']:
                if row[0] == '7769':
                    with open('d:/twse/debug_7769.txt', 'w', encoding='utf-8') as f:
                        f.write(str(row))
                    break
    except Exception as e:
        print(f"Failed to fetch: {e}")
        return

    
    # Fields parsing removed as we use heuristic now

    update_database(date_str, data['data'])

def parse_int(val):
    if val is None: return 0
    if isinstance(val, (int, float)): return int(val)
    try:
        return int(str(val).replace(',', ''))
    except:
        return 0

def parse_float(val):
    if val is None: return 0.0
    if isinstance(val, (int, float)): return float(val)
    try:
        return float(str(val).replace(',', ''))
    except:
        return 0.0

def update_database(date_str, records):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    date_int = int(date_str)
    
    updated_count = 0
    
    for row in records:
        try:
            code = row[0]
            
            # Heuristic to determine columns
            # Standard: 4=Holding, 5=Pct
            # Alternative (7769, 2330?): 4=Remaining, 5=Holding, 6=Remaining%, 7=Pct
            
            val_4 = parse_float(row[4])
            val_5 = parse_float(row[5])
            
            if val_5 > 100:
                # Case B: Index 5 is likely Shares (Holding)
                # Then Index 7 should be Pct
                holding_shares = int(val_5)
                holding_pct = parse_float(row[7])
            else:
                # Case A: Index 5 is likely Pct
                # Then Index 4 is Holding
                holding_shares = int(val_4)
                holding_pct = val_5
            
            # Check if record exists
            cursor.execute(
                "SELECT 1 FROM institutional_investors WHERE code=? AND date_int=?", 
                (code, date_int)
            )
            exists = cursor.fetchone()
            
            if exists:
                cursor.execute("""
                    UPDATE institutional_investors 
                    SET foreign_holding_shares = ?, foreign_holding_pct = ?
                    WHERE code = ? AND date_int = ?
                """, (holding_shares, holding_pct, code, date_int))
            else:
                cursor.execute("""
                    INSERT INTO institutional_investors 
                    (code, date_int, foreign_holding_shares, foreign_holding_pct, 
                     foreign_buy, foreign_sell, foreign_net,
                     trust_buy, trust_sell, trust_net,
                     dealer_buy, dealer_sell, dealer_net)
                    VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                """, (code, date_int, holding_shares, holding_pct))
                
            updated_count += 1
            
        except Exception as e:
            print(f"Error processing row {row}: {e}")
            continue

    conn.commit()
    conn.close()
    print(f"Updated {updated_count} records in database.")

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    fetch_foreign_holdings(target_date)
