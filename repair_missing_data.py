import sqlite3
import pandas as pd
import requests
import time
import random
from datetime import datetime
import urllib3
import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1_2,
            ssl_context=ctx
        )

def get_session():
    session = requests.Session()
    session.mount('https://', SSLAdapter())
    session.verify = False
    return session

DB_PATH = "taiwan_stock.db"

def get_missing_targets():
    """Get list of (code, date_int) with missing close"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT code, date_int FROM stock_history WHERE close IS NULL", conn)
    conn.close()
    return df

def repair_data():
    targets = get_missing_targets()
    if targets.empty:
        print("No missing data found.")
        return

    print(f"Found {len(targets)} missing records. Grouping by date...")
    
    # Group by date to minimize requests (download whole day)
    dates = targets['date_int'].unique()
    print(f"Dates to repair: {sorted(dates)}")
    
    # We can reuse existing download logic or write a simple fetcher
    # Since it's daily data, we should probably re-download the daily stats for those specific days
    # But wait, daily stats (MI_INDEX) gives ALL stocks.
    # So for each missing date, we just need to fetch that day's data and update the DB.
    
    for date_int in sorted(dates):
        print(f"\nReparing date: {date_int}...")
        try:
            # Convert to YYYYMMDD string
            date_str = str(date_int)
            
            # 1. TWSE (Listing)
            print(f"  Fetching TWSE data for {date_str}...")
            twse_data = fetch_twse_daily(date_str)
            if twse_data:
                update_db(date_int, twse_data, 'TWSE')
            
            time.sleep(3)
            
            # 2. TPEx (OTC)
            print(f"  Fetching TPEx data for {date_str}...")
            tpex_data = fetch_tpex_daily(date_str)
            if tpex_data:
                update_db(date_int, tpex_data, 'TPEx')
                
            time.sleep(5)
            
        except Exception as e:
            print(f"Error repairing {date_int}: {e}")

def fetch_twse_daily(date_str):
    # https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=20251219&type=ALLBUT0999
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999"
    try:
        session = get_session()
        r = session.get(url, timeout=10)
        data = r.json()
        if data.get('stat') != 'OK':
            print(f"    TWSE Stat: {data.get('stat')}")
            return None
        
        # Parse data9 (main board)
        # Columns: "證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", ...
        # Index: 0, 1, 2, 3, 4, 5, 6, 7, 8
        records = {}
        for row in data.get('data9', []):
            code = row[0]
            try:
                # Clean numeric strings (remove commas, handle '--')
                def clean_num(s):
                    s = s.replace(',', '')
                    return None if '--' in s or s == '' else float(s)
                
                open_p = clean_num(row[5])
                high_p = clean_num(row[6])
                low_p = clean_num(row[7])
                close_p = clean_num(row[8])
                vol = clean_num(row[2]) # Shares
                
                if close_p is not None:
                    records[code] = (open_p, high_p, low_p, close_p, vol)
            except:
                continue
        return records
    except Exception as e:
        print(f"    TWSE Fetch Error: {e}")
        return None

def fetch_tpex_daily(date_str):
    # https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=114/12/19&o=json
    # Date format: Minguo (YYY/MM/DD)
    y = int(date_str[:4]) - 1911
    m = date_str[4:6]
    d = date_str[6:]
    minguo_date = f"{y}/{m}/{d}"
    
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={minguo_date}&o=json"
    try:
        session = get_session()
        r = session.get(url, timeout=10)
        data = r.json()
        
        # aaData: ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數", ...]
        # Index: 0, 1, 2, 3, 4, 5, 6, 7
        records = {}
        for row in data.get('aaData', []):
            code = row[0]
            try:
                def clean_num(s):
                    s = s.replace(',', '')
                    return None if '--' in s or s == '' else float(s)
                
                close_p = clean_num(row[2])
                open_p = clean_num(row[4])
                high_p = clean_num(row[5])
                low_p = clean_num(row[6])
                vol = clean_num(row[7])
                
                if close_p is not None:
                    records[code] = (open_p, high_p, low_p, close_p, vol)
            except:
                continue
        return records
    except Exception as e:
        print(f"    TPEx Fetch Error: {e}")
        return None

def update_db(date_int, data_map, source):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get list of missing codes for this date
    cursor.execute("SELECT code FROM stock_history WHERE date_int = ? AND close IS NULL", (date_int,))
    missing_codes = [r[0] for r in cursor.fetchall()]
    
    updated_count = 0
    for code in missing_codes:
        if code in data_map:
            open_p, high_p, low_p, close_p, vol = data_map[code]
            cursor.execute("""
                UPDATE stock_history 
                SET open=?, high=?, low=?, close=?, volume=?
                WHERE code=? AND date_int=?
            """, (open_p, high_p, low_p, close_p, vol, code, date_int))
            updated_count += 1
            
    conn.commit()
    conn.close()
    print(f"    Updated {updated_count} records for {source} on {date_int}")

if __name__ == "__main__":
    repair_data()
