import sqlite3
import requests
import time
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB_PATH = 'taiwan_stock.db'
TARGET_DATE_INT = 20251219
TARGET_DATE_STR = "20251219"

def get_connection():
    return sqlite3.connect(DB_PATH)

def safe_num(v):
    if isinstance(v, (int, float)): return v
    if not v or v == '--': return 0.0
    try:
        return float(str(v).replace(',', ''))
    except:
        return 0.0

def safe_int(v):
    if isinstance(v, (int, float)): return int(v)
    if not v or v == '--': return 0
    try:
        return int(str(v).replace(',', ''))
    except:
        return 0

def fix_20251219():
    print(f"Fixing data for {TARGET_DATE_INT}...")
    
    # 1. Delete existing data
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM stock_history WHERE date_int = ?", (TARGET_DATE_INT,))
        deleted = cur.rowcount
        conn.commit()
        print(f"Deleted {deleted} records for {TARGET_DATE_INT}.")

    # 2. Download TWSE
    print("Downloading TWSE data...")
    url_twse = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={TARGET_DATE_STR}&type=ALLBUT0999"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    twse_count = 0
    try:
        resp = requests.get(url_twse, headers=headers, timeout=30, verify=False)
        data = resp.json()
        if data.get('stat') == 'OK':
            records = []
            for table in data.get('tables', []):
                if '每日收盤行情' in table.get('title', ''):
                    for row in table.get('data', []):
                        if len(row) >= 9:
                            code = str(row[0]).strip()
                            if len(code) == 4 and code.isdigit():
                                records.append((
                                    code, TARGET_DATE_INT,
                                    safe_num(row[5]), # Open
                                    safe_num(row[6]), # High
                                    safe_num(row[7]), # Low
                                    safe_num(row[8]), # Close
                                    safe_int(row[2]), # Volume
                                    safe_int(row[4])  # Amount
                                ))
            
            if records:
                with get_connection() as conn:
                    conn.executemany("""
                        INSERT OR REPLACE INTO stock_history 
                        (code, date_int, open, high, low, close, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, records)
                    conn.commit()
                twse_count = len(records)
                print(f"Saved {twse_count} TWSE records.")
        else:
            print(f"TWSE API returned: {data.get('stat')}")
    except Exception as e:
        print(f"TWSE Download failed: {e}")

    time.sleep(1)

    # 3. Download TPEx
    print("Downloading TPEx data...")
    # Convert to ROC date
    d_obj = datetime.strptime(TARGET_DATE_STR, '%Y%m%d')
    roc_date = f"{d_obj.year - 1911}/{d_obj.month:02d}/{d_obj.day:02d}"
    url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc_date}&o=json"
    
    tpex_count = 0
    try:
        resp = requests.get(url_tpex, headers=headers, timeout=30, verify=False)
        data = resp.json()
        
        records = []
        if data.get('aaData'):
            for row in data['aaData']:
                if len(row) >= 6:
                    code = str(row[0]).strip()
                    if len(code) == 4 and code.isdigit():
                        records.append((
                            code, TARGET_DATE_INT,
                            safe_num(row[4]), # Open
                            safe_num(row[5]), # High
                            safe_num(row[6]), # Low
                            safe_num(row[2]), # Close
                            safe_int(row[8]) if len(row) > 8 else 0, # Volume (shares)
                            safe_int(row[9]) if len(row) > 9 else 0  # Amount
                        ))
        
        if records:
            with get_connection() as conn:
                conn.executemany("""
                    INSERT OR REPLACE INTO stock_history 
                    (code, date_int, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, records)
                conn.commit()
            tpex_count = len(records)
            print(f"Saved {tpex_count} TPEx records.")
            
    except Exception as e:
        print(f"TPEx Download failed: {e}")

    print(f"Total records updated: {twse_count + tpex_count}")

if __name__ == "__main__":
    fix_20251219()
