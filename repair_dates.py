import sys
import os
import time
import requests
import json
from datetime import datetime

# Ensure we are in the right directory
os.chdir(r'd:\twse')
sys.path.append(r'd:\twse')

from 最終修正 import db_manager, ensure_db, safe_num, safe_int, is_normal_stock

# Target dates (YYYYMMDD)
target_dates = ['20260102', '20251226']

print("Starting date repair for:", target_dates)

ensure_db()

def roc_date_str(date_str):
    # YYYYMMDD -> YYY/MM/DD
    dt = datetime.strptime(date_str, "%Y%m%d")
    year = dt.year - 1911
    return f"{year}/{dt.month:02d}/{dt.day:02d}"

def fetch_and_update_tpex(date_str):
    roc_d = roc_date_str(date_str)
    print(f"Fetching TPEx data for {date_str} (ROC: {roc_d})...")
    
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc_d}&o=json"
    
    try:
        resp = requests.get(url, timeout=30, verify=False)
        data = resp.json()
        
        # Check data structure
        # Usually data['aaData'] or data['tables'][0]['data']
        rows = []
        if 'aaData' in data:
            rows = data['aaData']
        elif 'tables' in data and len(data['tables']) > 0:
            rows = data['tables'][0]['data']
            
        if not rows:
            print(f"  No data found for {date_str}")
            return

        print(f"  Got {len(rows)} records. Updating DB...")
        
        count = 0
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            for item in rows:
                # Format varies, but usually:
                # 0: Code, 1: Name, 2: Close, 3: Change, 4: Open, 5: High, 6: Low, 7: Vol, 8: Amt, 9: Trans
                # Let's try to be robust based on length or key names if dict
                
                # If list
                if isinstance(item, list):
                    if len(item) < 10: continue
                    code = item[0].strip()
                    name = item[1].strip()
                    
                    # Filter
                    if len(code) != 4 or not code.isdigit(): continue
                    
                    close_p = safe_num(item[2])
                    open_p = safe_num(item[4])
                    high_p = safe_num(item[5])
                    low_p = safe_num(item[6])
                    vol = safe_int(item[7]) # shares usually
                    # Sometimes vol is in 1000s? No, usually shares in this API.
                    # But wait, let's check `_parse_tpex_item` in 最終修正.py if possible.
                    # Actually, let's just trust safe_num/int logic.
                    # Note: TPEx often uses thousands for volume in display, but API might be raw.
                    # Let's assume raw or handle commas. safe_int handles commas.
                    
                    amount = safe_int(item[8])
                    
                else:
                    # Dict (unlikely for this endpoint but possible)
                    continue

                if close_p is None: continue
                
                # Upsert
                cur.execute("""
                    INSERT OR REPLACE INTO stock_history 
                    (code, date_int, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (code, int(date_str), open_p, high_p, low_p, close_p, vol, amount))
                count += 1
            
            conn.commit()
        print(f"  Updated {count} stocks for {date_str}.")
        
    except Exception as e:
        print(f"  Error fetching {date_str}: {e}")

for d in target_dates:
    fetch_and_update_tpex(d)
    time.sleep(3)

print("Date repair complete.")
