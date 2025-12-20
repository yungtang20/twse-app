import sqlite3
from datetime import datetime, timedelta
import os
import sys

# Add current directory to path
sys.path.append(os.getcwd())

# Mock or Import necessary components
try:
    from 最終修正 import db_manager, get_latest_market_date, MIN_DATA_COUNT
except ImportError:
    # Fallback if import fails (e.g. if run in isolation)
    print("Import failed, using mock values")
    MIN_DATA_COUNT = 250
    class DBManager:
        def get_connection(self):
            return sqlite3.connect('stocks.db')
    db_manager = DBManager()
    def get_latest_market_date():
        return datetime.now().strftime('%Y-%m-%d')

def check_missing():
    print("Checking missing data with optimized logic...")
    
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        
        # 1. Load Listing Dates
        list_date_map = {}
        missing_list_date = []
        try:
            cur.execute("SELECT code, name, list_date FROM stock_meta")
            for r in cur.fetchall():
                if r[2]: 
                    list_date_map[r[0]] = r[2]
                else:
                    missing_list_date.append((r[0], r[1]))
            print(f"Loaded {len(list_date_map)} listing dates.")
            print(f"Missing listing dates for {len(missing_list_date)} stocks.")
            if missing_list_date:
                print(f"Sample missing: {missing_list_date[:10]}")
        except Exception as e:
            print(f"Error loading stock_meta: {e}")
            return

        # 2. Get History Stats
        latest_market_date_str = get_latest_market_date()
        latest_market_date_int = int(latest_market_date_str.replace('-', ''))
        print(f"Latest Market Date: {latest_market_date_int}")

        print("Analyzing database stats...")
        cur.execute("""
            SELECT code, COUNT(*), MIN(date_int), MAX(date_int),
                   SUM(CASE WHEN amount IS NULL OR amount = 0 THEN 1 ELSE 0 END)
            FROM stock_history 
            GROUP BY code
        """)
        rows = cur.fetchall()
        print(f"Analyzed {len(rows)} stocks.")
        
        tasks = []
        for row in rows:
            code, count, min_date_int, max_date_int, missing_amount = row
            
            # Logic from step6
            if max_date_int < latest_market_date_int:
                tasks.append((code, count, f"Outdated (to {max_date_int})"))
                continue
                
            if missing_amount > 0:
                # Check if the missing amount is before listing date
                l_date_str = list_date_map.get(code)
                if l_date_str:
                    try:
                        l_date_int = int(l_date_str.replace('-', ''))
                        # Find the date of missing amount
                        cur.execute(f"SELECT date_int FROM stock_history WHERE code='{code}' AND (amount IS NULL OR amount=0) LIMIT 1")
                        bad_date = cur.fetchone()[0]
                        
                        if bad_date < l_date_int:
                            # This is the case user warned about!
                            # We should ignore this task (or better, delete the bad row)
                            # For now, just report it
                            tasks.append((code, count, f"Missing Amount BEFORE Listing ({bad_date} < {l_date_int})"))
                            continue
                    except:
                        pass
                
                if code == '1240':
                    cur.execute(f"SELECT * FROM stock_history WHERE code='{code}' AND (amount IS NULL OR amount=0)")
                    bad_row = cur.fetchone()
                    print(f"\n[DEBUG] Bad row for 1240: {bad_row}")
                
                tasks.append((code, count, f"Missing Amount ({missing_amount})"))
                continue
            
            if count < MIN_DATA_COUNT:
                is_new_stock = False
                l_date_str = list_date_map.get(code)
                
                if l_date_str:
                    try:
                        l_date = datetime.strptime(l_date_str, '%Y-%m-%d')
                        days_since = (datetime.now() - l_date).days
                        expected_count = int(days_since * 0.68)
                        
                        if count >= expected_count * 0.9:
                            is_new_stock = True
                        
                        if min_date_int:
                            min_date = datetime.strptime(str(min_date_int), '%Y%m%d')
                            if min_date <= l_date + timedelta(days=20):
                                is_new_stock = True
                    except:
                        pass
                
                if not is_new_stock:
                    tasks.append((code, count, f"Insufficient Count ({count})"))
                    continue
        
        print(f"\nFound {len(tasks)} stocks requiring backfill:")
        for t in tasks[:20]:
            print(f" - {t[0]}: {t[2]}")
        if len(tasks) > 20:
            print(f" ... and {len(tasks)-20} more.")

if __name__ == "__main__":
    check_missing()
