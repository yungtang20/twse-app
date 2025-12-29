import sys
import os
sys.path.append(os.getcwd())
from backend.services.db import db_manager

def debug_2560():
    print("Debugging 2560 Strategy...")
    
    # 0. Check Columns
    try:
        print("Checking stock_snapshot columns...")
        # Use execute_query to get one row and check keys
        res = db_manager.execute_query("SELECT * FROM stock_snapshot LIMIT 1")
        if res:
            print(f"Columns: {list(res[0].keys())}")
            if 'open' not in res[0].keys():
                print("CRITICAL: 'open' column missing!")
        else:
            print("stock_snapshot is empty!")
    except Exception as e:
        print(f"Error checking columns: {e}")

    # 1. Check Total Count
    base_where = "WHERE m.code GLOB '[0-9][0-9][0-9][0-9]'"
    try:
        count = db_manager.execute_single(f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where}")
        print(f"Total stocks: {count['count']}")
    except Exception as e:
        print(f"Error step 1: {e}")
    
    # 2. Check Volume > 500
    vol_cond = "AND s.volume >= 500000" # 500 * 1000
    try:
        count = db_manager.execute_single(f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond}")
        print(f"Volume > 500: {count['count']}")
    except Exception as e:
        print(f"Error step 2: {e}")
    
    # 3. Check MA25 Trend
    trend_cond = "AND s.ma25 IS NOT NULL AND s.ma25_slope IS NOT NULL AND s.close > s.ma25 AND s.ma25_slope > 0"
    try:
        count = db_manager.execute_single(f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {trend_cond}")
        print(f"Trend (MA25 Up): {count['count']}")
    except Exception as e:
        print(f"Error step 3: {e}")
    
    # 4. Check Vol Cross
    vol_cross_cond = "AND s.vol_ma5 IS NOT NULL AND s.vol_ma60 IS NOT NULL AND s.vol_ma5 > s.vol_ma60"
    try:
        count = db_manager.execute_single(f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {trend_cond} {vol_cross_cond}")
        print(f"Vol Cross (MA5 > MA60): {count['count']}")
    except Exception as e:
        print(f"Error step 4: {e}")

    # 5. Check Red Candle
    candle_cond = "AND s.close > s.open"
    try:
        count = db_manager.execute_single(f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {trend_cond} {vol_cross_cond} {candle_cond}")
        print(f"Red Candle: {count['count']}")
    except Exception as e:
        print(f"Error step 5: {e}")

    # 6. Check Proximity
    prox_cond = "AND s.close < s.ma25 * 1.1"
    try:
        count = db_manager.execute_single(f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {trend_cond} {vol_cross_cond} {candle_cond} {prox_cond}")
        print(f"Proximity (< 10%): {count['count']}")
    except Exception as e:
        print(f"Error step 6: {e}")

if __name__ == "__main__":
    debug_2560()
