import sqlite3
import pandas as pd

DB_PATH = "taiwan_stock.db"

try:
    conn = sqlite3.connect(DB_PATH)
    
    # Check stock_history
    print("--- stock_history (0000) ---")
    df_hist = pd.read_sql("SELECT * FROM stock_history WHERE code='0000' ORDER BY date_int DESC LIMIT 5", conn)
    if not df_hist.empty:
        print(df_hist)
    else:
        print("No data in stock_history for 0000")

    # Check market_index
    print("\n--- market_index (TAIEX) ---")
    df_index = pd.read_sql("SELECT * FROM market_index WHERE index_id='TAIEX' ORDER BY date_int DESC LIMIT 5", conn)
    if not df_index.empty:
        print(df_index)
    else:
        print("No data in market_index for TAIEX")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
