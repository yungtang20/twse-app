import requests
import pandas as pd
import sqlite3
import time

def fetch_finmind_tdcc(stock_id, start_date):
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockHoldingSharesPer",
        "data_id": stock_id,
        "start_date": start_date
    }
    res = requests.get(url, params=parameter)
    data = res.json()
    print("FinMind Response:", data.get("msg"))
    if data.get("msg") == "success":
        return pd.DataFrame(data.get("data"))
    return pd.DataFrame()

def backfill_tdcc(stock_id):
    df = fetch_finmind_tdcc(stock_id, "2024-01-01")
    if not df.empty:
        print(f"Found {len(df)} records for {stock_id}")
        conn = sqlite3.connect("taiwan_stock.db")
        
        # Map FinMind columns to our table
        # FinMind: date, stock_id, HoldingLevels, people, unit, last_unit, percent
        # Our table: code, date_int, level, holders, shares, proportion
        
        for _, row in df.iterrows():
            date_int = int(row['date'].replace('-', ''))
            level = int(row['HoldingLevels'])
            holders = int(row['people'])
            shares = int(row['unit'])
            proportion = float(row['percent'])
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (stock_id, date_int, level, holders, shares, proportion))
        
        conn.commit()
        conn.close()
        print(f"Backfilled {stock_id}")

if __name__ == "__main__":
    backfill_tdcc("2330")
