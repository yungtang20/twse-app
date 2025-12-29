import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = r"d:\twse\taiwan_stock.db"

def get_trading_days(conn):
    """Get all trading days from market index or most common dates"""
    # Assuming stock '2330' (TSMC) has a complete record of trading days
    # Or better, get all distinct dates from stock_history
    print("Determining valid trading days...")
    cursor = conn.execute("SELECT DISTINCT date_int FROM stock_history ORDER BY date_int")
    dates = [r[0] for r in cursor.fetchall()]
    return dates

def check_comprehensive():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Check for NULLs
    print("\n[1] Checking for NULL values...")
    null_df = pd.read_sql_query("""
        SELECT code, date_int, close 
        FROM stock_history 
        WHERE close IS NULL OR open IS NULL OR high IS NULL OR low IS NULL
        ORDER BY date_int, code
    """, conn)
    
    if not null_df.empty:
        print(f"Found {len(null_df)} records with NULL values.")
        null_df.to_csv("missing_nulls.csv", index=False)
    else:
        print("No NULL values found.")

    # 2. Check for Missing Dates (Gaps)
    print("\n[2] Checking for Missing Dates (Gaps)...")
    trading_days = get_trading_days(conn)
    all_dates_set = set(trading_days)
    
    # Get all stocks and their dates
    cursor = conn.execute("SELECT code, date_int FROM stock_history")
    stock_dates = {}
    for code, date_int in cursor.fetchall():
        if code not in stock_dates:
            stock_dates[code] = set()
        stock_dates[code].add(date_int)
        
    missing_gaps = []
    
    # Check each stock
    # We only check gaps within the stock's listing period (min date to max date)
    # or just check against the last N days for active stocks
    
    recent_days = sorted(list(all_dates_set))[-30:] # Check last 30 trading days
    print(f"Checking gaps for the last 30 trading days ({recent_days[0]} - {recent_days[-1]})...")
    
    for code, dates in stock_dates.items():
        # Filter trading days to check: only those >= stock's min date
        min_date = min(dates)
        
        # Check against recent days
        for day in recent_days:
            if day >= min_date and day not in dates:
                # Potential gap
                missing_gaps.append({'code': code, 'date_int': day, 'type': 'Missing Row'})
                
    gap_df = pd.DataFrame(missing_gaps)
    if not gap_df.empty:
        print(f"Found {len(gap_df)} missing rows in the last 30 days.")
        gap_df.to_csv("missing_gaps.csv", index=False)
        
        # Summary
        print("\nTop Missing Stocks (Last 30 Days):")
        print(gap_df['code'].value_counts().head(10))
        
        print("\nTop Missing Dates (Last 30 Days):")
        print(gap_df['date_int'].value_counts().head(10))
    else:
        print("No missing rows found in the last 30 days.")

    conn.close()
    
    # Combine results for user
    print("\n" + "="*50)
    print("FINAL REPORT")
    print("="*50)
    
    if not null_df.empty:
        print(f"NULL Records: {len(null_df)}")
        print("Example (Code, Date):")
        for _, row in null_df.head(5).iterrows():
            print(f"  {row['code']} - {row['date_int']}")
            
    if not gap_df.empty:
        print(f"Missing Rows (Last 30 Days): {len(gap_df)}")
        print("Example (Code, Date):")
        for _, row in gap_df.head(5).iterrows():
            print(f"  {row['code']} - {row['date_int']}")
            
    if null_df.empty and gap_df.empty:
        print("ALL CLEAR! No missing data found.")

if __name__ == "__main__":
    check_comprehensive()
