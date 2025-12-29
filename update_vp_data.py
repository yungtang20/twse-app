"""
Update Stock Snapshot with VP (Volume Profile) Data
"""
import sqlite3
import pandas as pd
import numpy as np
from backend.services.db import db_manager

def add_vp_columns():
    """Add VP columns to stock_snapshot if they don't exist"""
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    
    columns = ['vp_poc', 'vp_high', 'vp_low']
    existing_cols = [row[1] for row in cursor.execute("PRAGMA table_info(stock_snapshot)")]
    
    for col in columns:
        if col not in existing_cols:
            print(f"Adding column {col}...")
            cursor.execute(f"ALTER TABLE stock_snapshot ADD COLUMN {col} REAL")
    
    conn.commit()
    conn.close()

def calculate_vp(df, price_col='close', vol_col='volume', bins=50):
    """Calculate Volume Profile POC, High, Low"""
    if df.empty:
        return None, None, None
        
    df = df.dropna(subset=[price_col, vol_col])
    if df.empty:
        return None, None, None
        
    price_min = df[price_col].min()
    price_max = df[price_col].max()
    
    if price_min == price_max:
        return price_min, price_max, price_min
        
    hist, bin_edges = np.histogram(df[price_col], bins=bins, weights=df[vol_col])
    
    # Find POC (Point of Control) - price level with max volume
    max_idx = np.argmax(hist)
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    
    # Find Value Area (70% of volume)
    total_vol = np.sum(hist)
    target_vol = total_vol * 0.7
    
    current_vol = hist[max_idx]
    left = max_idx
    right = max_idx
    
    while current_vol < target_vol:
        left_vol = hist[left-1] if left > 0 else 0
        right_vol = hist[right+1] if right < len(hist)-1 else 0
        
        if left == 0 and right == len(hist)-1:
            break
            
        if left_vol > right_vol:
            left -= 1
            current_vol += left_vol
        elif right < len(hist)-1:
            right += 1
            current_vol += right_vol
        elif left > 0:
            left -= 1
            current_vol += left_vol
            
    va_low = bin_edges[left]
    va_high = bin_edges[right+1]
    
    return poc_price, va_high, va_low

def update_vp_data():
    """Calculate and update VP data for all stocks"""
    print("Fetching stocks...")
    stocks = db_manager.execute_query("SELECT code FROM stock_snapshot WHERE code GLOB '[0-9][0-9][0-9][0-9]'")
    
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    
    count = 0
    total = len(stocks)
    
    print(f"Updating VP data for {total} stocks...")
    
    for stock in stocks:
        code = stock['code']
        
        # Get history (last 60 days)
        history = db_manager.execute_query(
            "SELECT close, volume FROM stock_history WHERE code = ? ORDER BY date_int DESC LIMIT 60",
            (code,)
        )
        
        if not history or len(history) < 20:
            continue
            
        df = pd.DataFrame(history)
        poc, va_high, va_low = calculate_vp(df)
        
        if poc is not None:
            cursor.execute(
                "UPDATE stock_snapshot SET vp_poc = ?, vp_high = ?, vp_low = ? WHERE code = ?",
                (poc, va_high, va_low, code)
            )
            count += 1
            
        if count % 100 == 0:
            print(f"Processed {count}/{total}...")
            conn.commit()
            
    conn.commit()
    conn.close()
    print(f"Completed! Updated {count} stocks.")

if __name__ == "__main__":
    add_vp_columns()
    update_vp_data()
