import sqlite3
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = r"d:\twse\taiwan_stock.db"

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def update_estimated_holdings():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("Calculating estimated holdings for Trust and Dealer...")
    
    # 1. Get all stocks
    cursor.execute("SELECT code FROM stock_meta")
    stocks = [r[0] for r in cursor.fetchall()]
    
    # 2. Get latest date in institutional_investors
    cursor.execute("SELECT MAX(date_int) FROM institutional_investors")
    latest_date = cursor.fetchone()[0]
    
    if not latest_date:
        print("No institutional data found.")
        return

    print(f"Target date: {latest_date}")
    
    updated_count = 0
    
    for code in stocks:
        # Calculate cumulative net buy from stock_history with Min-Shifted Algorithm
        # 1. Get all daily buy/sell records ordered by date
        cursor.execute("""
            SELECT trust_buy, dealer_buy
            FROM stock_history
            WHERE code = ? AND date_int <= ?
            ORDER BY date_int ASC
        """, (code, latest_date))
        
        rows = cursor.fetchall()
        if not rows:
            continue

        # 2. Calculate running sum and find minimum
        trust_running = 0
        trust_min = 0
        dealer_running = 0
        dealer_min = 0
        
        for r in rows:
            t_buy = r[0] if r[0] is not None else 0
            d_buy = r[1] if r[1] is not None else 0
            
            trust_running += t_buy
            dealer_running += d_buy
            
            if trust_running < trust_min:
                trust_min = trust_running
            if dealer_running < dealer_min:
                dealer_min = dealer_running
        
        # 3. Adjust final holding: Final = Running Total + abs(Min)
        # This assumes that at the point of lowest cumulative sum, the holding was 0.
        trust_holding = trust_running - trust_min
        dealer_holding = dealer_running - dealer_min
        
        # Get total shares for % calculation
        cursor.execute("SELECT total_shares FROM stock_meta WHERE code = ?", (code,))
        meta_res = cursor.fetchone()
        total_shares = meta_res[0] if meta_res and meta_res[0] else 0
        
        trust_pct = round(trust_holding / total_shares * 100, 2) if total_shares > 0 else 0.0
        dealer_pct = round(dealer_holding / total_shares * 100, 2) if total_shares > 0 else 0.0
        
        # Update institutional_investors
        # We only update the record for the latest_date
        cursor.execute("""
            UPDATE institutional_investors
            SET trust_holding_shares = ?, trust_holding_pct = ?,
                dealer_holding_shares = ?, dealer_holding_pct = ?
            WHERE code = ? AND date_int = ?
        """, (trust_holding, trust_pct, dealer_holding, dealer_pct, code, latest_date))
        
        if cursor.rowcount > 0:
            updated_count += 1
            
    conn.commit()
    conn.close()
    print(f"Updated estimated holdings for {updated_count} stocks using Min-Shifted Algorithm.")

if __name__ == "__main__":
    update_estimated_holdings()
