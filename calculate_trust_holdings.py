"""
Calculate and update trust holdings (投信持股) from cumulative data.
Uses trust_cumulative from stock_snapshot and ensures no negative values.
Updates to the same date as foreign holdings (20251226).
"""
import sqlite3

DB_PATH = r"d:\twse\taiwan_stock.db"

def calculate_trust_holdings():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Use the foreign holding date (20251226) for consistency
    target_date = 20251226
    print(f"Target date for trust holdings: {target_date}")
    
    # Get trust_cumulative from stock_snapshot and total_shares from stock_meta
    cursor.execute("""
        SELECT 
            s.code, 
            s.name,
            COALESCE(s.trust_cumulative, 0) as trust_cumulative,
            COALESCE(m.total_shares, 0) as total_shares
        FROM stock_snapshot s
        JOIN stock_meta m ON s.code = m.code
        WHERE m.market_type IN ('TWSE', 'TPEx')
    """)
    
    stocks = cursor.fetchall()
    print(f"Found {len(stocks)} stocks to process")
    
    updated_count = 0
    
    for code, name, trust_cumulative, total_shares in stocks:
        # Ensure no negative values - floor at 0
        trust_holding_shares = max(0, trust_cumulative)
        
        # Calculate percentage (avoid division by zero)
        if total_shares > 0:
            trust_holding_pct = round((trust_holding_shares / total_shares) * 100, 2)
        else:
            trust_holding_pct = 0.0
        
        # Update institutional_investors table for target_date
        cursor.execute("""
            SELECT 1 FROM institutional_investors 
            WHERE code = ? AND date_int = ?
        """, (code, target_date))
        
        if cursor.fetchone():
            # Update existing record
            cursor.execute("""
                UPDATE institutional_investors 
                SET trust_holding_shares = ?, trust_holding_pct = ?
                WHERE code = ? AND date_int = ?
            """, (trust_holding_shares, trust_holding_pct, code, target_date))
            updated_count += 1
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO institutional_investors 
                (code, date_int, trust_holding_shares, trust_holding_pct,
                 foreign_buy, foreign_sell, foreign_net,
                 trust_buy, trust_sell, trust_net,
                 dealer_buy, dealer_sell, dealer_net)
                VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            """, (code, target_date, trust_holding_shares, trust_holding_pct))
            updated_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\nUpdated {updated_count} records with trust holding data for date {target_date}")
    print("Done!")

if __name__ == "__main__":
    calculate_trust_holdings()
