import sqlite3
import sys
import os

DB_PATH = r"d:\twse\taiwan_stock.db"

def update_estimated_holdings(target_date):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"Calculating estimated holdings for {target_date}...")
    
    cursor.execute("SELECT code FROM stock_meta")
    stocks = [r[0] for r in cursor.fetchall()]
    
    updated_count = 0
    
    for code in stocks:
        cursor.execute("""
            SELECT trust_buy, dealer_buy
            FROM stock_history
            WHERE code = ? AND date_int <= ?
            ORDER BY date_int ASC
        """, (code, target_date))
        
        rows = cursor.fetchall()
        if not rows:
            continue

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
        
        trust_holding = trust_running - trust_min
        dealer_holding = dealer_running - dealer_min
        
        cursor.execute("SELECT total_shares FROM stock_meta WHERE code = ?", (code,))
        meta_res = cursor.fetchone()
        total_shares = meta_res[0] if meta_res and meta_res[0] else 0
        
        trust_pct = round(trust_holding / total_shares * 100, 2) if total_shares > 0 else 0.0
        dealer_pct = round(dealer_holding / total_shares * 100, 2) if total_shares > 0 else 0.0
        
        cursor.execute("""
            UPDATE institutional_investors
            SET trust_holding_shares = ?, trust_holding_pct = ?,
                dealer_holding_shares = ?, dealer_holding_pct = ?
            WHERE code = ? AND date_int = ?
        """, (trust_holding, trust_pct, dealer_holding, dealer_pct, code, target_date))
        
        if cursor.rowcount > 0:
            updated_count += 1
            
    conn.commit()
    conn.close()
    print(f"Updated {updated_count} stocks for {target_date}.")

if __name__ == "__main__":
    update_estimated_holdings(20251226)
