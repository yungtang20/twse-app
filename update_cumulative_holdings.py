"""
Calculate cumulative institutional buy/sell from stock_history
and update stock_snapshot with the totals.
"""
from backend.services.db import db_manager

def update_cumulative():
    print("Calculating cumulative institutional holdings...")
    
    # Get sum of all historical buy/sell for each stock
    sql = """
        SELECT 
            code,
            COALESCE(SUM(foreign_buy), 0) as f_total,
            COALESCE(SUM(trust_buy), 0) as t_total,
            COALESCE(SUM(dealer_buy), 0) as d_total
        FROM stock_history
        GROUP BY code
    """
    
    results = db_manager.execute_query(sql)
    print(f"Processing {len(results)} stocks...")
    
    count = 0
    for row in results:
        code = row['code']
        f_total = int(row['f_total'] or 0)
        t_total = int(row['t_total'] or 0)
        d_total = int(row['d_total'] or 0)
        
        update_sql = """
            UPDATE stock_snapshot 
            SET foreign_cumulative = ?, trust_cumulative = ?, dealer_cumulative = ?
            WHERE code = ?
        """
        db_manager.execute_update(update_sql, (f_total, t_total, d_total, code))
        
        count += 1
        if count % 200 == 0:
            print(f"Updated {count}/{len(results)}...")
    
    print(f"Done! Updated {count} stocks.")

if __name__ == "__main__":
    update_cumulative()
