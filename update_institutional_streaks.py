from backend.services.db import db_manager
import pandas as pd

def calculate_streak_info(values):
    """
    Calculate consecutive streak and cumulative sum from a list of values (latest first).
    Returns (streak_count, streak_sum)
    streak_count: Positive for buy streak, negative for sell streak.
    streak_sum: Sum of values during the streak.
    """
    if not values:
        return 0, 0
    
    current = values[0]
    if current == 0:
        return 0, 0
    
    streak = 0
    streak_sum = 0
    is_buy = current > 0
    
    for val in values:
        if val == 0:
            break
        if (val > 0) == is_buy:
            streak += 1
            streak_sum += val
        else:
            break
            
    final_streak = streak if is_buy else -streak
    return final_streak, streak_sum

def update_streaks():
    print("Fetching stock list...")
    stocks = db_manager.execute_query("SELECT code FROM stock_snapshot")
    total = len(stocks)
    print(f"Found {total} stocks. Starting update...")
    
    count = 0
    for stock in stocks:
        code = stock['code']
        
        # Get history (last 60 days to be safe)
        query = """
            SELECT foreign_buy, trust_buy, dealer_buy 
            FROM stock_history 
            WHERE code = ? 
            ORDER BY date_int DESC 
            LIMIT 60
        """
        history = db_manager.execute_query(query, (code,))
        
        if not history:
            continue
            
        f_vals = [h['foreign_buy'] or 0 for h in history]
        t_vals = [h['trust_buy'] or 0 for h in history]
        d_vals = [h['dealer_buy'] or 0 for h in history]
        
        f_streak, f_sum = calculate_streak_info(f_vals)
        t_streak, t_sum = calculate_streak_info(t_vals)
        d_streak, d_sum = calculate_streak_info(d_vals)
        
        update_sql = """
            UPDATE stock_snapshot 
            SET foreign_streak = ?, foreign_cumulative = ?,
                trust_streak = ?, trust_cumulative = ?,
                dealer_streak = ?, dealer_cumulative = ?
            WHERE code = ?
        """
        db_manager.execute_update(update_sql, (f_streak, f_sum, t_streak, t_sum, d_streak, d_sum, code))
        
        count += 1
        if count % 100 == 0:
            print(f"Updated {count}/{total} stocks...")

    print("Update complete!")

if __name__ == "__main__":
    update_streaks()
