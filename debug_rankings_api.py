import sqlite3
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = r"d:\twse\taiwan_stock.db"

def debug_api(type="foreign", min_foreign_streak=1, min_trust_streak=1):
    print(f"Debugging API with type={type}, min_foreign_streak={min_foreign_streak}, min_trust_streak={min_trust_streak}")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Map type to column name (for sorting/filtering)
    column_map = {
        "foreign": "i.foreign_buy",
        "trust": "i.trust_buy",
        "dealer": "i.dealer_buy",
        "total": "(i.foreign_buy + i.trust_buy + i.dealer_buy)"
    }
    
    target_col = column_map.get(type)
    print(f"Target column: {target_col}")

    # Base Filter (Buy vs Sell Universe)
    # Assuming sort="buy"
    base_where = f"{target_col} > 0"
    
    where_clauses = [base_where]

    # Filters
    if min_foreign_streak != 0:
        op = ">=" if min_foreign_streak > 0 else "<="
        where_clauses.append(f"s.foreign_streak {op} {min_foreign_streak}")

    if min_trust_streak != 0:
        op = ">=" if min_trust_streak > 0 else "<="
        where_clauses.append(f"s.trust_streak {op} {min_trust_streak}")

    where_clause = " AND ".join(where_clauses)
    print(f"Where clause: {where_clause}")

    # Get latest date
    cursor.execute("SELECT MAX(date_int) FROM institutional_investors")
    latest_date = cursor.fetchone()[0]
    print(f"Latest date: {latest_date}")

    sql = f"""
        SELECT 
            s.code, s.name, 
            s.foreign_streak, s.trust_streak
        FROM stock_snapshot s
        JOIN stock_meta m ON s.code = m.code
        LEFT JOIN institutional_investors i ON s.code = i.code AND i.date_int = {latest_date}
        WHERE m.market_type IN ('TWSE', 'TPEx') 
          AND {where_clause}
        LIMIT 5
    """
    
    try:
        print(f"Executing SQL: {sql}")
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"Results count: {len(results)}")
        for row in results:
            print(dict(row))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_api()
