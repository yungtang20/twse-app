import sqlite3
from pathlib import Path

DB_PATH = Path(r"d:\twse\taiwan_stock.db")

def migrate_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Check if column exists
    cursor.execute("PRAGMA table_info(stock_meta)")
    columns = [r[1] for r in cursor.fetchall()]
    
    if 'total_shares' not in columns:
        print("Adding total_shares column to stock_meta...")
        cursor.execute("ALTER TABLE stock_meta ADD COLUMN total_shares INTEGER DEFAULT 0")
    else:
        print("total_shares column already exists.")
        
    # 2. Populate total_shares
    print("Populating total_shares from stock_shareholding_all...")
    
    # Get latest date for each stock
    # We want the latest level 17 (total) shares
    sql = """
    UPDATE stock_meta
    SET total_shares = (
        SELECT shares 
        FROM stock_shareholding_all s
        WHERE s.code = stock_meta.code 
          AND s.level = 17
        ORDER BY s.date_int DESC
        LIMIT 1
    )
    WHERE EXISTS (
        SELECT 1 
        FROM stock_shareholding_all s
        WHERE s.code = stock_meta.code 
          AND s.level = 17
    )
    """
    
    try:
        cursor.execute(sql)
        print(f"Updated {cursor.rowcount} rows.")
        conn.commit()
    except Exception as e:
        print(f"Error updating total_shares: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_db()
