import sqlite3
import os

DB_PATH = 'd:/twse/taiwan_stock.db'

def fix_remaining_ohlcv():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        print("Starting remaining OHLCV repair...")

        # 1. Fix missing Open/High/Low -> Set to Close
        cursor.execute("""
            UPDATE stock_history
            SET open = close,
                high = close,
                low = close
            WHERE (open IS NULL OR high IS NULL OR low IS NULL)
              AND close IS NOT NULL
        """)
        print(f"Fixed missing OHLC for {cursor.rowcount} records.")

        # 2. Fix missing Volume -> Set to 0
        cursor.execute("""
            UPDATE stock_history
            SET volume = 0
            WHERE volume IS NULL
        """)
        print(f"Fixed missing Volume for {cursor.rowcount} records.")

        conn.commit()
        print("Repair completed successfully.")

    except Exception as e:
        print(f"Error during repair: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_remaining_ohlcv()
