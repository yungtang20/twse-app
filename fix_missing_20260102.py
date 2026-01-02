import sqlite3
import os

DB_PATH = 'd:/twse/taiwan_stock.db'

def fix_missing_data():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        print("Starting data repair (Phase 2)...")

        # Data to insert/update
        # (code, date_int, close_price)
        updates = [
            # 3064 泰偉 (Stable at 50.40)
            ('3064', 20251229, 50.40),
            
            # 6904 伯鑫 (No trade days, filling with estimated close)
            # Dec dates (Stable at 115.0)
            ('6904', 20251223, 115.0),
            ('6904', 20251219, 115.0),
            ('6904', 20251215, 115.0),
            # Nov dates (Previous close was 128 on 11/13)
            ('6904', 20251119, 128.0), 
            ('6904', 20251117, 128.0),
            ('6904', 20251114, 128.0),
        ]

        for code, date_int, price in updates:
            # Check if row exists
            cursor.execute("SELECT open, high, low, close, volume FROM stock_history WHERE code = ? AND date_int = ?", (code, date_int))
            row = cursor.fetchone()

            if row:
                print(f"Updating {code} on {date_int} with close={price}")
                cursor.execute("""
                    UPDATE stock_history 
                    SET close = ?,
                        open = COALESCE(open, ?),
                        high = COALESCE(high, ?),
                        low = COALESCE(low, ?)
                    WHERE code = ? AND date_int = ?
                """, (price, price, price, price, code, date_int))
            else:
                print(f"Inserting {code} on {date_int} with close={price}")
                cursor.execute("""
                    INSERT INTO stock_history (code, date_int, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                """, (code, date_int, price, price, price, price))

        conn.commit()
        print("Repair Phase 2 completed successfully.")

    except Exception as e:
        print(f"Error during repair: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_missing_data()
