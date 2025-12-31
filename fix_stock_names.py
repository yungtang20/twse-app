import sqlite3
import os

DB_FILE = 'd:\\twse\\taiwan_stock.db'

def normalize_name(name):
    if not name:
        return name
    return name.replace('股份有限公司', '').strip()

def fix_db_names():
    if not os.path.exists(DB_FILE):
        print(f"Database not found at {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    try:
        # 1. Update stock_meta
        print("Updating stock_meta...")
        cur.execute("SELECT code, name FROM stock_meta")
        rows = cur.fetchall()
        updated_count = 0
        for code, name in rows:
            if name and '股份有限公司' in name:
                new_name = normalize_name(name)
                cur.execute("UPDATE stock_meta SET name = ? WHERE code = ?", (new_name, code))
                updated_count += 1
        print(f"Updated {updated_count} rows in stock_meta")

        # 2. Update stock_snapshot
        print("Updating stock_snapshot...")
        # Check if table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_snapshot'")
        if cur.fetchone():
            cur.execute("SELECT code, name FROM stock_snapshot")
            rows = cur.fetchall()
            updated_count = 0
            for code, name in rows:
                if name and '股份有限公司' in name:
                    new_name = normalize_name(name)
                    cur.execute("UPDATE stock_snapshot SET name = ? WHERE code = ?", (new_name, code))
                    updated_count += 1
            print(f"Updated {updated_count} rows in stock_snapshot")

        # 3. Update institutional_investors (if it has name column, though usually it links by code)
        # Based on previous file reads, institutional_investors might not have name, but let's check.
        # Actually, let's check if there are other tables with names.
        # But stock_meta and stock_snapshot are the primary sources for display.
        
        conn.commit()
        print("Database update completed successfully.")
        
    except Exception as e:
        print(f"Error updating database: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    fix_db_names()
