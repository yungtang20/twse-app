import sqlite3
from datetime import datetime, timedelta

DB_PATH = 'd:/twse/taiwan_stock.db'

def analyze_gaps():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Analyzing stocks with < 450 records...")
    
    # Get all stocks with < 450 records
    cursor.execute("""
        SELECT h.code, m.name, m.list_date, COUNT(*) as cnt 
        FROM stock_history h 
        LEFT JOIN stock_meta m ON h.code = m.code 
        WHERE LENGTH(h.code)=4 
          AND h.code GLOB '[0-9][0-9][0-9][0-9]' 
          AND CAST(h.code AS INTEGER) >= 1101
          AND CAST(h.code AS INTEGER) < 9000
        GROUP BY h.code 
        HAVING cnt < 450 
        ORDER BY cnt ASC
    """)
    rows = cursor.fetchall()
    
    cutoff_date = (datetime.now() - timedelta(days=630)).strftime('%Y-%m-%d')
    print(f"Cutoff Date for New Listing: {cutoff_date}")
    
    missing_history = []
    new_listings = []
    unknown_status = []
    
    for code, name, list_date, cnt in rows:
        if not list_date:
            unknown_status.append((code, name, cnt))
            continue
            
        if list_date < cutoff_date:
            missing_history.append((code, name, list_date, cnt))
        else:
            new_listings.append((code, name, list_date, cnt))
            
    print(f"\nFound {len(rows)} stocks with < 450 records.")
    print(f"  - New Listings (OK): {len(new_listings)}")
    print(f"  - Missing History (NEED FIX): {len(missing_history)}")
    print(f"  - Unknown Status (CHECK): {len(unknown_status)}")
    
    if missing_history:
        print("\n=== Stocks Needing Backfill (Old but < 450 records) ===")
        for code, name, ld, cnt in missing_history:
            print(f"{code} {name} Listed:{ld} Records:{cnt}")
            
    if unknown_status:
        print("\n=== Stocks with Unknown List Date ===")
        for code, name, cnt in unknown_status:
            print(f"{code} {name} Records:{cnt}")

    conn.close()

if __name__ == "__main__":
    analyze_gaps()
