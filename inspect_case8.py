import sqlite3
from collections import Counter

def inspect_case8():
    print("Inspecting Case 8 (All Empty) records...")
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    # Query Case 8: volume=0/null AND close=0/null AND amount=0/null
    sql = """
        SELECT code, date_int 
        FROM stock_history 
        WHERE (volume IS NULL OR volume = 0) 
          AND (close IS NULL OR close = 0) 
          AND (amount IS NULL OR amount = 0)
    """
    rows = cur.execute(sql).fetchall()
    print(f"Total Case 8 records: {len(rows)}")
    
    if not rows:
        return

    # Analyze by Stock
    stock_counts = Counter(r[0] for r in rows)
    print("\nTop 10 Stocks with Empty Records:")
    for code, count in stock_counts.most_common(10):
        print(f"  - {code}: {count} records")
        
    # Analyze by Date
    date_counts = Counter(r[1] for r in rows)
    print("\nTop 10 Dates with Empty Records:")
    for date_int, count in date_counts.most_common(10):
        print(f"  - {date_int}: {count} records")

    # Check if these stocks are delisted (if we have a way to know, e.g. not in stock_meta or marked)
    # For now, just listing them is a good start.

    conn.close()

if __name__ == "__main__":
    inspect_case8()
