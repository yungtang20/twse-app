import sqlite3
import pandas as pd

DB_PATH = r"d:\twse\taiwan_stock.db"

def analyze_nulls():
    conn = sqlite3.connect(DB_PATH)
    
    print("Analyzing NULL close prices...")
    df = pd.read_sql_query("""
        SELECT code, date_int, open, high, low, close, volume
        FROM stock_history
        WHERE close IS NULL
        ORDER BY date_int DESC, code
    """, conn)
    
    print(f"Total rows with NULL close: {len(df)}")
    
    if not df.empty:
        print("\nSample missing records:")
        print(df.head(10))
        
        print("\nMissing by Date:")
        print(df.groupby('date_int').size().sort_values(ascending=False).head(10))
        
        print("\nMissing by Stock:")
        print(df.groupby('code').size().sort_values(ascending=False).head(10))
        
        # Save to CSV for reference
        df.to_csv("missing_close_data.csv", index=False)
        print("\nFull list saved to 'missing_close_data.csv'")
        
    conn.close()

if __name__ == "__main__":
    analyze_nulls()
