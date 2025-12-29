import sqlite3
import sys

# Mock print_flush
def print_flush(msg):
    print(msg)

def test_check_db_nulls():
    print("Testing check_db_nulls logic...")
    conn = sqlite3.connect('taiwan_stock.db')
    
    try:
        # 1. Total Count
        cur = conn.execute("SELECT COUNT(*) FROM stock_history")
        total = cur.fetchone()[0]
        print(f"Total: {total}")
        
        # 2. Null Counts
        nulls = {}
        for col in ['open', 'high', 'low', 'close', 'volume']:
            cur = conn.execute(f"SELECT COUNT(*) FROM stock_history WHERE {col} IS NULL")
            nulls[col] = cur.fetchone()[0]
            
        for col, count in nulls.items():
            pct = (count / total) * 100 if total > 0 else 0
            print(f"- {col} nulls: {count} ({pct:.2f}%)")
            
        # 3. Detailed Analysis (Close)
        if nulls['close'] > 0:
            print("\n[Detailed Analysis (Close)]")
            
            print("Top 5 Missing Stocks:")
            cur = conn.execute("""
                SELECT code, COUNT(*) as cnt 
                FROM stock_history 
                WHERE close IS NULL 
                GROUP BY code 
                ORDER BY cnt DESC 
                LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"  - {row[0]}: {row[1]} records")
                
            print("\nTop 5 Missing Dates:")
            cur = conn.execute("""
                SELECT date_int, COUNT(*) as cnt 
                FROM stock_history 
                WHERE close IS NULL 
                GROUP BY date_int 
                ORDER BY cnt DESC 
                LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"  - {row[0]}: {row[1]} records")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_check_db_nulls()
