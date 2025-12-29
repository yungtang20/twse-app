import sqlite3

def check_need_crawl():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    print("Checking Case 4, 6, 7 (Need Crawl)...")
    cur.execute("""
        SELECT COUNT(*)
        FROM stock_history 
        WHERE (volume > 0 AND (close IS NULL OR close = 0) AND (amount IS NULL OR amount = 0))
           OR ((volume IS NULL OR volume = 0) AND close > 0 AND (amount IS NULL OR amount = 0))
           OR ((volume IS NULL OR volume = 0) AND (close IS NULL OR close = 0) AND amount > 0)
    """)
    count = cur.fetchone()[0]
    print(f"Need Crawl Count: {count}")
    
    conn.close()

if __name__ == "__main__":
    check_need_crawl()
