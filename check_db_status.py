import sqlite3
import pandas as pd
import os

DB_PATH = r"d:\twse\taiwan_stock.db"
STOCK_LIST_PATH = r"d:\twse\stock_list.csv"

def get_a_rule_stocks():
    if not os.path.exists(STOCK_LIST_PATH):
        print("stock_list.csv not found!")
        return []
    
    df = pd.read_csv(STOCK_LIST_PATH, dtype=str)
    df = df[df['market'].isin(['TWSE', 'TPEX'])]
    
    a_rule_stocks = []
    for _, row in df.iterrows():
        code = row['code']
        if len(code) != 4: continue
        if not code.isdigit(): continue
        if code.startswith('0'): continue
        a_rule_stocks.append(code)
    return set(a_rule_stocks)

def check_db():
    with open("db_report.txt", "w", encoding="utf-8") as f:
        def log(msg):
            print(msg)
            f.write(msg + "\n")

        if not os.path.exists(DB_PATH):
            log(f"Database not found: {DB_PATH}")
            return

        log(f"Checking database: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. Check Tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        log(f"Tables found: {tables}")

        a_rule_stocks = get_a_rule_stocks()
        log(f"Total A-Rule Stocks target: {len(a_rule_stocks)}")

        # 2. Check TDCC Data (stock_shareholding_all)
        if 'stock_shareholding_all' in tables:
            cursor.execute("SELECT COUNT(DISTINCT code) FROM stock_shareholding_all")
            tdcc_count = cursor.fetchone()[0]
            log(f"\n[TDCC Data] Stocks with data: {tdcc_count}")
            
            cursor.execute("SELECT DISTINCT code FROM stock_shareholding_all")
            existing_tdcc = set(row[0] for row in cursor.fetchall())
            
            missing_tdcc = a_rule_stocks - existing_tdcc
            log(f"Missing TDCC Stocks (Coverage): {len(missing_tdcc)}")
            
            # Check Depth
            log("\n[TDCC Depth Analysis]")
            cursor.execute("""
                SELECT code, COUNT(*), MIN(date_int), MAX(date_int) 
                FROM stock_shareholding_all 
                GROUP BY code
            """)
            rows = cursor.fetchall()
            
            low_data_stocks = []
            for r in rows:
                code, cnt, min_d, max_d = r
                if cnt < 10: # Arbitrary threshold for "low data"
                    low_data_stocks.append(code)
            
            log(f"Stocks with < 10 records: {len(low_data_stocks)}")
            if len(low_data_stocks) > 0:
                log(f"Example low data stocks: {low_data_stocks[:10]}")
                
            # Overall Date Range
            cursor.execute("SELECT MIN(date_int), MAX(date_int) FROM stock_shareholding_all")
            min_all, max_all = cursor.fetchone()
            log(f"Overall Date Range: {min_all} to {max_all}")

        else:
            log("\n[TDCC Data] Table 'stock_shareholding_all' NOT FOUND.")

        # 3. Check Price History (stock_history)
        if 'stock_history' in tables:
            cursor.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
            history_count = cursor.fetchone()[0]
            log(f"\n[Price History] Stocks with data: {history_count}")
            
            cursor.execute("SELECT DISTINCT code FROM stock_history")
            existing_history = set(row[0] for row in cursor.fetchall())
            
            missing_history = a_rule_stocks - existing_history
            log(f"Missing Price History Stocks: {len(missing_history)}")
        else:
            log("\n[Price History] Table 'stock_history' NOT FOUND.")

        conn.close()

if __name__ == "__main__":
    check_db()
