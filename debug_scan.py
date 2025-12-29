import sqlite3

def debug_scan():
    try:
        conn = sqlite3.connect('d:/twse/taiwan_stock.db')
        cursor = conn.cursor()
        
        # 1. Check total rows
        cursor.execute('SELECT count(*) FROM stock_snapshot')
        print(f"Total rows: {cursor.fetchone()[0]}")
        
        # 2. Check Built-in Strategy
        sql_builtin = """
        SELECT code, name, close, ma20, ma60, ma120, ma200 
        FROM stock_snapshot 
        WHERE volume >= 500 
        AND ma20 IS NOT NULL AND ma60 IS NOT NULL AND ma120 IS NOT NULL AND ma200 IS NOT NULL 
        AND close > ma20 AND close > ma60 AND close > ma120 AND close > ma200 
        AND (MAX(ma20, ma60, ma120, ma200) - MIN(ma20, ma60, ma120, ma200)) / MIN(ma20, ma60, ma120, ma200) <= 0.1
        """
        cursor.execute(sql_builtin)
        rows = cursor.fetchall()
        print(f"Built-in Matches: {len(rows)}")
        if rows:
            print("Sample:", rows[:3])
            
        # 3. Check MA Bull Strategy (Simpler)
        sql_ma = """
        SELECT code, name 
        FROM stock_snapshot 
        WHERE volume >= 500 
        AND ma20 > ma60 AND ma60 > ma120
        """
        cursor.execute(sql_ma)
        rows_ma = cursor.fetchall()
        print(f"MA Bull Matches: {len(rows_ma)}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    debug_scan()
