import time
import sqlite3
import os

DB_PATH = 'taiwan_stock.db'

def test_query():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = """
        SELECT 
            m.code, m.name,
            s.close, 
            ROUND((s.close - s.close_prev) / s.close_prev * 100, 2) as change_pct, 
            s.volume, 
            CAST(s.close * s.volume * 1000 AS INTEGER) as amount,
            s.ma3 as ma5, s.ma20, s.ma60, s.ma120, s.ma200,
            s.rsi12 as rsi, NULL as mfi, s.kdj_k as k, s.kdj_d as d,
            s.vp_poc, s.vp_upper, s.vp_lower,
            s.vsbc_upper, s.vsbc_lower,
            s.vol_ma60, s.ma25
        FROM stock_meta m
        JOIN stock_snapshot s ON m.code = s.code
        WHERE m.code GLOB '[0-9][0-9][0-9][0-9]'
        AND s.volume >= 500
        AND s.ma20 IS NOT NULL AND s.ma60 IS NOT NULL AND s.ma20 > s.ma60
        ORDER BY ((s.close - s.close_prev) / s.close_prev) DESC
        LIMIT 30
    """
    
    start = time.time()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        end = time.time()
        print(f"Query executed in {end - start:.4f} seconds")
        print(f"Returned {len(rows)} rows")
    except Exception as e:
        print(f"Query failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_query()
