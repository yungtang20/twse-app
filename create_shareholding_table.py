import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_shareholding_all (
        code TEXT,
        date_int INTEGER,
        level INTEGER,
        holders INTEGER,
        shares INTEGER,
        proportion REAL,
        PRIMARY KEY (code, date_int, level)
    )
""")
conn.commit()
conn.close()
print("Table stock_shareholding_all created.")
