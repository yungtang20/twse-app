import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

# Get total holders for each date (sum of all levels)
cur.execute("""
    SELECT code, date_int, SUM(holders) as total_holders 
    FROM stock_shareholding_all 
    WHERE code='2330' 
    GROUP BY code, date_int 
    ORDER BY date_int DESC 
    LIMIT 5
""")
print("=== Total Holders by Date (2330) ===")
for row in cur.fetchall():
    print(f"Code: {row[0]}, Date: {row[1]}, Total Holders: {row[2]}")

# Get distinct dates
cur.execute("SELECT DISTINCT date_int FROM stock_shareholding_all ORDER BY date_int DESC LIMIT 5")
print("\n=== Distinct Dates ===")
for row in cur.fetchall():
    print(row)

conn.close()
