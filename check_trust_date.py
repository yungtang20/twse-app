import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# Check what date the API will use for institutional_investors
c.execute("""
    SELECT MAX(date_int) as max_date FROM institutional_investors 
    WHERE foreign_holding_shares IS NOT NULL AND foreign_holding_shares != 0
""")
foreign_date = c.fetchone()[0]
print(f"Date with foreign holding data: {foreign_date}")

c.execute("""
    SELECT MAX(date_int) as max_date FROM institutional_investors 
    WHERE trust_holding_shares IS NOT NULL AND trust_holding_shares > 0
""")
trust_date = c.fetchone()[0]
print(f"Date with trust holding data: {trust_date}")

# Check specific stocks
c.execute("""
    SELECT code, date_int, trust_holding_shares, trust_holding_pct 
    FROM institutional_investors 
    WHERE code = '3481' AND trust_holding_shares > 0
    ORDER BY date_int DESC LIMIT 3
""")
print(f"\n群創 3481 trust holding records:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()
