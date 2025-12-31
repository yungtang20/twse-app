import sqlite3
import requests

# First check local DB directly
conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# Get the date being used
c.execute("""
    SELECT MAX(date_int) as max_date FROM institutional_investors 
    WHERE foreign_holding_shares IS NOT NULL AND foreign_holding_shares != 0
""")
holding_date = c.fetchone()[0]
print(f"Latest holding date: {holding_date}")

# Check if 3481 (群創) has holding data
c.execute("""
    SELECT code, date_int, foreign_holding_shares, foreign_holding_pct 
    FROM institutional_investors 
    WHERE code = '3481' AND date_int = ?
""", (holding_date,))
result = c.fetchone()
print(f"群創 3481 holding data at {holding_date}: {result}")

# Check top 5 stocks with highest foreign_holding_shares
c.execute("""
    SELECT code, foreign_holding_shares, foreign_holding_pct 
    FROM institutional_investors 
    WHERE date_int = ? AND foreign_holding_shares > 0
    ORDER BY foreign_holding_shares DESC
    LIMIT 5
""", (holding_date,))
print(f"\nTop 5 stocks with holding data at {holding_date}:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()

# Now check API
print("\n--- API Response ---")
r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=3')
data = r.json()
for item in data['data']:
    print(f"{item['code']} {item['name']}: foreign_holding={item.get('foreign_holding_shares')}, pct={item.get('foreign_holding_pct')}")
