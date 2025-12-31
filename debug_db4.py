import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# Try the exact query from rankings.py
c.execute('''
    SELECT COUNT(*) as count
    FROM stock_snapshot s
    JOIN stock_meta m ON s.code = m.code
    WHERE m.market_type IN ('TWSE', 'TPEx')
      AND 1=1
''')
print(f"Query with 1=1 where clause: {c.fetchone()[0]}")

# Check market_type exact values
c.execute("SELECT market_type, COUNT(*) FROM stock_meta GROUP BY market_type")
print("\nAll market_type values and counts:")
for row in c.fetchall():
    print(f"  '{row[0]}': {row[1]}")

# Check if there's a mismatch in JOIN
c.execute('''
    SELECT s.code, m.code, m.market_type
    FROM stock_snapshot s
    LEFT JOIN stock_meta m ON s.code = m.code
    WHERE m.code IS NULL
    LIMIT 5
''')
print("\nstock_snapshot codes NOT FOUND in stock_meta:")
for row in c.fetchall():
    print(f"  {row}")

# Check the actual join count
c.execute('''
    SELECT s.code, m.market_type
    FROM stock_snapshot s
    JOIN stock_meta m ON s.code = m.code
    LIMIT 10
''')
print("\nFirst 10 successfully joined rows:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()
