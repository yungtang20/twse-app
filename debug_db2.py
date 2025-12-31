import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# Check market_type values in stock_meta
c.execute('SELECT DISTINCT market_type, COUNT(*) FROM stock_meta GROUP BY market_type')
print("Market types in stock_meta:")
for row in c.fetchall():
    print(f"  {row}")

# Check if stock_snapshot code matches stock_meta code
c.execute('''
    SELECT COUNT(*) FROM stock_snapshot s
    WHERE EXISTS (SELECT 1 FROM stock_meta m WHERE m.code = s.code)
''')
print(f"\nstock_snapshot rows that exist in stock_meta: {c.fetchone()[0]}")

# Try the actual query without institutional_investors join
c.execute('''
    SELECT COUNT(*) FROM stock_snapshot s
    JOIN stock_meta m ON s.code = m.code
    WHERE m.market_type IN ('TWSE', 'TPEx')
''')
print(f"Joined count (TWSE/TPEx): {c.fetchone()[0]}")

# Try with just TWSE
c.execute('''
    SELECT COUNT(*) FROM stock_snapshot s
    JOIN stock_meta m ON s.code = m.code
    WHERE m.market_type = 'TWSE'
''')
print(f"Joined count (TWSE only): {c.fetchone()[0]}")

# Check if market_type might have different casing or values
c.execute("SELECT DISTINCT market_type FROM stock_meta LIMIT 10")
print("\nFirst 10 distinct market_type values:")
for row in c.fetchall():
    print(f"  '{row[0]}'")

conn.close()
