import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# Check sample codes from both tables
c.execute("SELECT code FROM stock_snapshot LIMIT 5")
print("Sample codes from stock_snapshot:")
for row in c.fetchall():
    print(f"  '{row[0]}'")

c.execute("SELECT code FROM stock_meta LIMIT 5")
print("\nSample codes from stock_meta:")
for row in c.fetchall():
    print(f"  '{row[0]}'")

# Check if there are codes in stock_snapshot that DON'T exist in stock_meta
c.execute('''
    SELECT COUNT(*) FROM stock_snapshot s
    WHERE NOT EXISTS (SELECT 1 FROM stock_meta m WHERE m.code = s.code)
''')
print(f"\nstock_snapshot rows NOT in stock_meta: {c.fetchone()[0]}")

# Check if stock_snapshot has duplicate or old stock codes
c.execute("SELECT code, name, close FROM stock_snapshot ORDER BY code LIMIT 10")
print("\nFirst 10 stock_snapshot rows:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()
