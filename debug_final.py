import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# Simple count to see number
c.execute('''
    SELECT COUNT(*) as count
    FROM stock_snapshot s
    JOIN stock_meta m ON s.code = m.code
    WHERE m.market_type IN ('TWSE', 'TPEx')
''')
count = c.fetchone()[0]
print(f"TOTAL JOINED COUNT: {count}")

# Check ORDER BY
c.execute('''
    SELECT s.code, s.name, s.foreign_buy
    FROM stock_snapshot s
    JOIN stock_meta m ON s.code = m.code
    WHERE m.market_type IN ('TWSE', 'TPEx')
    ORDER BY s.foreign_buy DESC
    LIMIT 10
''')
print("\nTop 10 by foreign_buy DESC:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()
