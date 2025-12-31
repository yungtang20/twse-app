import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

c.execute('SELECT MAX(date_int) FROM institutional_investors')
max_date = c.fetchone()[0]
print(f'Max date_int in institutional_investors: {max_date}')

c.execute('SELECT COUNT(*) FROM stock_meta WHERE market_type IN ("TWSE", "TPEx")')
print(f'Stock meta TWSE/TPEx count: {c.fetchone()[0]}')

c.execute('SELECT COUNT(*) FROM stock_snapshot s JOIN stock_meta m ON s.code = m.code WHERE m.market_type IN ("TWSE", "TPEx")')
print(f'Joined snapshot+meta count: {c.fetchone()[0]}')

# Check if the query works without any join
c.execute('SELECT COUNT(*) FROM stock_snapshot')
print(f'Total stock_snapshot rows: {c.fetchone()[0]}')

conn.close()
