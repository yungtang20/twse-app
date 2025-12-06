import sqlite3

conn = sqlite3.connect('d:/twse/taiwan_stock.db')
cur = conn.cursor()

# Check if tables exist
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
table_names = [t[0] for t in tables]
print('=== 現有表格 ===')
for t in table_names:
    print(f'  - {t}')

# Check stock_data row count
cur.execute('SELECT COUNT(*) FROM stock_data')
stock_data_count = cur.fetchone()[0]
print(f'\n舊表 stock_data: {stock_data_count:,} 筆')

# Check new tables exist
for table in ['stock_meta', 'stock_history', 'stock_snapshot']:
    if table in table_names:
        cur.execute(f'SELECT COUNT(*) FROM {table}')
        count = cur.fetchone()[0]
        print(f'新表 {table}: {count:,} 筆')
    else:
        print(f'新表 {table}: 尚未建立')

conn.close()
print('\n✓ 驗證完成')
