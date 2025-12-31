import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
cur = conn.execute('PRAGMA table_info(stock_snapshot)')
cols = cur.fetchall()

with open('supabase_stock_snapshot.sql', 'w', encoding='utf-8') as f:
    f.write('-- Supabase stock_snapshot table creation SQL\n')
    f.write('CREATE TABLE IF NOT EXISTS stock_snapshot (\n')

    col_defs = []
    for c in cols:
        col_name = c[1]
        col_type = c[2]
        
        if col_type == 'TEXT':
            supabase_type = 'TEXT'
        elif col_type == 'REAL':
            supabase_type = 'REAL'
        else:
            supabase_type = 'BIGINT'
        
        if col_name == 'code':
            col_defs.append(f'  {col_name} TEXT PRIMARY KEY')
        else:
            col_defs.append(f'  {col_name} {supabase_type}')

    f.write(',\n'.join(col_defs))
    f.write('\n);\n')

print(f'Generated supabase_stock_snapshot.sql with {len(cols)} columns')

