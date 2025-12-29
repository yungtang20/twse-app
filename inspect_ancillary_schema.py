import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

tables = ['stock_data', 'stock_shareholding_all', 'institutional_investors', 'margin_data']

print('='*60)
print('附屬資料表 Schema 與深度檢查')
print('='*60)

for tbl in tables:
    print(f'\n【{tbl}】')
    
    # 1. Schema
    cur.execute(f"PRAGMA table_info({tbl})")
    cols = [row[1] for row in cur.fetchall()]
    print(f'  Columns: {cols}')
    
    # 2. Depth
    try:
        cur.execute(f"SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM {tbl}")
        row = cur.fetchone()
        print(f'  Count: {row[0]}')
        print(f'  Range: {row[1]} ~ {row[2]}')
        
        # 3. Check 2330 depth
        cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE code = '2330'")
        cnt_2330 = cur.fetchone()[0]
        print(f'  2330 Count: {cnt_2330}')
        
    except Exception as e:
        print(f'  Error checking depth: {e}')

conn.close()
