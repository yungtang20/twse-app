import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

tables = [
    ('stock_data', 'date'), 
    ('stock_shareholding_all', 'date_int'), 
    ('institutional_investors', 'date_int'), 
    ('margin_data', 'date_int')
]

print('='*60)
print('附屬資料表詳細檢查')
print('='*60)

for tbl, date_col in tables:
    print(f'\n【{tbl}】')
    
    # 1. Schema
    cur.execute(f"PRAGMA table_info({tbl})")
    cols = [row[1] for row in cur.fetchall()]
    # print(f'  Columns: {cols}') # 避免輸出過長
    
    # Check for PE/PB
    has_pe = 'pe' in cols or 'PE' in cols
    has_pb = 'pb' in cols or 'PB' in cols
    print(f'  Has PE/PB: {has_pe}/{has_pb}')
    
    # 2. Depth
    try:
        cur.execute(f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {tbl}")
        row = cur.fetchone()
        print(f'  Total Rows: {row[0]}')
        print(f'  Date Range: {row[1]} ~ {row[2]}')
        
        # 3. Check 2330 depth
        cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE code = '2330'")
        cnt_2330 = cur.fetchone()[0]
        print(f'  2330 Count: {cnt_2330}')
        
        # 4. Check 6904 depth (for comparison)
        cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE code = '6904'")
        cnt_6904 = cur.fetchone()[0]
        print(f'  6904 Count: {cnt_6904}')
        
    except Exception as e:
        print(f'  Error checking depth: {e}')

conn.close()
