import sqlite3

c = sqlite3.connect('taiwan_stock.db').cursor()
c.execute('''
    SELECT code, name, trust_cumulative 
    FROM stock_snapshot 
    WHERE code IN ('3481', '2002', '2882', '2610', '1101')
''')
print("trust_cumulative values from stock_snapshot:")
for row in c.fetchall():
    code, name, tc = row
    print(f"  {code} {name}: trust_cumulative = {tc}")
