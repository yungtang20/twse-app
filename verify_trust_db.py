import sqlite3

c = sqlite3.connect('taiwan_stock.db').cursor()
c.execute('''
    SELECT code, trust_holding_shares, trust_holding_pct 
    FROM institutional_investors 
    WHERE code IN ('3481', '2002', '2882') AND date_int=20251226
''')
results = c.fetchall()
print("Trust holding data for ranking stocks (date=20251226):")
for row in results:
    print(f"  {row}")
