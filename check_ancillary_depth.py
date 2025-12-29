import sqlite3
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('附屬資料歷史長度檢查')
print('='*60)

# 1. 檢查 stock_history 中的估值欄位 (PE, PB, Yield)
print('\n【1. 估值資料 (PE/PB/Yield) in stock_history】')
# 抽樣檢查 2330
cur.execute("SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM stock_history WHERE code = '2330' AND pe IS NOT NULL")
row = cur.fetchone()
print(f'  2330 PE 有值筆數: {row[0]} ({row[1]} ~ {row[2]})')

# 2. 檢查 法人資料 (institutional_investors)
print('\n【2. 法人資料 (institutional_investors)】')
cur.execute("SELECT COUNT(DISTINCT date_int), MIN(date_int), MAX(date_int) FROM institutional_investors")
row = cur.fetchone()
print(f'  總交易日數: {row[0]} ({row[1]} ~ {row[2]})')
# 檢查特定股票
cur.execute("SELECT COUNT(*) FROM institutional_investors WHERE code = '2330'")
cnt = cur.fetchone()[0]
print(f'  2330 筆數: {cnt}')

# 3. 檢查 融資融券 (margin_data)
print('\n【3. 融資融券 (margin_data)】')
cur.execute("SELECT COUNT(DISTINCT date_int), MIN(date_int), MAX(date_int) FROM margin_data")
row = cur.fetchone()
print(f'  總交易日數: {row[0]} ({row[1]} ~ {row[2]})')
# 檢查特定股票
cur.execute("SELECT COUNT(*) FROM margin_data WHERE code = '2330'")
cnt = cur.fetchone()[0]
print(f'  2330 筆數: {cnt}')

# 4. 檢查 集保分佈 (shareholding) - 週資料
print('\n【4. 集保分佈 (shareholding)】')
cur.execute("SELECT COUNT(DISTINCT date_int), MIN(date_int), MAX(date_int) FROM shareholding")
row = cur.fetchone()
print(f'  總資料週數: {row[0]} ({row[1]} ~ {row[2]})')
# 檢查特定股票
cur.execute("SELECT COUNT(*) FROM shareholding WHERE code = '2330'")
cnt = cur.fetchone()[0]
print(f'  2330 筆數: {cnt}')

conn.close()
