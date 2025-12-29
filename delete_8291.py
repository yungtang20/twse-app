import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('強制刪除 8291')
print('='*60)

cur.execute("DELETE FROM stock_history WHERE code = '8291'")
rows_hist = cur.rowcount
cur.execute("DELETE FROM stock_meta WHERE code = '8291'")
rows_meta = cur.rowcount
conn.commit()

print(f'已刪除 stock_history: {rows_hist} 筆')
print(f'已刪除 stock_meta: {rows_meta} 筆')

# 再次檢查空值
print('\n【最終空值檢查】')
cur.execute('SELECT COUNT(*) FROM stock_history WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL')
cnt = cur.fetchone()[0]
print(f'剩餘 OHLCV 空值: {cnt}')

conn.close()
