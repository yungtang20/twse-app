import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

# 刪除 2330 2023/12/01 以前的資料
threshold_date = 20231201
print(f"刪除 {threshold_date} 以前的資料...")

cur.execute('DELETE FROM stock_shareholding_all WHERE code="2330" AND date_int < ?', (threshold_date,))
deleted_count = cur.rowcount
print(f"已刪除 stock_shareholding_all 筆數: {deleted_count}")

# 同步清除 stock_history 的 large_shareholder_pct
cur.execute('UPDATE stock_history SET large_shareholder_pct = NULL WHERE code="2330" AND date_int < ?', (threshold_date,))
updated_count = cur.rowcount
print(f"已清除 stock_history 欄位筆數: {updated_count}")

conn.commit()

# 檢查剩餘筆數
cur.execute('SELECT COUNT(*) FROM stock_shareholding_all WHERE code="2330"')
remaining = cur.fetchone()[0]
print(f"剩餘筆數: {remaining}")

# 檢查日期範圍
cur.execute('SELECT MIN(date_int), MAX(date_int) FROM stock_shareholding_all WHERE code="2330"')
print(f"剩餘日期範圍: {cur.fetchone()}")

conn.close()
