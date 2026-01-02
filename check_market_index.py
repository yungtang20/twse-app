import sqlite3
import os

db_path = r"d:\twse\taiwan_stock.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=== Market Index Data (最近 10 筆，按日期排序) ===")
cursor.execute("""
    SELECT date_int, index_id, close, volume 
    FROM market_index 
    ORDER BY date_int DESC 
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f"Date: {row[0]}, Index: {row[1]}, Close: {row[2]}, Vol: {row[3]}")

print("\n=== Institutional Investors Table Schema ===")
cursor.execute("PRAGMA table_info(institutional_investors)")
for col in cursor.fetchall():
    print(f"  {col[1]} ({col[2]})")

print("\n=== 今日 (20260102) 法人資料筆數 ===")
cursor.execute("SELECT COUNT(*) FROM institutional_investors WHERE date_int = 20260102")
print(f"法人買賣超: {cursor.fetchone()[0]} 筆")

print("\n=== 今日 (20260102) 市場指數資料筆數 ===")
cursor.execute("SELECT COUNT(*) FROM market_index WHERE date_int = 20260102")
print(f"市場指數: {cursor.fetchone()[0]} 筆")

print("\n=== 各日期市場指數資料筆數 (最近 5 天) ===")
cursor.execute("""
    SELECT date_int, COUNT(*) as cnt
    FROM market_index
    GROUP BY date_int
    ORDER BY date_int DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"Date: {row[0]}, Count: {row[1]}")

conn.close()
