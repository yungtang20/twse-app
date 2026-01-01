import sqlite3
import pandas as pd

conn = sqlite3.connect('taiwan_stock.db')
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(stock_snapshot)")
columns = [info[1] for info in cursor.fetchall()]
print("Checking for holding columns in stock_snapshot...")
for col in columns:
    if 'holding' in col:
        print(f"Found: {col}")

print("-" * 20)
# Check if data exists in these columns if they exist
holding_cols = ['foreign_holding_shares', 'foreign_holding_pct', 'trust_holding_shares', 'trust_holding_pct']
existing_cols = [c for c in holding_cols if c in columns]

if existing_cols:
    print(f"Found holding columns: {existing_cols}")
    df = pd.read_sql(f"SELECT code, {', '.join(existing_cols)} FROM stock_snapshot LIMIT 5", conn)
    print(df)
else:
    print("No holding columns found in stock_snapshot")

conn.close()
