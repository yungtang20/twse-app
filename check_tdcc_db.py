import sqlite3
conn = sqlite3.connect('taiwan_stock.db')
cur = conn.cursor()

# List all tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print("Tables:", tables)

# Check for TDCC related tables
for t in tables:
    if 'tdcc' in t.lower() or 'hold' in t.lower() or 'share' in t.lower():
        print(f"\n=== {t} ===")
        cur.execute(f"PRAGMA table_info({t})")
        cols = cur.fetchall()
        print("Columns:", [c[1] for c in cols])
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print("Count:", cur.fetchone()[0])
        cur.execute(f"SELECT * FROM {t} LIMIT 3")
        for row in cur.fetchall():
            print(row)

# Check stock_snapshot columns
cur.execute("PRAGMA table_info(stock_snapshot)")
snapshot_cols = [c[1] for c in cur.fetchall()]
print("\n=== stock_snapshot columns ===")
print(snapshot_cols)

# Check stock_history columns
cur.execute("PRAGMA table_info(stock_history)")
history_cols = [c[1] for c in cur.fetchall()]
print("\n=== stock_history columns ===")
print(history_cols)

# Check stock_shareholding_all
cur.execute("SELECT * FROM stock_shareholding_all WHERE code='2330' ORDER BY date_int DESC LIMIT 20")
print("\n=== stock_shareholding_all (2330) ===")
for row in cur.fetchall():
    print(row)

conn.close()
