import sys
import os
import sqlite3

# Ensure we are in the right directory
os.chdir(r'd:\twse')
sys.path.append(r'd:\twse')

from 最終修正 import db_manager, ensure_db, step4_check_data_gaps

ensure_db()

print("1. Verifying repaired stocks (5310, 6212, 6236, 8921)...")
targets = ['5310', '6212', '6236', '8921']
dates = [20260102, 20251226]

with db_manager.get_connection() as conn:
    cur = conn.cursor()
    for code in targets:
        print(f"  Checking {code}:")
        for date_int in dates:
            row = cur.execute("SELECT close, volume FROM stock_history WHERE code=? AND date_int=?", (code, date_int)).fetchone()
            if row:
                print(f"    - {date_int}: Close={row[0]}, Vol={row[1]} (OK)")
            else:
                print(f"    - {date_int}: Missing (Still Gapped)")

print("\n2. Handling 8291 (Suspended)...")
# User confirmed 8291 is suspended. We mark it as such to stop gap warnings.
with db_manager.get_connection() as conn:
    cur = conn.cursor()
    # Update status to 'Suspended'
    cur.execute("UPDATE stock_meta SET status='Suspended' WHERE code='8291'")
    conn.commit()
    print("  8291 status updated to 'Suspended'.")

print("\n3. Running Final Gap Check (Step 4)...")
# This should now show 0 gaps for these stocks
step4_check_data_gaps()
