import sys
import os
import sqlite3

# Ensure we are in the right directory
os.chdir(r'd:\twse')
sys.path.append(r'd:\twse')

from 最終修正 import db_manager, ensure_db

ensure_db()

with db_manager.get_connection() as conn:
    cur = conn.cursor()
    row = cur.execute('SELECT * FROM stock_meta WHERE code=?', ('8291',)).fetchone()
    print(f"8291 Meta: {row}")
    
    # Check columns
    cur.execute("PRAGMA table_info(stock_meta)")
    cols = cur.fetchall()
    print("Columns:", [c[1] for c in cols])
