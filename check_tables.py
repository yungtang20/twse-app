import sqlite3
import sys

try:
    with open('schema_match_utf8.txt', 'w', encoding='utf-8') as f:
        conn = sqlite3.connect('taiwan_stock.db')
        cursor = conn.cursor()
        
        # List all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        f.write(f"Tables: {tables}\n")
        
        keywords = ['tdcc', 'hold', 'large', 'level', 'count', 'percent']
        
        # Check schema for each table
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Check if table name or any column matches keywords
            if any(k in table.lower() for k in keywords) or any(k in str(columns).lower() for k in keywords):
                f.write(f"\n--- MATCH FOUND IN {table} ---\n")
                f.write(f"Columns: {columns}\n")
            else:
                f.write(f"Checked {table} - No match\n")
            
        conn.close()
    print("Done writing to schema_match_utf8.txt")
except Exception as e:
    print(f"Error: {e}")
