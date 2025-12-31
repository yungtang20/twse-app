import sqlite3

def inspect():
    conn = sqlite3.connect('taiwan_stock.db')
    cursor = conn.cursor()
    
    with open('db_schema.txt', 'w', encoding='utf-8') as f:
        # Check stock_snapshot columns
        cursor.execute("PRAGMA table_info(stock_snapshot)")
        columns = [row[1] for row in cursor.fetchall()]
        f.write(f"Stock Snapshot Columns: {columns}\n")
            
    conn.close()

if __name__ == "__main__":
    inspect()
