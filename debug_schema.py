import sqlite3

def check_schema():
    conn = sqlite3.connect('taiwan_stock.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(stock_snapshot);")
    columns = cursor.fetchall()
    print("Columns in stock_snapshot:")
    for col in columns:
        print(col)
    conn.close()

if __name__ == "__main__":
    check_schema()
