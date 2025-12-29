from backend.services.db import db_manager

def add_column_if_not_exists(table, column, type_def):
    try:
        # Check if column exists
        cols = db_manager.execute_query(f"PRAGMA table_info({table})")
        col_names = [c['name'] for c in cols]
        
        if column not in col_names:
            print(f"Adding column {column} to {table}...")
            db_manager.execute_update(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}")
            print(f"Column {column} added.")
        else:
            print(f"Column {column} already exists.")
    except Exception as e:
        print(f"Error adding {column}: {e}")

if __name__ == "__main__":
    add_column_if_not_exists("stock_snapshot", "foreign_streak", "INTEGER DEFAULT 0")
    add_column_if_not_exists("stock_snapshot", "trust_streak", "INTEGER DEFAULT 0")
    add_column_if_not_exists("stock_snapshot", "dealer_streak", "INTEGER DEFAULT 0")
