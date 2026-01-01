import os
import sys
import sqlite3
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from backend.services.db import db_manager

def sync_snapshot_to_cloud():
    print("Starting sync stock_snapshot to cloud...")
    
    # Force connect
    db_manager.is_cloud_mode = True
    if not db_manager.supabase:
        db_manager.connect_supabase()
        
    if not db_manager.supabase:
        print("Failed to connect to Supabase")
        return

    # Read local SQLite
    print("Reading local SQLite data...")
    conn = sqlite3.connect('taiwan_stock.db')
    df = pd.read_sql("SELECT * FROM stock_snapshot", conn)
    # Get INT columns from schema to force integer type
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(stock_snapshot)")
    columns_info = cursor.fetchall()
    int_columns = [col[1] for col in columns_info if "INT" in col[2].upper()]
    
    # Also add holding columns to int_columns if they are integers
    int_columns.extend(['foreign_holding_shares', 'trust_holding_shares'])
    
    print(f"Found {len(int_columns)} INT columns to cast")
    
    # Fetch latest holding data
    print("Fetching latest institutional holding data...")
    try:
        cursor.execute("SELECT MAX(date_int) FROM institutional_investors WHERE foreign_holding_shares IS NOT NULL")
        latest_date_res = cursor.fetchone()
        latest_date = latest_date_res[0] if latest_date_res else None
        
        if latest_date:
            print(f"Latest holding date: {latest_date}")
            df_holding = pd.read_sql(f"""
                SELECT code, foreign_holding_shares, foreign_holding_pct, trust_holding_shares, trust_holding_pct 
                FROM institutional_investors 
                WHERE date_int = {latest_date}
            """, conn)
            
            # Merge with snapshot
            print(f"Merging holding data for {len(df_holding)} stocks...")
            df = pd.merge(df, df_holding, on='code', how='left')
            
            # Fill NaN with 0 for holding columns
            for col in ['foreign_holding_shares', 'foreign_holding_pct', 'trust_holding_shares', 'trust_holding_pct']:
                df[col] = df[col].fillna(0)
        else:
            print("No holding data found.")
    except Exception as e:
        print(f"Error fetching holding data: {e}")

    conn.close()
    
    print(f"Read {len(df)} records")
    
    # Force INT columns to numeric then int (handling NaN)
    for col in int_columns:
        if col in df.columns:
            # Fill NaN with 0 or -1? Or keep as None?
            # Supabase BIGINT can be null.
            # But pandas Int64 supports null.
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
    # Convert to records
    # Ensure standard python types
    # df.where(pd.notnull(df), None) doesn't work well with Int64, it converts back to object/float sometimes
    
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                record[col] = None
            elif col in int_columns:
                record[col] = int(val) # Force python int
            else:
                record[col] = val
        records.append(record)
    
    # Batch upload
    batch_size = 50
    total = len(records)
    
    print("Starting upload...")
    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        try:
            # Deep clean to ensure no numpy types remain
            cleaned_batch = []
            for row in batch:
                cleaned_row = {}
                for k, v in row.items():
                    # Handle Infinity
                    if isinstance(v, float) and (v == float('inf') or v == float('-inf')):
                        cleaned_row[k] = None
                        continue
                    cleaned_row[k] = v
                cleaned_batch.append(cleaned_row)
            
            # Debug types to file
            with open('types.txt', 'w', encoding='utf-8') as f:
                for k, v in cleaned_batch[0].items():
                    f.write(f"{k}: {type(v)} = {v}\n")

            # Handle Infinity
            for row in cleaned_batch:
                for k, v in row.items():
                    if isinstance(v, float) and (v == float('inf') or v == float('-inf')):
                        row[k] = None

            response = db_manager.supabase.table('stock_snapshot').upsert(cleaned_batch).execute()
            if i == 0:
                print(f"First batch response data length: {len(response.data) if response.data else 0}")
            print(f"Progress: {min(i+batch_size, total)}/{total}")
            
        except Exception as e:
            print(f"Upload failed (Batch {i}): {e}")
            # Continue to next batch or stop?
            # Let's stop to avoid flooding errors if schema is wrong
            return
            
    print("Sync complete!")

if __name__ == "__main__":
    sync_snapshot_to_cloud()
