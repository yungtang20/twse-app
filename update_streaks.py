
import os
import re
from supabase import create_client
import json
from collections import defaultdict

# Supabase configuration
SUPABASE_URL = "https://bshxromrtsetlfjdeggv.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def is_common_stock(code):
    """
    Check if the stock code follows 'A Rule':
    - 4 digits only (Common Stock)
    - Exclude ETFs (00xx), Warrants (6 digits), DR, etc.
    - But wait, 0050 is an ETF but often tracked.
    - User rule: "僅普通股：TWSE+TPEX+KY，排除ETF/權證/DR/ETN/債券/指數/創新板/特別股/非數字代碼"
    - "僅顯示四個字" -> implies 4-digit codes.
    - Common stocks usually start with 1-9. 00xx are ETFs.
    """
    if not code.isdigit() or len(code) != 4:
        return False
    # Exclude ETFs (start with 00)
    if code.startswith('00'):
        return False
    return True

def calculate_streaks():
    print("Fetching institutional investors data...")
    
    # Get all data ordered by date desc
    # We need enough history to calculate streaks. Let's fetch last 30 days for all stocks.
    # To do this efficiently, we might need to fetch by stock or just fetch a large chunk.
    # Given 50,000 records limit in previous upload, we likely have ~20-30 days of data for ~2000 stocks.
    
    res = supabase.table('institutional_investors').select('code, date_int, foreign_net, trust_net').order('date_int', desc=True).limit(50000).execute()
    data = res.data
    
    if not data:
        print("No data found.")
        return

    print(f"Fetched {len(data)} records.")
    
    # Group by code
    stock_data = defaultdict(list)
    for row in data:
        stock_data[row['code']].append(row)
    
    updates = []
    
    print("Calculating streaks...")
    for code, rows in stock_data.items():
        # Apply A Rule filtering
        if not is_common_stock(code):
            continue
            
        # Sort by date desc (already sorted but ensure)
        rows.sort(key=lambda x: x['date_int'], reverse=True)
        
        # Calculate Foreign Streak
        f_streak = 0
        if rows:
            first_val = rows[0].get('foreign_net', 0) or 0
            if first_val > 0:
                for row in rows:
                    val = row.get('foreign_net', 0) or 0
                    if val > 0: f_streak += 1
                    else: break
            elif first_val < 0:
                for row in rows:
                    val = row.get('foreign_net', 0) or 0
                    if val < 0: f_streak -= 1
                    else: break
        
        # Calculate Trust Streak
        t_streak = 0
        if rows:
            first_val = rows[0].get('trust_net', 0) or 0
            if first_val > 0:
                for row in rows:
                    val = row.get('trust_net', 0) or 0
                    if val > 0: t_streak += 1
                    else: break
            elif first_val < 0:
                for row in rows:
                    val = row.get('trust_net', 0) or 0
                    if val < 0: t_streak -= 1
                    else: break
        
        updates.append({
            'code': code,
            'foreign_streak': f_streak,
            'trust_streak': t_streak
        })
    
    print(f"Prepared updates for {len(updates)} stocks (A Rule filtered).")
    
    # Batch update stock_snapshot
    batch_size = 500
    total_updated = 0
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        print(f"Updating batch {i//batch_size + 1}...")
        try:
            # Upsert into stock_snapshot
            # Note: stock_snapshot primary key is code. Upsert will update existing records.
            # We only want to update streaks, but upsert requires all non-nullable fields or it might fail if row doesn't exist?
            # Actually, stock_snapshot rows should already exist. We can use upsert to update specific columns if the row exists.
            # But supabase-py upsert might replace the row if we don't provide all columns?
            # No, upsert usually merges if we provide the PK. But let's be careful.
            # Ideally we should use 'update' but that's one by one or requires a WHERE IN.
            # Upsert is best for batch. We just need to make sure we don't wipe other columns.
            # To be safe, we should probably fetch the existing rows first or trust that upsert merges?
            # Supabase upsert: "Perform an UPSERT on the table."
            # If we only provide code and streaks, other columns might be set to default/null if it's treated as a new row insertion for missing ones?
            # But these rows exist.
            # Let's try to fetch existing snapshot data for this batch to merge, to be safe.
            
            codes = [u['code'] for u in batch]
            snap_res = supabase.table('stock_snapshot').select('*').in_('code', codes).execute()
            existing_map = {r['code']: r for r in snap_res.data}
            
            final_batch = []
            for u in batch:
                if u['code'] in existing_map:
                    # Merge with existing
                    record = existing_map[u['code']]
                    record['foreign_streak'] = u['foreign_streak']
                    record['trust_streak'] = u['trust_streak']
                    final_batch.append(record)
                # If not in snapshot, we skip it (it might be a stock not in our snapshot list)
            
            if final_batch:
                supabase.table('stock_snapshot').upsert(final_batch).execute()
                total_updated += len(final_batch)
                
        except Exception as e:
            print(f"Error updating batch: {e}")
            
    print(f"Successfully updated streaks for {total_updated} stocks.")

if __name__ == "__main__":
    calculate_streaks()
