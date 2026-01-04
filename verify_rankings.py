#!/usr/bin/env python3
"""Verify Supabase data merge for Rankings page"""
from supabase import create_client
import json

sb = create_client('https://bshxromrtsetlfjdeggv.supabase.co', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0')

print("="*60)
print("Verifying Rankings data merge")
print("="*60)

# Step 1: Get latest date
res = sb.table('institutional_investors').select('date_int').order('date_int', desc=True).limit(1).execute()
latest_date = res.data[0]['date_int'] if res.data else None
print(f'Latest date: {latest_date}')

# Step 2: Get institutional data (top 10 by foreign_net)
res = sb.table('institutional_investors').select('code,foreign_net,trust_net').eq('date_int', latest_date).order('foreign_net', desc=True).limit(10).execute()
inst_data = res.data
print(f'Institutional data: {len(inst_data)} rows')

# Step 3: Get codes and fetch stock_snapshot
codes = [d['code'] for d in inst_data]
res = sb.table('stock_snapshot').select('code,name,close,volume,foreign_streak,trust_streak').in_('code', codes).execute()
snap_data = res.data
print(f'Snapshot data: {len(snap_data)} rows')

# Step 4: Merge and display
print("\n" + "="*60)
print("Merged Results:")
print("="*60)
print(f"{'Code':<8} {'Name':<12} {'Close':>10} {'Volume':>12} {'Foreign Net':>12} {'Trust Net':>10}")
print("-"*70)

snap_map = {s['code']: s for s in snap_data}
for d in inst_data:
    snap = snap_map.get(d['code'], {})
    name = snap.get('name', '-')[:10] if snap.get('name') else '-'
    close = snap.get('close', 0) or 0
    volume = snap.get('volume', 0) or 0
    fn = d.get('foreign_net', 0) or 0
    tn = d.get('trust_net', 0) or 0
    print(f"{d['code']:<8} {name:<12} {close:>10.2f} {volume:>12,} {fn:>12,} {tn:>10,}")

print("\n" + "="*60)
print("Verification complete!")
print("="*60)
