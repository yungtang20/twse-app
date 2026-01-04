import numpy as np
from supabase import create_client
import json

sb = create_client('https://bshxromrtsetlfjdeggv.supabase.co', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0')

def calculate_vp(data, bars=20):
    if not data: return None
    
    # Simple VP simulation
    prices = [d['close'] for d in data]
    volumes = [d['volume'] for d in data]
    
    min_p = min(prices)
    max_p = max(prices)
    if min_p == max_p: return {'lower': min_p, 'upper': max_p, 'poc': min_p}
    
    hist, bins = np.histogram(prices, bins=bars, weights=volumes)
    poc_idx = np.argmax(hist)
    poc_price = (bins[poc_idx] + bins[poc_idx+1]) / 2
    
    # Value Area (70%)
    total_vol = sum(volumes)
    target_vol = total_vol * 0.7
    current_vol = hist[poc_idx]
    
    low_idx = poc_idx
    high_idx = poc_idx
    
    while current_vol < target_vol:
        lower_vol = hist[low_idx-1] if low_idx > 0 else 0
        upper_vol = hist[high_idx+1] if high_idx < bars-1 else 0
        
        if lower_vol == 0 and upper_vol == 0: break
        
        if lower_vol > upper_vol:
            low_idx -= 1
            current_vol += lower_vol
        else:
            high_idx += 1
            current_vol += upper_vol
            
    return {
        'lower': bins[low_idx],
        'upper': bins[high_idx+1],
        'poc': poc_price
    }

# Fetch history for 2330
print("Fetching history for 2330...")
res = sb.table('stock_history').select('close,volume').eq('code', '2330').order('date_int', desc=False).limit(60).execute()
data = res.data

if not data:
    print("No data found")
else:
    vp = calculate_vp(data)
    last_close = data[-1]['close']
    
    print(f"Last Close: {last_close}")
    print(f"VP Lower (Support): {vp['lower']:.2f}")
    print(f"VP Upper (Resistance): {vp['upper']:.2f}")
    print(f"VP POC: {vp['poc']:.2f}")
    
    dist_low = (last_close - vp['lower']) / vp['lower']
    dist_high = (vp['upper'] - last_close) / vp['upper']
    
    print(f"Distance from Support: {dist_low*100:.2f}%")
    print(f"Distance from Resistance: {dist_high*100:.2f}%")
    
    if 0 <= dist_low <= 0.02:
        print("MATCHES Support Filter!")
    else:
        print("Does NOT match Support Filter")
