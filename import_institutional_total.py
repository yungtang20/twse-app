"""
從 FinMind 抓取三大法人買賣金額統計表 (大盤) 並存入資料庫
"""
from FinMind.data import DataLoader
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

print("正在從 FinMind 抓取三大法人買賣金額統計表...")

dl = DataLoader()

# 設定日期範圍 (最近2年)
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')

try:
    df = dl.taiwan_stock_institutional_investors_total(start_date=start_date, end_date=end_date)
    print(f"抓到 {len(df)} 筆資料")
    
    # 計算買賣超 (Net Buy = Buy - Sell)
    df['net_buy'] = df['buy'] - df['sell']
    
    # 轉換日期格式 YYYY-MM-DD -> YYYYMMDD
    df['date_int'] = df['date'].apply(lambda x: int(x.replace('-', '')))
    
    # Pivot table to get columns for each investor type
    # Values will be net_buy
    pivot = df.pivot_table(index='date_int', columns='name', values='net_buy', aggfunc='sum').fillna(0)
    
    # 合併自營商 (Dealer_Self + Dealer_Hedging)
    if 'Dealer_Self' in pivot.columns and 'Dealer_Hedging' in pivot.columns:
        pivot['Dealer'] = pivot['Dealer_Self'] + pivot['Dealer_Hedging']
    elif 'Dealer_Self' in pivot.columns:
        pivot['Dealer'] = pivot['Dealer_Self']
    elif 'Dealer_Hedging' in pivot.columns:
        pivot['Dealer'] = pivot['Dealer_Hedging']
    else:
        pivot['Dealer'] = 0
        
    # 準備寫入資料庫
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    updated = 0
    for date_int, row in pivot.iterrows():
        foreign = row.get('Foreign_Investor', 0)
        trust = row.get('Investment_Trust', 0)
        dealer = row.get('Dealer', 0)
        
        # 更新 stock_history
        cur.execute("""
            UPDATE stock_history 
            SET foreign_buy = ?, trust_buy = ?, dealer_buy = ?
            WHERE code = '0000' AND date_int = ?
        """, (foreign, trust, dealer, date_int))
        
        if cur.rowcount > 0:
            updated += 1
            
    conn.commit()
    conn.close()
    print(f"成功更新 {updated} 筆加權指數法人資料")
    
except Exception as e:
    print(f"錯誤: {e}")
    import traceback
    traceback.print_exc()
