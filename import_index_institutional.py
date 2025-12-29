import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from FinMind.data import DataLoader

# Add parent directory to path to import backend services if needed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.services.db import db_manager

def fetch_index_institutional(days=730):
    """下載加權指數法人買賣超 (FinMind)"""
    print(f"\n【下載加權指數法人買賣超 (FinMind)】")
    
    try:
        dl = DataLoader()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        print(f"  正在抓取資料 ({start_date} ~ {end_date})...")
        df = dl.taiwan_stock_institutional_investors_total(start_date=start_date, end_date=end_date)
        
        if df.empty:
            print("  ⚠ 查無資料")
            return
            
        print(f"  抓到 {len(df)} 筆原始資料")
        
        # 計算買賣超 (Net Buy = Buy - Sell)
        df['net_buy'] = df['buy'] - df['sell']
        
        # 轉換日期格式 YYYY-MM-DD -> YYYYMMDD
        df['date_int'] = df['date'].apply(lambda x: int(x.replace('-', '')))
        
        # Pivot table
        pivot = df.pivot_table(index='date_int', columns='name', values='net_buy', aggfunc='sum').fillna(0)
        
        # 合併自營商
        if 'Dealer_Self' in pivot.columns and 'Dealer_Hedging' in pivot.columns:
            pivot['Dealer'] = pivot['Dealer_Self'] + pivot['Dealer_Hedging']
        elif 'Dealer_Self' in pivot.columns:
            pivot['Dealer'] = pivot['Dealer_Self']
        elif 'Dealer_Hedging' in pivot.columns:
            pivot['Dealer'] = pivot['Dealer_Hedging']
        else:
            pivot['Dealer'] = 0
            
        # 寫入資料庫
        with db_manager.get_connection() as conn:
            updated = 0
            for date_int, row in pivot.iterrows():
                foreign = float(row.get('Foreign_Investor', 0))
                trust = float(row.get('Investment_Trust', 0))
                dealer = float(row.get('Dealer', 0))
                
                conn.execute("""
                    UPDATE stock_history 
                    SET foreign_buy = ?, trust_buy = ?, dealer_buy = ?
                    WHERE code = '0000' AND date_int = ?
                """, (foreign, trust, dealer, date_int))
                
                if conn.total_changes > 0:
                    updated += 1
            conn.commit()
            print(f"  ✓ 成功更新 {updated} 筆加權指數法人資料")
            
    except Exception as e:
        print(f"  ❌ 下載失敗: {e}")

if __name__ == "__main__":
    fetch_index_institutional()
