"""
從 FinMind 抓取法人買賣超資料存入資料庫
"""
from FinMind.data import DataLoader
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

print("正在從 FinMind 抓取法人買賣超資料...")

dl = DataLoader()

# 設定日期範圍 (最近2年)
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')

# 抓取加權指數法人買賣超
print(f"抓取加權指數法人資料 ({start_date} ~ {end_date})...")
try:
    # 加權指數用 TAIEX
    df = dl.taiwan_stock_institutional_investors(stock_id='TAIEX', start_date=start_date, end_date=end_date)
    print(f"Columns: {df.columns.tolist()}")
    print(f"抓到 {len(df)} 筆資料")
    print(df.head(10))
    
    # 按日期 pivot
    pivot = df.pivot_table(index='date', columns='name', values='buy', aggfunc='sum').fillna(0)
    print("\nPivot sample:")
    print(pivot.head())
except Exception as e:
    print(f"錯誤: {e}")
