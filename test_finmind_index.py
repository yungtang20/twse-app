from FinMind.data import DataLoader
import pandas as pd

dl = DataLoader()
df = dl.taiwan_stock_index_daily(stock_id='TAIEX', start_date='2025-12-01')
print(df.head())
print(df.columns)
