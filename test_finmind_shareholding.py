from FinMind.data import DataLoader
dl = DataLoader()
try:
    df = dl.taiwan_stock_shareholding(stock_id='2330', start_date='2024-01-01')
    if not df.empty:
        print("Found data!")
        print(df.head())
        print(df.columns)
    else:
        print("No data found.")
except Exception as e:
    print(f"Error: {e}")
