from FinMind.data import DataLoader
import pandas as pd

def test_finmind():
    dl = DataLoader()
    print("正在從 FinMind 抓取 1626 集保資料...")
    
    # TaiwanStockHoldingSharesPer: 個股股權分散表
    df = dl.taiwan_stock_holding_shares_per(
        stock_id='1626',
        start_date='2023-12-01',
        end_date='2025-12-19'
    )
    
    if df is not None and not df.empty:
        print(f"成功取得 {len(df)} 筆資料")
        print(df.head())
        print(df.tail())
        
        # 檢查是否有我們需要的欄位
        # 通常需要: date, HoldingSharesLevel, people, percent
        print("\n欄位:", df.columns.tolist())
        
        # 檢查是否有大戶資料 (>1000張, level 15)
        # FinMind 的 level 定義可能不同，需確認
        # 通常 15 代表 >1000張 (或是最大等級)
        print("\n等級分佈:", df['HoldingSharesLevel'].unique())
        
    else:
        print("未取得資料 (可能需要 Token 或資料源問題)")

if __name__ == "__main__":
    test_finmind()
