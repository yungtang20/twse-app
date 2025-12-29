"""
填補集保/大戶資料空缺 (Forward Fill)

集保資料為週頻率 (通常週五)，但 K 線為日頻率。
此腳本將每週的數據向後填補至每日，直到有新數據為止。
這樣圖表才不會出現斷層或歸零。
"""

import sqlite3
import pandas as pd
from pathlib import Path
import argparse

DB_PATH = Path(__file__).parent / "taiwan_stock.db"

def fill_gaps(stock_id=None):
    conn = sqlite3.connect(str(DB_PATH))
    
    # 取得要處理的股票列表
    if stock_id:
        stocks = [stock_id]
    else:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT code FROM stock_history")
        stocks = [row[0] for row in cursor.fetchall()]
    
    print(f"準備處理 {len(stocks)} 檔股票...")
    
    count = 0
    for code in stocks:
        # 讀取該股票的所有歷史資料 (只取需要的欄位以節省記憶體)
        df = pd.read_sql(f"""
            SELECT date_int, large_shareholder_pct, tdcc_count 
            FROM stock_history 
            WHERE code = '{code}' 
            ORDER BY date_int
        """, conn)
        
        if df.empty:
            continue
            
        # 檢查是否有需要填補的資料
        # 如果 large_shareholder_pct 全是 0 或 NULL，則跳過
        if df['large_shareholder_pct'].max() == 0 or df['large_shareholder_pct'].isnull().all():
            continue

        # 使用 Pandas 的 ffill (Forward Fill)
        # 將 0 視為 NaN 以便填補 (假設 0 是無效值)
        df['large_shareholder_pct'] = df['large_shareholder_pct'].replace(0, pd.NA)
        df['tdcc_count'] = df['tdcc_count'].replace(0, pd.NA)
        
        df['large_shareholder_pct'] = df['large_shareholder_pct'].ffill()
        df['tdcc_count'] = df['tdcc_count'].ffill()
        
        # 將 NaN 轉回 0 或保持 NULL (視資料庫設定而定，這裡轉回 0 避免 SQL 問題)
        df = df.fillna(0)
        
        # 更新回資料庫
        # 為了效能，我們只更新那些原本是 0/NULL 但現在有值的 row
        # 但 Pandas 比較難直接找出這些 row 並批量 update，
        # 我們可以用 executemany
        
        updates = []
        for _, row in df.iterrows():
            updates.append((
                float(row['large_shareholder_pct']), 
                float(row['tdcc_count']), 
                code, 
                int(row['date_int'])
            ))
            
        if updates:
            cursor = conn.cursor()
            cursor.executemany("""
                UPDATE stock_history 
                SET large_shareholder_pct = ?, tdcc_count = ?
                WHERE code = ? AND date_int = ?
            """, updates)
            conn.commit()
            count += 1
            print(f"已填補 {code} ({len(updates)} 筆)")
            
    conn.close()
    print(f"完成! 共處理 {count} 檔股票")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock", type=str, help="指定股票代碼")
    args = parser.parse_args()
    
    fill_gaps(args.stock)
