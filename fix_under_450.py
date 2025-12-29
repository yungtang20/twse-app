"""
使用 FinMind 補齊資料不足 450 筆且有缺漏的股票
"""
import sqlite3
import requests
import pandas as pd
import time
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'

# 需要補齊的股票列表 (從 check_all_under_450.py 獲得)
targets = [
    '7777', '6620', '6910', '7810', '7631', '6955', '6924', '6904'
]

def get_finmind_data(stock_id, start_date):
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": start_date,
        "token": "" # 公開數據不需要 token，但若有可填入
    }
    try:
        r = requests.get(url, params=parameter, timeout=10)
        data = r.json()
        if data['msg'] == 'success' and data['data']:
            return pd.DataFrame(data['data'])
        else:
            print(f"  FinMind 無資料或錯誤: {data.get('msg')}")
            return None
    except Exception as e:
        print(f"  FinMind 請求失敗: {e}")
        return None

def main():
    print("="*60)
    print("開始補齊資料不足的股票 (FinMind)")
    print("="*60)
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    total_updated = 0
    
    for code in targets:
        # 1. 查詢上市日期
        cur.execute("SELECT name, list_date FROM stock_meta WHERE code = ?", (code,))
        res = cur.fetchone()
        if not res:
            print(f"⚠ 找不到 {code} 的 Meta 資料")
            continue
        name, list_date = res
        
        print(f"\n處理 {code} {name} (上市: {list_date})...")
        
        if not list_date:
            print("  無上市日期，跳過")
            continue
            
        # 2. 從 FinMind 抓取資料 (從上市日開始)
        df = get_finmind_data(code, list_date)
        
        if df is not None and not df.empty:
            print(f"  取得 {len(df)} 筆資料")
            
            # 3. 寫入資料庫
            inserted = 0
            for _, row in df.iterrows():
                date_str = row['date'] # YYYY-MM-DD
                date_int = int(date_str.replace('-', ''))
                
                open_p = row['open']
                high_p = row['max']
                low_p = row['min']
                close_p = row['close']
                volume = row['Trading_Volume'] # 股數
                
                # 檢查是否已存在
                cur.execute("SELECT 1 FROM stock_history WHERE code = ? AND date_int = ?", (code, date_int))
                if cur.fetchone():
                    continue
                
                cur.execute("""
                    INSERT INTO stock_history (code, date_int, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (code, date_int, open_p, high_p, low_p, close_p, volume))
                inserted += 1
            
            conn.commit()
            print(f"  ✓ 補入 {inserted} 筆新資料")
            total_updated += inserted
        else:
            print("  無法取得資料")
            
        time.sleep(1) # 避免過於頻繁請求

    print("\n" + "="*60)
    print(f"全部完成！總計補入 {total_updated} 筆資料")
    conn.close()

if __name__ == "__main__":
    main()
