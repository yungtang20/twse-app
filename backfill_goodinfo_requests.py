import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import time
import random
from pathlib import Path

# 資料庫路徑
DB_PATH = Path(__file__).parent / "taiwan_stock.db"

# 讀取缺資料股票清單
try:
    with open("missing_stocks.txt", "r") as f:
        stocks = f.read().strip().split(",")
        stocks = [s for s in stocks if s] # 過濾空字串
except FileNotFoundError:
    print("找不到 missing_stocks.txt，請先執行 generate_missing_list.py")
    exit()

print(f"準備回補 {len(stocks)} 檔股票...")

# 設定日期範圍 (2年)
start_date = datetime.strptime("20231201", "%Y%m%d")
end_date   = datetime.strptime("20251219", "%Y%m%d")

base_url = "https://goodinfo.tw/tw/EquityDistributionClassHis.asp"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://goodinfo.tw/tw/StockList.asp"
}

def save_to_db(stock_id, df):
    """將 DataFrame 寫入資料庫"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    count = 0
    for _, row in df.iterrows():
        try:
            date_str = row['date_str'] # 格式: YYYY-MM-DD
            date_int = int(date_str.replace("-", ""))
            
            # 處理數值 (去除逗號)
            def clean_num(val):
                if pd.isna(val) or val == '-': return 0
                if isinstance(val, str): return float(val.replace(',', ''))
                return float(val)

            # 欄位映射 (依據 Goodinfo 預設表格: 持股分級)
            # 假設欄位: 週別, 統計日期, 收盤價, ..., >1千張(比例)
            # 注意: requests 抓到的預設表格通常是 "持股比例"
            
            # 尋找 ">1千張" 或類似欄位
            # Goodinfo 表頭可能複雜，需依 index 或名稱
            # 這裡假設最後一欄是 >1000張比例 (需驗證)
            # 根據之前的經驗，比例表最後一欄是 >1000張
            
            pct_large = clean_num(row.iloc[-1]) # 最後一欄
            
            # 寫入 stock_shareholding_all (Level 15)
            cursor.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all
                (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, 15, 
                    COALESCE((SELECT holders FROM stock_shareholding_all WHERE code=? AND date_int=? AND level=15), 0),
                    0, 
                    ?)
            """, (stock_id, date_int, stock_id, date_int, pct_large))
            
            # 更新 stock_history
            cursor.execute("""
                UPDATE stock_history 
                SET large_shareholder_pct = ?
                WHERE code = ? AND date_int = ?
            """, (pct_large, stock_id, date_int))
            
            count += 1
        except Exception as e:
            print(f"  寫入錯誤 ({date_int}): {e}")
            continue
            
    conn.commit()
    conn.close()
    return count

def fetch_one_stock(stock_id):
    print(f"正在抓取 {stock_id}...", end="", flush=True)
    params = {"STOCK_ID": str(stock_id)}
    
    try:
        r = requests.get(base_url, params=params, headers=headers, timeout=15)
        r.encoding = "utf-8"
        
        if r.status_code != 200:
            print(f" HTTP {r.status_code} 失敗")
            return None
            
        soup = BeautifulSoup(r.text, "lxml")

        # 找到歷史資料表格 (id="tblDetail")
        table = soup.find("table", {"id": "tblDetail"})
        if table is None:
            print(" 找不到表格")
            return None

        # 使用 pandas 解析
        # header=1 表示第二列是標題 (第一列是合併儲存格)
        dfs = pd.read_html(str(table), header=1)
        if not dfs:
            print(" 解析失敗")
            return None
            
        df = dfs[0]
        
        # 清理資料
        # 欄位 0: 週別, 1: 統計日期
        # 移除無效列
        df = df[df.iloc[:, 0].astype(str).str.contains("W", na=False)].copy()
        
        # 處理日期
        # 格式: 23W51 12/22 -> 需轉換
        # 簡單起見，直接用第二欄 (統計日期)
        # 但 Goodinfo 只有 月/日，需配合週別年份
        
        def parse_date(row):
            try:
                week_str = str(row.iloc[0]) # e.g. 23W51
                date_part = str(row.iloc[1]) # e.g. 12/22
                
                year_short = int(week_str[:2]) # 23
                year = 2000 + year_short
                
                return f"{year}-{date_part.replace('/', '-')}"
            except:
                return None

        df['date_str'] = df.apply(parse_date, axis=1)
        df = df[df['date_str'].notna()]
        
        # 轉換為 datetime 物件以進行篩選
        df['dt'] = pd.to_datetime(df['date_str'])
        
        # 篩選日期
        mask = (df['dt'] >= start_date) & (df['dt'] <= end_date)
        df = df.loc[mask].copy()
        
        print(f" 取得 {len(df)} 筆")
        return df

    except Exception as e:
        print(f" 錯誤: {e}")
        return None

# 主迴圈
total_processed = 0
for i, sid in enumerate(stocks):
    df = fetch_one_stock(sid)
    if df is not None and not df.empty:
        count = save_to_db(sid, df)
        total_processed += 1
    
    # 避免被擋，每 5 檔休息一下
    if (i + 1) % 5 == 0:
        time.sleep(random.uniform(2, 5))
    else:
        time.sleep(random.uniform(0.5, 1.5))

print(f"\n全部完成！共處理 {total_processed} 檔股票")
