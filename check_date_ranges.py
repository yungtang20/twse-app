import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime
import io

# 完整的 User-Agent 池，模仿真實瀏覽器
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# 股票代碼清單
STOCKS = [
    1626,2405,2838,2908,2941,3051,3289,3297,3646,4107,4108,4175,4416,4417,4503,4523,
    4556,4566,4572,4584,4588,4707,4711,4716,4722,4735,4736,4737,4739,4741,4743,4744,
    4745,4746,4747,4749,4754,4755,4760,4763,4764,4766,4767,4768,4770,4771,4772,4804,
    # ... 完整清單貼在這裡 ...
]

START_DATE = "2023-12-01"
END_DATE = "2025-12-19"

def get_headers():
    """回傳隨機 headers 以避免被偵測"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9",
        "Referer": "https://goodinfo.tw/tw/StockList.asp",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

def fetch_equity_distribution(stock_id, session):
    """抓單一股票的股東持股分級資料"""
    url = f"https://goodinfo.tw/tw/EquityDistributionClassHis.asp"
    
    params = {
        "STOCK_ID": str(stock_id),
        "START_DATE": START_DATE,
        "END_DATE": END_DATE,
    }
    
    try:
        # 隨機延遲 5~10 秒
        sleep_time = random.uniform(5, 10)
        print(f"[{stock_id}] Sleeping {sleep_time:.1f}s before request...")
        time.sleep(sleep_time)
        
        # 發送請求
        headers = get_headers()
        response = session.get(url, params=params, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            print(f"[{stock_id}] HTTP {response.status_code}")
            return None
        
        # 檢查是否被擋（通常會回傳空白或維修頁面）
        if "暫時維修" in response.text or len(response.text) < 1000:
            print(f"[{stock_id}] Possible blocking, skipping...")
            return None
        
        # 解析 HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 找表格
        table = soup.find('table', class_='solid_1_padding_3_4_tbl')
        if table is None:
            print(f"[{stock_id}] No table found")
            return None
        
        # 用 pandas 讀表格
        dfs = pd.read_html(io.StringIO(str(table)))
        if not dfs:
            return None
        
        df = dfs[0]
        
        # 新增股票代碼欄
        df.insert(0, "StockID", stock_id)
        
        print(f"[{stock_id}] ✓ {len(df)} rows extracted")
        return df
        
    except requests.Timeout:
        print(f"[{stock_id}] Timeout, skipping...")
        return None
    except Exception as e:
        print(f"[{stock_id}] Error: {e}")
        return None

def main():
    """主函式：批次下載所有股票"""
    
    # 建立 session 以重用連線
    session = requests.Session()
    
    all_dfs = []
    success_count = 0
    fail_count = 0
    
    print(f"Starting batch download for {len(STOCKS)} stocks...")
    print(f"Date range: {START_DATE} ~ {END_DATE}\n")
    
    for idx, stock_id in enumerate(STOCKS):
        print(f"[{idx+1}/{len(STOCKS)}] Processing {stock_id}...")
        
        df = fetch_equity_distribution(stock_id, session)
        
        if df is not None:
            all_dfs.append(df)
            success_count += 1
        else:
            fail_count += 1
        
        # 每 50 檔休息 30~60 秒，降低被偵測風險
        if (idx + 1) % 50 == 0:
            rest_time = random.uniform(30, 60)
            print(f"\n>>> Batch pause: {rest_time:.0f}s (to avoid blocking)...\n")
            time.sleep(rest_time)
    
    # 整併所有資料
    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)
        output_file = f"goodinfo_equity_distribution_{START_DATE.replace('-','')}_{END_DATE.replace('-','')}.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ Success! Saved to: {output_file}")
        print(f"Total rows: {len(result_df)}, Total stocks: {success_count}/{len(STOCKS)}")
    else:
        print("\n✗ No data collected")
    
    print(f"Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    main()
