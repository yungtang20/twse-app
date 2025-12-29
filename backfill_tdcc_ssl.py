"""
從集保結算所抓取歷史集保資料 (禁用 SSL 驗證)
"""

import requests
import sqlite3
import time
from datetime import datetime, timedelta
import warnings
import urllib3

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_tdcc_data(date_str):
    """
    從 TDCC 抓取指定日期的集保分佈資料
    date_str: YYYYMMDD 格式
    """
    url = "https://www.tdcc.com.tw/smWeb/QryStockAmt.do"
    
    # 將 YYYYMMDD 轉為 YYYY/MM/DD
    formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml",
    }
    
    data = {
        "scaDates": formatted_date,
        "scaDate": formatted_date,
        "SqlMethod": "StockNo",
        "StockNo": "",  # 空白表示全部股票
        "REession": "none",
        "clession": "none",
    }
    
    try:
        res = requests.post(url, headers=headers, data=data, verify=False, timeout=60)
        res.raise_for_status()
        return res.text
    except Exception as e:
        print(f"Error fetching TDCC data for {date_str}: {e}")
        return None

def parse_tdcc_html(html_content):
    """解析 TDCC 回傳的 HTML"""
    # 這會是複雜的 HTML 解析
    # 簡化起見，改用 CSV 端點
    pass

def fetch_tdcc_csv(date_str):
    """
    從 TDCC 抓取 CSV 格式的資料
    """
    # 嘗試另一個端點
    url = f"https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5&date={date_str}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=60)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"Error fetching TDCC CSV for {date_str}: {e}")
        return None

def backfill_stock(stock_code, conn, weeks=52):
    """
    補齊特定股票的歷史集保資料
    """
    print(f"\n補齊 {stock_code} 的集保資料...")
    
    # 集保資料是每週更新，通常是週五
    today = datetime.now()
    count = 0
    
    for week in range(weeks):
        # 計算過去每週的週五
        days_back = week * 7
        target_date = today - timedelta(days=days_back)
        
        # 找到該週的週五
        days_to_friday = (target_date.weekday() - 4) % 7
        friday = target_date - timedelta(days=days_to_friday)
        date_str = friday.strftime("%Y%m%d")
        
        # 檢查資料庫是否已有該日期的資料
        existing = conn.execute(
            "SELECT COUNT(*) FROM stock_shareholding_all WHERE code = ? AND date_int = ?",
            (stock_code, int(date_str))
        ).fetchone()[0]
        
        if existing > 0:
            print(f"  {date_str}: 已有資料，跳過")
            continue
        
        print(f"  抓取 {date_str}...")
        data = fetch_tdcc_csv(date_str)
        
        if data and isinstance(data, list):
            # 篩選目標股票
            for item in data:
                code = item.get("證券代號", "").strip()
                if code != stock_code:
                    continue
                
                try:
                    # 解析日期 (民國年)
                    raw_date = item.get("資料日期", "")
                    if "/" in raw_date:
                        parts = raw_date.split("/")
                        year = int(parts[0]) + 1911
                        date_int = int(f"{year}{parts[1]}{parts[2]}")
                    else:
                        date_int = int(date_str)
                    
                    level = int(item.get("持股分級", 1))
                    holders = int(item.get("人數", 0))
                    shares = int(item.get("股數", 0))
                    proportion = float(item.get("占集保庫存數比例%", 0))
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_shareholding_all 
                        (code, date_int, level, holders, shares, proportion)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (stock_code, date_int, level, holders, shares, proportion))
                    count += 1
                except Exception as e:
                    print(f"    Error: {e}")
        
        time.sleep(0.5)  # 避免過於頻繁的請求
    
    conn.commit()
    print(f"  完成！共補齊 {count} 筆資料")
    return count

if __name__ == "__main__":
    print("=== 集保資料補齊工具 ===")
    
    # 先測試 API 是否可用
    print("\n測試 TDCC API...")
    test_data = fetch_tdcc_csv("20251219")
    
    if test_data:
        print(f"API 可用，取得 {len(test_data)} 筆資料")
        if len(test_data) > 0:
            print(f"Sample: {list(test_data[0].keys())}")
        
        # 補齊主要股票
        conn = sqlite3.connect("taiwan_stock.db")
        
        # 從資料庫取得有成交的股票清單
        stocks = conn.execute("""
            SELECT DISTINCT code FROM stock_history 
            WHERE code GLOB '[0-9][0-9][0-9][0-9]' 
            ORDER BY code 
            LIMIT 10
        """).fetchall()
        
        for (code,) in stocks[:3]:  # 先測試3檔
            backfill_stock(code, conn, weeks=4)  # 先補4週
        
        conn.close()
        print("\n補齊完成！")
    else:
        print("API 不可用，請檢查網路連線")
