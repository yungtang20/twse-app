"""
從 TDCC 開放資料回補過去一年的集保資料
每週抓取一次 CSV，共 52 週

日期規則：
- 集保資料每週五更新，以該週最後一個營業日為準
- TDCC 開放資料保留過去一年的歷史
"""

import requests
import sqlite3
import csv
import io
import time
from datetime import datetime, timedelta
from pathlib import Path

# 資料庫路徑
DB_PATH = Path(__file__).parent / "taiwan_stock.db"

# TDCC 開放資料 API
TDCC_API = "https://smart.tdcc.com.tw/opendata/getOD.ashx"

# 忽略 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_friday_dates(weeks=52):
    """取得過去 N 週的週五日期"""
    dates = []
    today = datetime.now()
    
    for i in range(weeks):
        days_ago = i * 7
        date = today - timedelta(days=days_ago)
        # 調整到週五 (weekday 4)
        days_until_friday = (date.weekday() - 4) % 7
        friday = date - timedelta(days=days_until_friday)
        date_str = friday.strftime("%Y%m%d")
        dates.append(date_str)
    
    return dates


def fetch_tdcc_csv(date_str=None):
    """
    抓取 TDCC 集保資料 (CSV 格式)
    - 無 date 參數: 取得最新當週資料
    - 有 date 參數: 嘗試取得該日期的資料 (不一定成功)
    """
    params = {"id": "1-5"}
    if date_str:
        params["date"] = date_str
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        res = requests.get(TDCC_API, params=params, headers=headers, 
                          timeout=60, verify=False)
        res.raise_for_status()
        
        # 確認是 CSV 格式
        if "text/csv" in res.headers.get("Content-Type", "") or res.text.startswith("資料日期"):
            return res.text
        
        # 可能是 JSON 錯誤訊息
        return res.text
        
    except Exception as e:
        print(f"抓取失敗 ({date_str}): {e}")
        return None


def parse_and_insert_csv(csv_text, conn, target_code=None):
    """
    解析 TDCC CSV 並插入資料庫
    
    CSV 欄位:
    - 資料日期 (ROC 格式如 113/12/20)
    - 證券代號
    - 持股分級 (1-17)
    - 人數
    - 股數
    - 占集保庫存數比例%
    """
    if not csv_text or csv_text.startswith("{"):  # JSON 錯誤
        return 0
    
    count = 0
    cursor = conn.cursor()
    
    # 使用 csv 讀取
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader, None)  # 跳過標題行
    
    for row in reader:
        if len(row) < 6:
            continue
        
        try:
            # 解析欄位
            date_raw = row[0].strip()
            code = row[1].strip()
            level = int(row[2].strip())
            holders = int(row[3].strip().replace(",", "") or 0)
            shares = int(row[4].strip().replace(",", "") or 0)
            proportion = float(row[5].strip() or 0)
            
            # 篩選
            if target_code and code != target_code:
                continue
            
            # 只處理普通股 (4位數字)
            if not code or len(code) != 4 or not code.isdigit():
                continue
            
            # 日期轉換 (ROC -> AD)
            if "/" in date_raw:
                parts = date_raw.split("/")
                year = int(parts[0]) + 1911
                month = parts[1].zfill(2)
                day = parts[2].zfill(2)
                date_int = int(f"{year}{month}{day}")
            else:
                date_int = int(date_raw)
            
            # 插入
            cursor.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all 
                (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (code, date_int, level, holders, shares, proportion))
            count += 1
            
        except Exception as e:
            # 靜默跳過錯誤行
            continue
    
    conn.commit()
    return count


def backfill_tdcc_history(weeks=52, target_code=None):
    """回補過去 N 週的集保資料"""
    conn = sqlite3.connect(str(DB_PATH))
    
    # 確保資料表存在
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_shareholding_all (
            code TEXT NOT NULL,
            date_int INTEGER NOT NULL,
            level INTEGER NOT NULL,
            holders INTEGER DEFAULT 0,
            shares INTEGER DEFAULT 0,
            proportion REAL DEFAULT 0,
            PRIMARY KEY (code, date_int, level)
        )
    """)
    
    # 先抓取最新資料 (不指定日期)
    print("抓取最新當週資料...")
    csv_text = fetch_tdcc_csv()
    if csv_text:
        count = parse_and_insert_csv(csv_text, conn, target_code)
        print(f"  ✓ 寫入 {count} 筆")
    
    # TDCC 開放資料目前只提供當週資料，無法指定歷史日期
    # 需要每週定期抓取來累積歷史
    print(f"\n注意: TDCC 開放資料 API 只提供當週資料")
    print(f"若需歷史資料，請:")
    print(f"  1. 設定每週排程執行此腳本")
    print(f"  2. 使用 FinMind API (需註冊 token)")
    print(f"  3. 從 TDCC 網站手動下載 CSV")
    
    conn.close()
    print("\n完成!")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="從 TDCC 開放資料回補集保資料")
    parser.add_argument("--stock", type=str, help="指定股票代碼")
    parser.add_argument("--weeks", type=int, default=1, help="回補週數 (預設 1)")
    
    args = parser.parse_args()
    backfill_tdcc_history(args.weeks, args.stock)


if __name__ == "__main__":
    main()
