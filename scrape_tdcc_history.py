"""
TDCC 集保中心 股權分散表 網頁爬蟲
從 TDCC 網站抓取個股的歷史集保資料 (最多一年)

使用方式:
    python scrape_tdcc_history.py --stock 2330 --weeks 52
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

# 資料庫路徑
DB_PATH = Path(__file__).parent / "taiwan_stock.db"

# TDCC 查詢 URL
TDCC_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"

# 忽略 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_friday_dates(weeks=52):
    """取得過去 N 週的週五日期 (民國格式: YYYYMMDD)"""
    dates = []
    today = datetime.now()
    
    for i in range(weeks):
        days_ago = i * 7
        date = today - timedelta(days=days_ago)
        # 調整到週五 (weekday 4)
        days_until_friday = (date.weekday() - 4) % 7
        friday = date - timedelta(days=days_until_friday)
        # 轉民國年格式 (YYYMMDD)
        roc_year = friday.year - 1911
        date_str = f"{roc_year}{friday.strftime('%m%d')}"
        dates.append((date_str, friday.strftime("%Y%m%d")))  # (民國, 西元)
    
    return dates


def scrape_tdcc_stock(stock_id, roc_date):
    """
    從 TDCC 網站抓取單一股票的集保資料
    
    Args:
        stock_id: 股票代碼 (如 2330)
        roc_date: 民國年日期 (如 1131220)
    
    Returns:
        list: 分級持股資料 [(level, holders, shares, proportion), ...]
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": TDCC_URL
    }
    
    # 查詢參數
    params = {
        "SCA_DATE": roc_date,
        "SqlMethod": "StockNo",
        "StockNo": stock_id,
        "StockName": ""
    }
    
    try:
        res = requests.get(TDCC_URL, params=params, headers=headers, 
                          verify=False, timeout=30)
        res.raise_for_status()
        
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 找表格
        table = soup.find("table", {"class": "mt-2"})
        if not table:
            table = soup.find("table")
        
        if not table:
            return []
        
        results = []
        rows = table.find_all("tr")
        
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                try:
                    # 欄位: 持股分級, 人數, 股數, 佔集保庫存數比例
                    level_text = cells[0].get_text(strip=True)
                    holders_text = cells[1].get_text(strip=True).replace(",", "")
                    shares_text = cells[2].get_text(strip=True).replace(",", "")
                    proportion_text = cells[3].get_text(strip=True).replace("%", "")
                    
                    # 解析級距
                    level = parse_level_from_text(level_text)
                    if level == 0:
                        continue
                    
                    holders = int(holders_text) if holders_text.isdigit() else 0
                    shares = int(shares_text) if shares_text.isdigit() else 0
                    proportion = float(proportion_text) if proportion_text else 0.0
                    
                    results.append((level, holders, shares, proportion))
                    
                except (ValueError, IndexError):
                    continue
        
        return results
        
    except Exception as e:
        print(f"爬取 {stock_id} ({roc_date}) 失敗: {e}")
        return []


def parse_level_from_text(text):
    """
    解析持股分級文字為數字
    如 "1-999" -> 1, "1,000,001以上" -> 15
    """
    text = text.strip()
    
    level_patterns = [
        ("1-999", 1),
        ("1,000-5,000", 2),
        ("5,001-10,000", 3),
        ("10,001-15,000", 4),
        ("15,001-20,000", 5),
        ("20,001-30,000", 6),
        ("30,001-40,000", 7),
        ("40,001-50,000", 8),
        ("50,001-100,000", 9),
        ("100,001-200,000", 10),
        ("200,001-400,000", 11),
        ("400,001-600,000", 12),
        ("600,001-800,000", 13),
        ("800,001-1,000,000", 14),
        ("1,000,001以上", 15),
        ("差異數調整", 16),
        ("合計", 17),
    ]
    
    for pattern, level in level_patterns:
        if pattern in text:
            return level
    
    # 數字直接返回
    if text.isdigit():
        return int(text)
    
    return 0


def insert_tdcc_data(conn, stock_id, date_int, records):
    """插入集保資料到資料庫"""
    cursor = conn.cursor()
    count = 0
    
    for level, holders, shares, proportion in records:
        if level == 0 or level > 15:  # 只保存 1-15 級
            continue
        
        cursor.execute("""
            INSERT OR REPLACE INTO stock_shareholding_all 
            (code, date_int, level, holders, shares, proportion)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (stock_id, date_int, level, holders, shares, proportion))
        count += 1
    
    conn.commit()
    return count


def backfill_stock_history(stock_id, weeks=52):
    """回補單一股票的歷史集保資料"""
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
    
    dates = get_friday_dates(weeks)
    total_count = 0
    
    print(f"開始回補 {stock_id} 過去 {weeks} 週的集保資料...")
    
    for i, (roc_date, ad_date) in enumerate(dates):
        date_int = int(ad_date)
        
        # 檢查是否已有資料
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM stock_shareholding_all 
            WHERE code = ? AND date_int = ?
        """, (stock_id, date_int))
        
        if cursor.fetchone()[0] > 0:
            print(f"  跳過 {ad_date} (已有資料)")
            continue
        
        # 爬取資料
        records = scrape_tdcc_stock(stock_id, roc_date)
        
        if records:
            count = insert_tdcc_data(conn, stock_id, date_int, records)
            total_count += count
            print(f"  ✓ {ad_date}: {count} 筆")
        else:
            print(f"  ✗ {ad_date}: 無資料")
        
        # 避免請求過快
        time.sleep(1)
        
        # 進度顯示
        if (i + 1) % 10 == 0:
            print(f"  進度: {i+1}/{len(dates)} ({(i+1)/len(dates)*100:.0f}%)")
    
    conn.close()
    print(f"\n完成! 共寫入 {total_count} 筆資料")
    return total_count


def main():
    parser = argparse.ArgumentParser(description="從 TDCC 網站爬取集保歷史資料")
    parser.add_argument("--stock", type=str, required=True, help="股票代碼 (如 2330)")
    parser.add_argument("--weeks", type=int, default=52, help="回補週數 (預設 52)")
    
    args = parser.parse_args()
    backfill_stock_history(args.stock, args.weeks)


if __name__ == "__main__":
    main()
