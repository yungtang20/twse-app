"""
從神秘金字塔 (twsthr.info) 抓取集保歷史資料
這個網站已整理 TDCC 的歷史股權分散表資料
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import argparse
from pathlib import Path

# 資料庫路徑
DB_PATH = Path(__file__).parent / "taiwan_stock.db"

# 神秘金字塔網站
PYRAMID_URL = "https://norway.twsthr.info/StockHolders.aspx"


def fetch_pyramid_data(stock_id):
    """
    從神秘金字塔抓取集保資料
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    params = {"stock": stock_id}
    
    try:
        res = requests.get(PYRAMID_URL, params=params, headers=headers, timeout=60)
        res.raise_for_status()
        return res.text
    except Exception as e:
        print(f"抓取失敗: {e}")
        return None


def parse_pyramid_html(html):
    """
    解析神秘金字塔的 HTML，提取集保資料
    
    Returns:
        list: [(date_int, level, holders, shares, proportion), ...]
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    
    # 找所有表格
    tables = soup.find_all("table")
    print(f"找到 {len(tables)} 個表格")
    
    for table in tables:
        rows = table.find_all("tr")
        
        # 檢查表頭是否包含集保相關欄位
        header = rows[0] if rows else None
        if not header:
            continue
        
        header_text = header.get_text()
        if "持股分級" not in header_text and "股東人數" not in header_text:
            continue
        
        print(f"找到集保表格: {len(rows)} 行")
        
        # 解析每一行
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            
            try:
                # 嘗試解析資料
                date_text = cells[0].get_text(strip=True)
                level_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                holders_text = cells[2].get_text(strip=True).replace(",", "") if len(cells) > 2 else "0"
                shares_text = cells[3].get_text(strip=True).replace(",", "") if len(cells) > 3 else "0"
                proportion_text = cells[4].get_text(strip=True).replace("%", "") if len(cells) > 4 else "0"
                
                # 解析日期 (格式可能是 2024-12-20 或 20241220)
                date_int = parse_date(date_text)
                if not date_int:
                    continue
                
                level = parse_level(level_text)
                if level == 0:
                    continue
                
                holders = int(holders_text) if holders_text.isdigit() else 0
                shares = int(shares_text) if shares_text.isdigit() else 0
                proportion = float(proportion_text) if proportion_text else 0.0
                
                results.append((date_int, level, holders, shares, proportion))
                
            except (ValueError, IndexError) as e:
                continue
    
    return results


def parse_date(text):
    """解析日期文字為 YYYYMMDD 格式"""
    # 移除空白
    text = text.strip()
    
    # YYYY-MM-DD 格式
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return int(f"{match.group(1)}{match.group(2)}{match.group(3)}")
    
    # YYYYMMDD 格式
    if len(text) == 8 and text.isdigit():
        return int(text)
    
    # YYYY/MM/DD 格式
    match = re.match(r"(\d{4})/(\d{2})/(\d{2})", text)
    if match:
        return int(f"{match.group(1)}{match.group(2)}{match.group(3)}")
    
    return None


def parse_level(text):
    """解析持股分級"""
    text = text.strip()
    
    level_map = {
        "1-999": 1,
        "1,000-5,000": 2,
        "5,001-10,000": 3,
        "10,001-15,000": 4,
        "15,001-20,000": 5,
        "20,001-30,000": 6,
        "30,001-40,000": 7,
        "40,001-50,000": 8,
        "50,001-100,000": 9,
        "100,001-200,000": 10,
        "200,001-400,000": 11,
        "400,001-600,000": 12,
        "600,001-800,000": 13,
        "800,001-1,000,000": 14,
        "1,000,001以上": 15,
    }
    
    for pattern, level in level_map.items():
        if pattern in text:
            return level
    
    if text.isdigit():
        return int(text)
    
    return 0


def insert_data(conn, stock_id, records):
    """插入資料到資料庫"""
    cursor = conn.cursor()
    count = 0
    
    for date_int, level, holders, shares, proportion in records:
        if level < 1 or level > 15:
            continue
        
        cursor.execute("""
            INSERT OR REPLACE INTO stock_shareholding_all 
            (code, date_int, level, holders, shares, proportion)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (stock_id, date_int, level, holders, shares, proportion))
        count += 1
    
    conn.commit()
    return count


def backfill_from_pyramid(stock_id):
    """從神秘金字塔回補集保資料"""
    print(f"從神秘金字塔抓取 {stock_id} 集保資料...")
    
    html = fetch_pyramid_data(stock_id)
    if not html:
        print("抓取失敗")
        return 0
    
    print(f"HTML 長度: {len(html)} bytes")
    
    records = parse_pyramid_html(html)
    print(f"解析出 {len(records)} 筆資料")
    
    if not records:
        # 試著直接輸出一些 HTML 以便調試
        print("未解析到資料，嘗試尋找其他表格...")
        soup = BeautifulSoup(html, "html.parser")
        
        # 找包含數字的表格
        for i, table in enumerate(soup.find_all("table")):
            rows = table.find_all("tr")
            if len(rows) > 5:
                print(f"\n表格 {i}: {len(rows)} 行")
                for row in rows[:3]:
                    print("  ", row.get_text("|", strip=True)[:100])
        return 0
    
    conn = sqlite3.connect(str(DB_PATH))
    
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
    
    count = insert_data(conn, stock_id, records)
    conn.close()
    
    print(f"完成! 寫入 {count} 筆資料")
    return count


def main():
    parser = argparse.ArgumentParser(description="從神秘金字塔抓取集保歷史資料")
    parser.add_argument("--stock", type=str, required=True, help="股票代碼 (如 2330)")
    
    args = parser.parse_args()
    backfill_from_pyramid(args.stock)


if __name__ == "__main__":
    main()
