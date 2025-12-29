"""
使用 Playwright 自動化抓取 TDCC 集保歷史資料
可繞過 JavaScript 動態載入限制

安裝: pip install playwright && playwright install chromium

使用方式:
    python scrape_tdcc_playwright.py --stock 2330 --weeks 52
"""

import asyncio
import sqlite3
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("請先安裝 playwright: pip install playwright && playwright install chromium")
    exit(1)

# 資料庫路徑
DB_PATH = Path(__file__).parent / "taiwan_stock.db"

# TDCC 查詢 URL
TDCC_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"


def get_friday_dates(weeks=52):
    """取得過去 N 週的週五日期"""
    dates = []
    today = datetime.now()
    
    for i in range(weeks):
        days_ago = i * 7
        date = today - timedelta(days=days_ago)
        days_until_friday = (date.weekday() - 4) % 7
        friday = date - timedelta(days=days_until_friday)
        date_str = friday.strftime("%Y%m%d")
        dates.append(date_str)
    
    return dates


async def scrape_tdcc_stock(page, stock_id, date_str):
    """
    使用 Playwright 抓取單一股票的集保資料
    """
    try:
        # 等待頁面載入
        await page.goto(TDCC_URL, wait_until="networkidle")
        
        # 選擇日期
        await page.select_option('select[name="scaDate"]', date_str)
        
        # 選擇「依代號查詢」
        await page.click('input[value="StockNo"]')
        
        # 輸入股票代碼
        await page.fill('input[name="stockNo"]', stock_id)
        
        # 點擊查詢按鈕
        await page.click('input[type="submit"][value="查詢"]')
        
        # 等待結果載入
        await page.wait_for_selector('table.mt-2', timeout=10000)
        
        # 取得表格資料
        rows = await page.query_selector_all('table.mt-2 tr')
        
        results = []
        for row in rows:
            cells = await row.query_selector_all('td')
            if len(cells) >= 4:
                level_text = await cells[0].inner_text()
                holders_text = await cells[1].inner_text()
                shares_text = await cells[2].inner_text()
                proportion_text = await cells[3].inner_text()
                
                level = parse_level(level_text.strip())
                if level == 0:
                    continue
                
                holders = int(holders_text.strip().replace(",", "") or "0")
                shares = int(shares_text.strip().replace(",", "") or "0")
                proportion = float(proportion_text.strip().replace("%", "") or "0")
                
                results.append((level, holders, shares, proportion))
        
        return results
        
    except Exception as e:
        print(f"  抓取失敗: {e}")
        return []


def parse_level(text):
    """解析持股分級"""
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
    
    return 0


def insert_data(conn, stock_id, date_int, records):
    """插入資料"""
    cursor = conn.cursor()
    count = 0
    
    for level, holders, shares, proportion in records:
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


async def backfill_stock_history(stock_id, weeks=52):
    """回補股票歷史集保資料"""
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
    
    dates = get_friday_dates(weeks)
    total_count = 0
    
    print(f"開始回補 {stock_id} 過去 {weeks} 週的集保資料...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        for i, date_str in enumerate(dates):
            date_int = int(date_str)
            
            # 檢查是否已有資料
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM stock_shareholding_all 
                WHERE code = ? AND date_int = ?
            """, (stock_id, date_int))
            
            if cursor.fetchone()[0] > 0:
                print(f"  跳過 {date_str} (已有資料)")
                continue
            
            print(f"  抓取 {date_str}...", end=" ")
            
            records = await scrape_tdcc_stock(page, stock_id, date_str)
            
            if records:
                count = insert_data(conn, stock_id, date_int, records)
                total_count += count
                print(f"✓ {count} 筆")
            else:
                print("✗ 無資料")
            
            # 進度顯示
            if (i + 1) % 10 == 0:
                print(f"  進度: {i+1}/{len(dates)}")
            
            # 延遲避免請求過快
            await asyncio.sleep(2)
        
        await browser.close()
    
    conn.close()
    print(f"\n完成! 共寫入 {total_count} 筆資料")
    return total_count


def main():
    parser = argparse.ArgumentParser(description="使用 Playwright 抓取 TDCC 集保歷史")
    parser.add_argument("--stock", type=str, required=True, help="股票代碼")
    parser.add_argument("--weeks", type=int, default=52, help="回補週數")
    
    args = parser.parse_args()
    asyncio.run(backfill_stock_history(args.stock, args.weeks))


if __name__ == "__main__":
    main()
