"""
TDCC 集保資料完整回補腳本
使用 Playwright 抓取 TDCC 官網的歷史集保資料

可抓取 TDCC 網站上可選的所有歷史日期 (約 51 週)
"""

from playwright.sync_api import sync_playwright
import sqlite3
import time
import argparse
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "taiwan_stock.db"


def parse_level(text):
    """解析持股分級"""
    level_map = {
        "1-999": 1, "1,000-5,000": 2, "5,001-10,000": 3,
        "10,001-15,000": 4, "15,001-20,000": 5, "20,001-30,000": 6,
        "30,001-40,000": 7, "40,001-50,000": 8, "50,001-100,000": 9,
        "100,001-200,000": 10, "200,001-400,000": 11, "400,001-600,000": 12,
        "600,001-800,000": 13, "800,001-1,000,000": 14, "1,000,001以上": 15,
    }
    for pattern, level in level_map.items():
        if pattern in text:
            return level
    return 0


def get_available_dates(page):
    """從 TDCC 網站取得可用的日期列表"""
    select = page.query_selector("select")
    if not select:
        return []
    
    options = select.query_selector_all("option")
    dates = [opt.get_attribute("value") for opt in options if opt.get_attribute("value")]
    return dates


def scrape_stock_date(page, stock_id, date_str):
    """抓取單一股票單一日期的集保資料"""
    try:
        # 使用 URL 參數直接查詢
        url = f"https://www.tdcc.com.tw/portal/zh/smWeb/qryStock?SCA_DATE={date_str}&SqlMethod=StockNo&StockNo={stock_id}&StockName="
        page.goto(url)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        
        # 解析表格
        tables = page.query_selector_all("table")
        results = []
        
        for table in tables:
            rows = table.query_selector_all("tr")
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) >= 4:
                    texts = [c.inner_text().strip() for c in cells]
                    level = parse_level(texts[0])
                    if level > 0:
                        try:
                            holders = int(texts[1].replace(",", "") or "0")
                            shares = int(texts[2].replace(",", "") or "0") 
                            proportion = float(texts[3].replace("%", "") or "0")
                            results.append((level, holders, shares, proportion))
                        except:
                            pass
        
        return results
        
    except Exception as e:
        print(f"  錯誤: {e}")
        return []


def save_to_db(conn, stock_id, date_str, results):
    """儲存結果到資料庫"""
    cursor = conn.cursor()
    count = 0
    
    for level, holders, shares, proportion in results:
        if 1 <= level <= 15:
            cursor.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all 
                (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (stock_id, int(date_str), level, holders, shares, proportion))
            count += 1
    
    conn.commit()
    return count


def check_existing(conn, stock_id, date_str):
    """檢查資料庫是否已有資料"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM stock_shareholding_all 
        WHERE code = ? AND date_int = ?
    """, (stock_id, int(date_str)))
    return cursor.fetchone()[0] > 0


def get_all_stocks(conn):
    """取得所有普通股代碼"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT code FROM stock_meta 
        WHERE code GLOB '[0-9][0-9][0-9][0-9]'
        ORDER BY code
    """)
    return [row[0] for row in cursor.fetchall()]


def backfill_stock(stock_id, weeks=51, skip_existing=True):
    """回補單一股票的歷史集保資料"""
    conn = sqlite3.connect(str(DB_PATH))
    
    # 確保表格存在
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
    
    total_count = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print(f"開啟 TDCC 網站...")
        page.goto("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock")
        page.wait_for_load_state("networkidle")
        
        # 取得可用日期
        dates = get_available_dates(page)
        print(f"可用日期: {len(dates)} 個")
        
        if weeks < len(dates):
            dates = dates[:weeks]
        
        print(f"開始回補 {stock_id} 過去 {len(dates)} 週的集保資料...\n")
        
        for i, date_str in enumerate(dates):
            # 檢查是否已存在
            if skip_existing and check_existing(conn, stock_id, date_str):
                print(f"  [{i+1}/{len(dates)}] {date_str}: 跳過 (已存在)")
                continue
            
            # 需要重新載入頁面
            if i > 0:
                page.goto("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock")
                page.wait_for_load_state("networkidle")
            
            results = scrape_stock_date(page, stock_id, date_str)
            
            if results:
                count = save_to_db(conn, stock_id, date_str, results)
                total_count += count
                print(f"  [{i+1}/{len(dates)}] {date_str}: ✓ {count} 筆")
            else:
                print(f"  [{i+1}/{len(dates)}] {date_str}: ✗ 無資料")
            
            # 避免請求過快
            time.sleep(1)
        
        browser.close()
    
    conn.close()
    print(f"\n完成! {stock_id} 共寫入 {total_count} 筆資料")
    return total_count


def backfill_all_stocks(weeks=51, limit=None):
    """回補所有股票的歷史集保資料"""
    conn = sqlite3.connect(str(DB_PATH))
    stocks = get_all_stocks(conn)
    conn.close()
    
    if limit:
        stocks = stocks[:limit]
    
    print(f"共有 {len(stocks)} 檔股票需要回補\n")
    
    total = 0
    for i, stock_id in enumerate(stocks):
        print(f"\n=== [{i+1}/{len(stocks)}] {stock_id} ===")
        count = backfill_stock(stock_id, weeks)
        total += count
        
        # 每 10 檔顯示進度
        if (i + 1) % 10 == 0:
            print(f"\n>>> 進度: {i+1}/{len(stocks)} ({(i+1)/len(stocks)*100:.1f}%)")
    
    print(f"\n\n=== 全部完成! 共寫入 {total} 筆資料 ===")


def main():
    parser = argparse.ArgumentParser(description="TDCC 集保資料回補")
    parser.add_argument("--stock", type=str, help="指定股票代碼 (如 2330)")
    parser.add_argument("--all", action="store_true", help="回補所有股票")
    parser.add_argument("--weeks", type=int, default=51, help="回補週數 (預設 51)")
    parser.add_argument("--limit", type=int, help="限制股票數量 (測試用)")
    parser.add_argument("--force", action="store_true", help="強制重新抓取 (不跳過已存在)")
    
    args = parser.parse_args()
    
    if args.stock:
        backfill_stock(args.stock, args.weeks, not args.force)
    elif args.all:
        backfill_all_stocks(args.weeks, args.limit)
    else:
        # 預設回補熱門股票
        print("預設回補熱門股票 (2330, 2317, 2454, 2308, 3711)")
        for stock in ["2330", "2317", "2454", "2308", "3711"]:
            backfill_stock(stock, args.weeks, not args.force)


if __name__ == "__main__":
    main()
