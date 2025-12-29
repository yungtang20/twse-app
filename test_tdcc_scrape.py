"""
使用 Playwright 抓取 TDCC 集保歷史資料
測試版 - 先抓一個日期驗證
"""

from playwright.sync_api import sync_playwright
import sqlite3
from pathlib import Path

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


def scrape_tdcc_single(stock_id, date_str, debug_file=None):
    """抓取單一股票單一日期的集保資料"""
    def log(msg):
        if debug_file:
            debug_file.write(msg + "\n")
        print(msg)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        log(f"開啟 TDCC 網站...")
        page.goto("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock")
        page.wait_for_load_state("networkidle")
        
        log(f"選擇日期 {date_str}...")
        page.select_option("select", date_str)
        
        log(f"選擇依代號查詢...")
        page.check('input[name="sqlMethod"][value="StockNo"]')
        
        log(f"輸入股票代碼 {stock_id}...")
        stock_input = page.query_selector('input[name="stockNo"]')
        if stock_input:
            stock_input.fill(stock_id)
        else:
            stock_input = page.query_selector('input[type="text"]')
            if stock_input:
                stock_input.fill(stock_id)
        
        log(f"提交查詢...")
        page.evaluate("document.querySelector('form').submit()")
        
        # 等待結果
        page.wait_for_timeout(3000)
        
        # 找結果表格
        tables = page.query_selector_all("table")
        log(f"找到 {len(tables)} 個表格")
        
        results = []
        for idx, table in enumerate(tables):
            rows = table.query_selector_all("tr")
            log(f"\n表格 {idx}: {len(rows)} 行")
            
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
                            log(f"  ✓ Level {level}: {holders} 人, {proportion}%")
                        except Exception as e:
                            log(f"  解析錯誤: {e}")
        
        browser.close()
        return results


def save_to_db(stock_id, date_str, results):
    """儲存結果到資料庫"""
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
    
    for level, holders, shares, proportion in results:
        conn.execute("""
            INSERT OR REPLACE INTO stock_shareholding_all 
            (code, date_int, level, holders, shares, proportion)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (stock_id, int(date_str), level, holders, shares, proportion))
    
    conn.commit()
    conn.close()


def main():
    stock_id = "2330"
    date_str = "20251212"
    
    print(f"=== 測試抓取 {stock_id} @ {date_str} ===")
    
    with open("tdcc_debug.log", "w", encoding="utf-8") as f:
        results = scrape_tdcc_single(stock_id, date_str, f)
    
    print(f"\n抓取結果: {len(results)} 筆")
    
    if results:
        print("\n資料預覽:")
        for level, holders, shares, proportion in results[:5]:
            print(f"  Level {level}: {holders} 人, {shares} 股, {proportion}%")
        
        save_to_db(stock_id, date_str, results)
        print(f"\n已寫入資料庫!")
    else:
        print("未取得資料，請查看 tdcc_debug.log")


if __name__ == "__main__":
    main()
