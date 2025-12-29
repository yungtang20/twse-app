"""
從 Goodinfo 下載並匯入集保資料

支援兩種方式:
1. 自動下載 HTML 並解析
2. 解析本地已下載的 HTML/XLS 檔案

Goodinfo URL 格式:
- HTML: https://goodinfo.tw/tw/EquityDistributionClassHis.asp?STOCK_ID=2330
- 匯出時會自動下載對應格式

使用方式:
    python scrape_goodinfo_tdcc.py --stock 2330           # 自動下載
    python scrape_goodinfo_tdcc.py --file xxx.html        # 使用本地檔案
    python scrape_goodinfo_tdcc.py --all --limit 10       # 批次下載多檔
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
import re
import time
import argparse
from pathlib import Path

DB_PATH = Path(__file__).parent / "taiwan_stock.db"

# Goodinfo URL
GOODINFO_URL = "https://goodinfo.tw/tw/EquityDistributionClassHis.asp"


def download_goodinfo_html(stock_id, mode='pct'):
    """
    使用 Playwright 從 Goodinfo 下載集保 HTML
    mode: 'pct' (持有比例, 預設), 'holders' (股東人數)
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = f"{GOODINFO_URL}?STOCK_ID={stock_id}"
        print(f"開啟 {url} ({mode})...")
        
        try:
            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle", timeout=60000)
            
            # 點擊 "查5年"
            try:
                page.click('input[value="查5年"]', timeout=5000)
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_timeout(2000)
            except:
                pass # 可能已是完整資料
            
            # 如果是抓取股東人數，切換下拉選單
            if mode == 'holders':
                print("切換到 '持有人數區間分級一覽(簡化)'...")
                # ID 為 selSheet
                # 嘗試用 index 選擇 (19: 持有人數區間分級一覽(簡化))
                try:
                    page.select_option('select#selSheet', index=19)
                except:
                    # 如果 index 失敗，嘗試用 Label
                    print("Index 選擇失敗，嘗試用 Label...")
                    page.select_option('select#selSheet', label='持有人數區間分級一覽(簡化)')
                    
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_timeout(2000)
            
            html = page.content()
            browser.close()
            return html
            
        except Exception as e:
            print(f"下載失敗: {e}")
            browser.close()
            return None


def parse_goodinfo_html(html, stock_id, year=2025, min_date=20231201, mode='pct'):
    """
    解析 Goodinfo 集保 HTML
    mode: 'pct' (持有比例), 'holders' (股東人數)
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # 找 tblDetail 表格
    table = soup.find("table", {"id": "tblDetail"})
    if not table:
        print("找不到 tblDetail 表格")
        return []
    
    results = []
    rows = table.find_all("tr")
    
    # 跳過表頭 (通常前2行)
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) < 14:
            continue
        
        try:
            # 週別
            week_str = cells[0].get_text(strip=True)
            
            # 統計日期
            date_str = cells[1].get_text(strip=True)
            
            # 解析年份
            week_match = re.match(r"(\d{2})W(\d+)", week_str)
            if week_match:
                yr = int(week_match.group(1)) + 2000
            else:
                yr = year
            
            # 轉換日期
            if "/" in date_str:
                month, day = date_str.split("/")
                date_int = int(f"{yr}{month.zfill(2)}{day.zfill(2)}")
            else:
                continue
            
            # 過濾日期 (只保留 2 年內)
            if date_int < min_date:
                continue
            
            if mode == 'pct':
                # 各分級持股比例
                pct_10 = float(cells[6].get_text(strip=True).replace(',','') or 0)
                pct_50 = float(cells[7].get_text(strip=True).replace(',','') or 0)
                pct_100 = float(cells[8].get_text(strip=True).replace(',','') or 0)
                pct_200 = float(cells[9].get_text(strip=True).replace(',','') or 0)
                pct_400 = float(cells[10].get_text(strip=True).replace(',','') or 0)
                pct_800 = float(cells[11].get_text(strip=True).replace(',','') or 0)
                pct_1000 = float(cells[12].get_text(strip=True).replace(',','') or 0)
                pct_large = float(cells[13].get_text(strip=True).replace(',','') or 0)
                
                results.append({
                    "date_int": date_int,
                    "pct_large": pct_large # >1千張 (大戶)
                })
                
            elif mode == 'holders':
                # 總股東人數 (第6欄, index 5)
                # 欄位: 週別, 日期, 收盤, 漲跌, 漲跌%, 股東人數, <=10...
                total_holders = int(cells[5].get_text(strip=True).replace(',','') or 0)
                
                # 大戶人數 (>1千張)
                holders_large = int(cells[13].get_text(strip=True).replace(',','') or 0)
                
                results.append({
                    "date_int": date_int,
                    "total_holders": total_holders,
                    "holders_large": holders_large
                })
            
        except Exception as e:
            continue
    
    return results


def save_to_db(stock_id, data, mode='pct'):
    """儲存到資料庫"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # 確保表格存在
    cursor.execute("""
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
    
    count = 0
    for item in data:
        date_int = item["date_int"]
        
        if mode == 'pct':
            # 更新大戶比例
            # level 15 = 大戶 (>1000張)
            cursor.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all
                (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, 15, 
                    COALESCE((SELECT holders FROM stock_shareholding_all WHERE code=? AND date_int=? AND level=15), 0),
                    0, 
                    ?)
            """, (stock_id, date_int, stock_id, date_int, item["pct_large"]))
            
            # 更新 stock_history
            cursor.execute("""
                UPDATE stock_history 
                SET large_shareholder_pct = ?
                WHERE code = ? AND date_int = ?
            """, (item["pct_large"], stock_id, date_int))
            
        elif mode == 'holders':
            # 更新總人數 (stock_history)
            cursor.execute("""
                UPDATE stock_history 
                SET tdcc_count = ?
                WHERE code = ? AND date_int = ?
            """, (item["total_holders"], stock_id, date_int))
            
            # 更新大戶人數 (stock_shareholding_all)
            cursor.execute("""
                UPDATE stock_shareholding_all
                SET holders = ?
                WHERE code = ? AND date_int = ? AND level = 15
            """, (item["holders_large"], stock_id, date_int))
            
            # 存入總人數到 level 0 (方便查詢)
            cursor.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all
                (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, 0, ?, 0, 100)
            """, (stock_id, date_int, item["total_holders"]))
        
        count += 1
    
    conn.commit()
    conn.close()
    return count


def get_all_stocks(limit=None):
    """
    取得符合 A 規則的普通股代碼
    A規則: TWSE+TPEX+KY，排除ETF/權證/DR/ETN/債券/指數/創新板/特別股/非數字代碼
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # 初步篩選：只取 4 位數字代碼
    cursor.execute("""
        SELECT DISTINCT code, name FROM stock_meta 
        WHERE code GLOB '[0-9][0-9][0-9][0-9]'
        ORDER BY code
    """)
    rows = cursor.fetchall()
    conn.close()
    
    valid_stocks = []
    for code, name in rows:
        # 排除 ETF (00開頭)
        if code.startswith('00'):
            continue
            
        # 排除 DR (91開頭)
        if code.startswith('91'):
            continue
            
        # 排除特別股 (名稱含 "特")
        if "特" in name:
            continue
            
        valid_stocks.append(code)
    
    print(f"符合 A 規則股票: {len(valid_stocks)} 檔 (已排除 ETF/DR/特別股)")
    
    if limit:
        return valid_stocks[:limit]
    return valid_stocks


def scrape_stock(stock_id):
    """抓取並匯入單一股票的集保資料 (比例 + 人數)"""
    print(f"\n=== {stock_id} ===")
    
    # 檢查是否已有資料
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("SELECT count(DISTINCT date_int) FROM stock_shareholding_all WHERE code = ?", (stock_id,))
    dates_count = cursor.fetchone()[0]
    conn.close()
    
    if dates_count > 5:
        print(f"已有 {dates_count} 筆日期資料，跳過")
        return 0
    
    # 重試機制
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 1. 抓取持有比例 (預設)
            html_pct = download_goodinfo_html(stock_id, mode='pct')
            if html_pct:
                data_pct = parse_goodinfo_html(html_pct, stock_id, mode='pct')
                print(f"解析(比例): {len(data_pct)} 筆")
                if data_pct:
                    count = save_to_db(stock_id, data_pct, mode='pct')
                    print(f"寫入(比例): {count} 筆")
                    total_count += count
            
            # 2. 抓取股東人數
            html_holders = download_goodinfo_html(stock_id, mode='holders')
            if html_holders:
                data_holders = parse_goodinfo_html(html_holders, stock_id, mode='holders')
                print(f"解析(人數): {len(data_holders)} 筆")
                if data_holders:
                    count = save_to_db(stock_id, data_holders, mode='holders')
                    print(f"寫入(人數): {count} 筆")
                    total_count += count
            
            # 如果成功抓到資料，就跳出重試迴圈
            if total_count > 0:
                return total_count
            else:
                print(f"警告: {stock_id} 未抓到任何資料 (嘗試 {attempt+1}/{max_retries})")
                time.sleep(5) # 休息一下再試
                
        except Exception as e:
            print(f"發生錯誤 (嘗試 {attempt+1}/{max_retries}): {e}")
            time.sleep(10) # 發生錯誤休息久一點
            
    print(f"放棄 {stock_id}，已達最大重試次數")
    return 0


def main():
    parser = argparse.ArgumentParser(description="從 Goodinfo 下載集保資料")
    parser.add_argument("--stock", type=str, help="指定股票代碼")
    parser.add_argument("--file", type=str, help="使用本地 HTML 檔案")
    parser.add_argument("--all", action="store_true", help="下載所有股票")
    parser.add_argument("--limit", type=int, help="限制股票數量")
    parser.add_argument("--year", type=int, default=2025, help="年份")
    
    args = parser.parse_args()
    
    if args.file:
        # 使用本地檔案
        with open(args.file, "r", encoding="utf-8") as f:
            html = f.read()
        
        stock_id = args.stock or "2330"
        data = parse_goodinfo_html(html, stock_id, args.year)
        print(f"解析: {len(data)} 筆")
        
        if data:
            count = save_to_db(stock_id, data)
            print(f"寫入: {count} 筆")
    
    elif args.stock:
        # 下載單一股票
        scrape_stock(args.stock)
    
    elif args.all:
        # 批次下載 (優先讀取 missing_stocks.txt)
        missing_file = Path("missing_stocks.txt")
        if missing_file.exists():
            print(f"讀取 {missing_file}...")
            with open(missing_file, "r") as f:
                content = f.read().strip()
                if content:
                    stocks = content.split(",")
                else:
                    stocks = []
            print(f"待處理股票: {len(stocks)} 檔")
        else:
            # 若無檔案，則重新掃描
            print("未找到 missing_stocks.txt，重新掃描資料庫...")
            stocks = get_all_stocks(args.limit)
            # 這裡可以加入過濾已完成的邏輯，但為了簡單，假設使用者會先產生清單
            
        total = 0
        # 複製一份清單用於迭代，因為我們要修改檔案
        stocks_to_process = list(stocks)
        
        for i, stock_id in enumerate(stocks_to_process):
            # 檢查是否為空字串
            if not stock_id.strip():
                continue
                
            count = scrape_stock(stock_id)
            
            if count > 0:
                total += count
                # 成功後從 missing_stocks.txt 移除
                if missing_file.exists():
                    try:
                        # 重新讀取 (避免並發問題，雖然這裡是單執行緒)
                        with open(missing_file, "r") as f:
                            current_stocks = f.read().strip().split(",")
                        
                        if stock_id in current_stocks:
                            current_stocks.remove(stock_id)
                            # 寫回檔案
                            with open(missing_file, "w") as f:
                                f.write(",".join(current_stocks))
                            print(f"已從清單移除 {stock_id}")
                    except Exception as e:
                        print(f"更新清單失敗: {e}")
            
            # 進度
            print(f">>> 進度: {i+1}/{len(stocks_to_process)} (剩餘 {len(stocks_to_process) - (i+1)})")
            
            # 避免過快
            time.sleep(2)
        
        print(f"\n\n=== 完成! 共 {total} 筆 ===")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
