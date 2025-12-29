"""
解析 Goodinfo 集保分級 HTML 檔案並匯入資料庫

Goodinfo HTML 表格欄位:
- 週別 (如 25W51)
- 統計日期 (如 12/19)
- 收盤價, 漲跌(元), 漲跌(%)
- 集保庫存(萬張)
- ≦10張, >10張≦50張, >50張≦100張, ... , >1千張 的持股比例

我們主要需要:
- 統計日期 -> 轉換為 date_int
- 各持股分級的比例
"""

from bs4 import BeautifulSoup
import sqlite3
import re
import argparse
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "taiwan_stock.db"


def parse_goodinfo_html(html_path, stock_id, year=2025):
    """
    解析 Goodinfo 集保分級 HTML 檔案
    
    Args:
        html_path: HTML 檔案路徑
        stock_id: 股票代碼
        year: 年份 (用於轉換日期)
    
    Returns:
        list: [(date_int, large_pct), ...]  
              large_pct = 大戶持股比例 (>1千張)
    """
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "tblDetail"})
    
    if not table:
        print("找不到 tblDetail 表格")
        return []
    
    results = []
    rows = table.find_all("tr")
    
    # 跳過表頭 (前2行)
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) < 14:  # 確保有足夠欄位
            continue
        
        try:
            # 週別 (如 25W51)
            week_str = cells[0].get_text(strip=True)
            
            # 統計日期 (如 12/19)
            date_str = cells[1].get_text(strip=True)
            
            # 解析年份從週別 (25W51 -> 2025)
            week_match = re.match(r"(\d{2})W(\d+)", week_str)
            if week_match:
                yr = int(week_match.group(1)) + 2000
            else:
                yr = year
            
            # 轉換日期
            month, day = date_str.split("/")
            date_int = int(f"{yr}{month.zfill(2)}{day.zfill(2)}")
            
            # 集保庫存(萬張) - 欄位 5 (index 5)
            # 各持股分級比例 - 欄位 6-13
            # ≦10張: index 6
            # >10張≦50張: index 7
            # >50張≦100張: index 8
            # >100張≦200張: index 9
            # >200張≦400張: index 10
            # >400張≦800張: index 11
            # >800張≦1千張: index 12
            # >1千張: index 13
            
            pct_10 = float(cells[6].get_text(strip=True) or 0)      # ≦10張
            pct_50 = float(cells[7].get_text(strip=True) or 0)      # >10張≦50張
            pct_100 = float(cells[8].get_text(strip=True) or 0)     # >50張≦100張
            pct_200 = float(cells[9].get_text(strip=True) or 0)     # >100張≦200張
            pct_400 = float(cells[10].get_text(strip=True) or 0)    # >200張≦400張
            pct_800 = float(cells[11].get_text(strip=True) or 0)    # >400張≦800張
            pct_1000 = float(cells[12].get_text(strip=True) or 0)   # >800張≦1千張
            pct_large = float(cells[13].get_text(strip=True) or 0)  # >1千張
            
            # 計算總人數 (這邊沒有人數資料，只有比例)
            # 我們存的是各級比例，而非人數
            
            results.append({
                "date_int": date_int,
                "week": week_str,
                "pct_10": pct_10,
                "pct_50": pct_50,
                "pct_100": pct_100,
                "pct_200": pct_200,
                "pct_400": pct_400,
                "pct_800": pct_800,
                "pct_1000": pct_1000,
                "pct_large": pct_large,  # 大戶持股 (>1千張)
            })
            
            print(f"  {date_int} ({week_str}): 大戶 {pct_large}%")
            
        except Exception as e:
            print(f"  解析錯誤: {e}")
            continue
    
    return results


def save_to_db(stock_id, data):
    """
    儲存到資料庫
    
    由於 Goodinfo 的分級與 TDCC 不同，我們需要建一個新表或映射
    暫時只存大戶持股比例到 stock_history 表的 large_shareholder_pct
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    count = 0
    for item in data:
        date_int = item["date_int"]
        pct_large = item["pct_large"]
        
        # 更新 stock_history 表的 large_shareholder_pct
        cursor.execute("""
            UPDATE stock_history 
            SET large_shareholder_pct = ?
            WHERE code = ? AND date_int = ?
        """, (pct_large, stock_id, date_int))
        
        if cursor.rowcount > 0:
            count += 1
    
    conn.commit()
    
    # 也存入 stock_shareholding_all 表
    # 映射 Goodinfo 分級到 TDCC 分級:
    # TDCC 15級 (1000張以上) -> Goodinfo "大於1千張"
    for item in data:
        date_int = item["date_int"]
        
        # 只存 >1000張 的比例到 level 15
        cursor.execute("""
            INSERT OR REPLACE INTO stock_shareholding_all
            (code, date_int, level, holders, shares, proportion)
            VALUES (?, ?, 15, 0, 0, ?)
        """, (stock_id, date_int, item["pct_large"]))
    
    conn.commit()
    conn.close()
    
    return count


def main():
    parser = argparse.ArgumentParser(description="解析 Goodinfo 集保 HTML 並匯入資料庫")
    parser.add_argument("--file", type=str, default="EquityDistributionClassHis.html", 
                       help="HTML 檔案路徑")
    parser.add_argument("--stock", type=str, default="2330", help="股票代碼")
    parser.add_argument("--year", type=int, default=2025, help="年份")
    
    args = parser.parse_args()
    
    html_path = Path(args.file)
    if not html_path.exists():
        print(f"找不到檔案: {html_path}")
        return
    
    print(f"=== 解析 {html_path} ===")
    print(f"股票代碼: {args.stock}")
    print()
    
    data = parse_goodinfo_html(html_path, args.stock, args.year)
    
    print(f"\n共解析 {len(data)} 筆資料")
    
    if data:
        count = save_to_db(args.stock, data)
        print(f"已更新 {count} 筆 stock_history 記錄")
        print(f"已寫入 {len(data)} 筆 stock_shareholding_all 記錄")


if __name__ == "__main__":
    main()
