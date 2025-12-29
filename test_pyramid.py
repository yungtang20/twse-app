from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def test_pyramid(stock_id="1626"):
    url = f"https://norway.twsthr.info/StockHolders.aspx?StockCode={stock_id}"
    print(f"Testing {url}...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, timeout=30000)
            page.wait_for_selector("table.lvt.small", timeout=10000) # 假設表格 class
            
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            # 尋找資料表格
            # 神秘金字塔的表格通常包含 "集保總張數", "總股東人數", "百萬大戶" 等
            # 這裡簡單印出表格文字來確認結構
            rows = soup.find_all("tr")
            print(f"Found {len(rows)} rows")
            
            for i, row in enumerate(rows[:10]): # 印出前 10 行
                print(f"Row {i}: {row.get_text(strip=True)[:100]}")
                
        except Exception as e:
            print(f"Error: {e}")
            
        browser.close()

if __name__ == "__main__":
    test_pyramid()
