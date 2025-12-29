from playwright.sync_api import sync_playwright

def test():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = 'https://goodinfo.tw/tw/EquityDistributionClassHis.asp?STOCK_ID=2330'
        print(f"開啟 {url}...")
        page.goto(url)
        
        # 印出所有 select 資訊
        selects = page.locator('select').all()
        print(f"找到 {len(selects)} 個 select 元素")
        
        for i, sel in enumerate(selects):
            id_attr = sel.get_attribute('id')
            name_attr = sel.get_attribute('name')
            options = sel.locator('option').all_inner_texts()
            print(f"Select {i}: id={id_attr}, name={name_attr}")
            print(f"Options (前3個): {options[:3]}")
            
            # 嘗試找到目標選項
            if '股東人數區間分級一覽(簡化)' in options:
                print(f"找到目標選單！ID: {id_attr}")
                sel.select_option(label='股東人數區間分級一覽(簡化)')
                break
        
        page.wait_for_load_state('networkidle')
        page.wait_for_timeout(2000)
        
        # 印出表頭 (第二列)
        try:
            header = page.locator('table#tblDetail tr').nth(1).inner_text()
            print(f"表頭:\n{header}")
            
            # 印出第一筆資料
            row = page.locator('table#tblDetail tr').nth(2).inner_text()
            print(f"第一筆:\n{row}")
        except:
            print("無法讀取表格")
        
        browser.close()

if __name__ == "__main__":
    test()
