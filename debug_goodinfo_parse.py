import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

def main():
    url = "https://goodinfo.tw/tw/EquityDistributionClassHis.asp?STOCK_ID=1101"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://goodinfo.tw/tw/StockList.asp"
    }
    
    try:
        print(f"Fetching {url}...")
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            print(f"HTTP Error: {response.status_code}")
            return
            
        html = response.text
        with open("debug_1101.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("Saved debug_1101.html")
        
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {'id': 'tblDetail'})
        
        if not table:
            print("Table tblDetail not found!")
            # Try to find any table with "週別"
            tables = soup.find_all('table')
            for t in tables:
                if '週別' in t.get_text():
                    print("Found table by keyword '週別'")
                    table = t
                    break
        
        if not table:
            print("No suitable table found.")
            return
            
        print("Table found. Parsing...")
        dfs = pd.read_html(StringIO(str(table)))
        df = dfs[0]
        print("Columns:", df.columns)
        print("First 5 rows:")
        print(df.head())
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
