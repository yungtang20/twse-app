"""
Scrape institutional holdings data from Goodinfo.
Gets: 持有張數 (holdings) and 持股比 (holding %) for each institutional type.
"""
import requests
from bs4 import BeautifulSoup
import time
import re
from backend.services.db import db_manager

def fetch_goodinfo_holdings(stock_code):
    """Fetch institutional holdings for a single stock from Goodinfo."""
    url = f"https://goodinfo.tw/tw/StockBzPerformance.asp?STOCK_ID={stock_code}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://goodinfo.tw/tw/index.asp',
        'Cookie': 'IS_TOUCH_DEVICE=F'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table with institutional data
        # Look for table containing "外資" and "持有張數"
        tables = soup.find_all('table')
        
        result = {
            'foreign_holding': None,
            'foreign_ratio': None,
            'trust_holding': None,
            'trust_ratio': None,
            'dealer_holding': None,
            'dealer_ratio': None,
        }
        
        for table in tables:
            text = table.get_text()
            if '外資' in text and '持有張數' in text and '持股比' in text:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 6:
                        row_text = cells[0].get_text(strip=True)
                        
                        if '外資' in row_text and '張數' in row_text:
                            try:
                                result['foreign_holding'] = parse_number(cells[3].get_text(strip=True))
                                result['foreign_ratio'] = parse_percentage(cells[5].get_text(strip=True))
                            except:
                                pass
                        elif '投信' in row_text and '張數' in row_text:
                            try:
                                result['trust_holding'] = parse_number(cells[3].get_text(strip=True))
                                result['trust_ratio'] = parse_percentage(cells[5].get_text(strip=True))
                            except:
                                pass
                        elif '自營' in row_text and '張數' in row_text:
                            try:
                                result['dealer_holding'] = parse_number(cells[3].get_text(strip=True))
                                result['dealer_ratio'] = parse_percentage(cells[5].get_text(strip=True))
                            except:
                                pass
                                
        return result
        
    except Exception as e:
        print(f"Error fetching {stock_code}: {e}")
        return None

def parse_number(text):
    """Parse number from text like '21,456' or '1.5萬' """
    text = text.replace(',', '').replace(' ', '')
    if '萬' in text:
        return int(float(text.replace('萬', '')) * 10000)
    elif '億' in text:
        return int(float(text.replace('億', '')) * 100000000)
    else:
        return int(float(text))

def parse_percentage(text):
    """Parse percentage from text like '11.92%' """
    text = text.replace('%', '').replace(' ', '')
    return float(text)

def update_all_holdings():
    """Update holdings for all stocks."""
    # Get all stock codes
    stocks = db_manager.execute_query("SELECT code FROM stock_snapshot ORDER BY code")
    total = len(stocks)
    print(f"Updating holdings for {total} stocks...")
    
    for i, stock in enumerate(stocks):
        code = stock['code']
        
        # Skip non-numeric codes
        if not code.isdigit():
            continue
            
        holdings = fetch_goodinfo_holdings(code)
        
        if holdings and any(holdings.values()):
            # Update database
            sql = """
                UPDATE stock_snapshot SET 
                    foreign_cumulative = COALESCE(?, foreign_cumulative),
                    trust_cumulative = COALESCE(?, trust_cumulative),
                    dealer_cumulative = COALESCE(?, dealer_cumulative)
                WHERE code = ?
            """
            db_manager.execute_update(sql, (
                holdings.get('foreign_holding'),
                holdings.get('trust_holding'),
                holdings.get('dealer_holding'),
                code
            ))
            print(f"[{i+1}/{total}] {code}: 外資={holdings.get('foreign_holding')}, 投信={holdings.get('trust_holding')}, 自營={holdings.get('dealer_holding')}")
        else:
            print(f"[{i+1}/{total}] {code}: No data")
        
        # Rate limiting - be respectful to the server
        time.sleep(0.5)
        
        if (i + 1) % 50 == 0:
            print(f"Progress: {i+1}/{total}")

if __name__ == "__main__":
    # Test with a single stock first
    print("Testing with 2330...")
    result = fetch_goodinfo_holdings("2330")
    print(f"Result: {result}")
    
    # Uncomment to update all stocks:
    # update_all_holdings()
