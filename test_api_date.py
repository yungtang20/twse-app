import sys
sys.path.insert(0, 'd:\\twse')

# 模擬簡化版
import requests
import time
from datetime import datetime

TWSE_STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

def get_latest_market_date():
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    today_int = int(now.strftime("%Y%m%d"))

    dates = []

    # 1. Check TWSE
    try:
        url = f"{TWSE_STOCK_DAY_ALL_URL}&_={int(time.time())}"
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        resp = session.get(url, timeout=10, verify=False)
        
        if resp.status_code == 200:
            data = resp.json()
            if data and len(data) > 0:
                first_item = data[0]
                raw_date = first_item.get('Date', '')
                print(f"Raw Date from API: {raw_date}")
                
                # 轉換民國年為西元年
                if raw_date:
                    parts = raw_date.split('/')
                    if len(parts) == 3:
                        roc_year = int(parts[0]) + 1911
                        western_date = f"{roc_year}-{parts[1]}-{parts[2]}"
                        print(f"Converted date: {western_date}")
                        return western_date
    except Exception as e:
        print(f"Error: {e}")

    return today_str

result = get_latest_market_date()
print(f"get_latest_market_date() returned: {result}")
