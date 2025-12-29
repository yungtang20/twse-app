import requests
import time

# 測試 API 返回值
TWSE_STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_DAILY_TRADING_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

# 1. TWSE API
print("Testing TWSE API...")
try:
    url = f"{TWSE_STOCK_DAY_ALL_URL}&_={int(time.time())}"
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    })
    res = session.get(url, timeout=10, verify=False)
    print(f"Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        if isinstance(data, list) and len(data) > 0:
            print(f"First item: {data[0]}")
            raw_date = data[0].get('Date', 'N/A')
            print(f"Date field: {raw_date}")
        elif isinstance(data, dict):
            print(f"Date field in dict: {data.get('date', 'N/A')}")
            print(f"First 100 chars: {str(data)[:100]}")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "="*60 + "\n")

# 2. TPEx API
print("Testing TPEx API...")
try:
    url = f"{TPEX_DAILY_TRADING_URL}?d=&stk_code=&o=json&_={int(time.time())}"
    res = requests.get(url, timeout=10, verify=False)
    print(f"Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        print(f"Keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
        if 'reportDate' in data:
            print(f"reportDate: {data['reportDate']}")
        print(f"First 200 chars: {str(data)[:200]}")
except Exception as e:
    print(f"Error: {e}")
