import requests
import time

# 測試 TPEx API
TPEX_DAILY_TRADING_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

print("Testing TPEx API...")
try:
    url = f"{TPEX_DAILY_TRADING_URL}?d=&stk_code=&o=json&_={int(time.time())}"
    res = requests.get(url, timeout=10, verify=False)
    print(f"Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        print(f"Keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
        if 'reportDate' in data:
            raw_date = data['reportDate']
            print(f"reportDate (raw): '{raw_date}'")
            
            # 模擬 roc_to_western_date
            parts = raw_date.split('/')
            print(f"Parts: {parts}")
            if len(parts) == 3:
                roc_year = int(parts[0])
                western_year = roc_year + 1911
                print(f"ROC year: {roc_year}, Western year: {western_year}")
                western_date = f"{western_year}-{parts[1]}-{parts[2]}"
                print(f"Converted: {western_date}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
