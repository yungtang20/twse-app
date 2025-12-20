import requests
import json

url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
try:
    res = requests.get(url, timeout=10, verify=False)
    data = res.json()
    print(f"Total items: {len(data)}")
    # Check stock 1240
    found = False
    for item in data:
        if item.get('Code') == '1240':
            print(f"Stock 1240 data: {item}")
            found = True
            break
    if not found:
        print("Stock 1240 not found in STOCK_DAY_AVG_ALL")
except Exception as e:
    print(f"Error: {e}")
