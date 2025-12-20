import requests
import json

url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
print(f"Testing URL: {url}")

try:
    res = requests.get(url, verify=False, timeout=30)
    print(f"Status: {res.status_code}")
    if res.status_code == 200:
        data = res.json()
        print(f"Data type: {type(data)}")
        if isinstance(data, list) and len(data) > 0:
            print(f"First item keys: {list(data[0].keys())}")
            import pprint
            pprint.pprint(data[0])
        else:
            print("No data or empty list")
    else:
        print(f"Error: {res.text[:100]}")
except Exception as e:
    print(f"Exception: {e}")
