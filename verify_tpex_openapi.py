import requests
import json

url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
print(f"Testing URL: {url}")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

try:
    res = requests.get(url, headers=headers, verify=False, timeout=30)
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
