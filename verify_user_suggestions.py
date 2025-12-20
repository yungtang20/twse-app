import requests
import json

# Potential URL patterns
base_twse = "https://openapi.twse.com.tw/v1"
base_tpex = "https://www.tpex.org.tw/openapi/v1"

endpoints = [
    ("TWSE Institutional (T86_ALL)", f"{base_twse}/fund/T86_ALL"),
    ("TWSE PE/PB (BWIBYK_ALL)", f"{base_twse}/exchangeReport/BWIBYK_ALL"),
    ("TPEx Institutional", f"{base_tpex}/tpex_mainboard_daily_investor_trading"),
    ("TPEx Margin", f"{base_tpex}/tpex_mainboard_daily_margin_trading"),
    ("TPEx PE/PB", f"{base_tpex}/tpex_mainboard_per_pb_dividend_yield")
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

for name, url in endpoints:
    print(f"\nTesting {name}...")
    print(f"URL: {url}")
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=10)
        print(f"Status: {res.status_code}")
        if res.status_code == 200:
            try:
                data = res.json()
                if isinstance(data, list) and len(data) > 0:
                    print(f"Success! Keys: {list(data[0].keys())[:5]}...")
                else:
                    print("Empty data or not a list")
            except Exception as e:
                print(f"JSON Decode Error: {e}")
                print(f"Content Preview: {res.text[:200]}")
        else:
            print("Failed")
    except Exception as e:
        print(f"Error: {e}")
