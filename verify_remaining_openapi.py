import requests
import json

endpoints = [
    ("TWSE Institutional (T86)", "https://openapi.twse.com.tw/v1/fund/T86_ALL"),
    ("TWSE Margin (MI_MARGN)", "https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN_ALL"),
    ("TPEx Institutional", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_trade_fund"),
    ("TPEx Valuation", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis")
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

for name, url in endpoints:
    print(f"\nTesting {name}...")
    print(f"URL: {url}")
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=15)
        print(f"Status: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list) and len(data) > 0:
                print(f"✅ Success! First item keys: {list(data[0].keys())}")
            else:
                print("⚠ Empty data or not a list")
        else:
            print(f"❌ Failed: {res.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")
