import requests
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

print("=== Testing TPEx Institutional API Endpoints ===")

# 測試新版 OpenAPI
urls = [
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_institution_netbuy",
    "https://www.tpex.org.tw/www/zh-tw/insti/tradingInfo?d=115/01/02&t=D&o=json",
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d=115/01/02&se=EW&t=D&o=json"
]

for url in urls:
    print(f"\n--- Testing: {url[:60]}... ---")
    try:
        resp = requests.get(url, timeout=15, verify=False)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            text = resp.text[:200]
            if text.startswith('{') or text.startswith('['):
                print(f"Response (JSON): {text}")
            else:
                print(f"Response (HTML): {text[:100]}")
    except Exception as e:
        print(f"Error: {e}")
