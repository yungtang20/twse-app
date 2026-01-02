import requests
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://www.tpex.org.tw/'
}

print("=== Testing Multiple TPEx Institutional API Endpoints ===")

# 嘗試多種 API 格式
urls = [
    # 舊版 API (2024 前)
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d=115/01/02&se=EW&t=D&o=json",
    # 政府資料開放平臺格式
    "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_institution_netbuy",
    # 新版網頁 API
    "https://www.tpex.org.tw/www/zh-tw/insti/allTradingInfo?d=115/01/02&t=D",
    # 新版網頁 API (JSON格式)
    "https://www.tpex.org.tw/www/zh-tw/insti/tradingInfo?d=115/01/02&t=D&o=json",
    # RWD API (類似 TWSE)
    "https://www.tpex.org.tw/rwd/zh-tw/insti/tradingInfo?response=json&date=115/01/02",
]

for url in urls:
    print(f"\n--- Testing: {url[:70]}... ---")
    try:
        resp = requests.get(url, headers=headers, timeout=15, verify=False)
        print(f"Status: {resp.status_code}")
        ctype = resp.headers.get('Content-Type', 'N/A')
        print(f"Content-Type: {ctype}")
        text = resp.text[:150]
        if 'json' in ctype.lower() or text.startswith('[') or text.startswith('{'):
            print(f"✓ JSON Response: {text}")
        else:
            print(f"✗ Not JSON: {text[:80]}...")
    except Exception as e:
        print(f"Error: {e}")
