import requests
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'
}

print("=== Testing TPEx OpenAPI with Headers ===")
url1 = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_institution_netbuy"
try:
    resp = requests.get(url1, headers=headers, timeout=15, verify=False)
    print(f"Status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    text = resp.text[:200]
    if text.startswith('[') or text.startswith('{'):
        print(f"JSON Response: {text}")
    else:
        print(f"HTML Response (likely 404/redirect)")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Testing TWSE BFIAUU with RWD API ===")
# 嘗試 RWD API 格式
url2 = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&date=20260102&selectType=ALL"
try:
    resp = requests.get(url2, headers=headers, timeout=15, verify=False)
    print(f"Status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    text = resp.text[:200]
    if text.startswith('[') or text.startswith('{'):
        print(f"JSON Response: {text}")
    else:
        print(f"HTML Response")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Testing TPEx Old API with Headers ===")
url3 = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d=115/01/02&se=EW&t=D&o=json"
try:
    resp = requests.get(url3, headers=headers, timeout=15, verify=False)
    print(f"Status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    text = resp.text[:200]
    if text.startswith('[') or text.startswith('{'):
        print(f"JSON Response: {text}")
    else:
        print(f"HTML Response")
except Exception as e:
    print(f"Error: {e}")
