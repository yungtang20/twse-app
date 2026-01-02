import requests
import ssl
import json
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Referer': 'https://www.tpex.org.tw/'
}

url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d=115/01/02&se=EW&t=D&o=json"
print(f"Testing: {url}")

resp = requests.get(url, headers=headers, timeout=15, verify=False)
print(f"Status: {resp.status_code}")
print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")

data = resp.json()
print(f"\nKeys: {data.keys()}")
print(f"columnNum: {data.get('columnNum')}")

tables = data.get('tables', [])
print(f"Tables: {len(tables)}")

if tables:
    t = tables[0]
    print(f"\nTable 0 - Title: {t.get('title')}")
    print(f"Fields: {t.get('fields')}")
    print(f"Data rows: {len(t.get('data', []))}")
    if t.get('data'):
        print(f"Sample row: {t['data'][0]}")
