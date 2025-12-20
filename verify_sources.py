import requests
import json
import time

def test_twse():
    print("Testing TWSE (STOCK_DAY)...")
    # URL for 2330, Date 20251217
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20251217&stockNo=2330&response=json"
    print(f"URL: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        res = requests.get(url, timeout=10, headers=headers)
        data = res.json()
        if data.get('stat') == 'OK':
            print("✅ TWSE Fetch Success!")
            print(f"Data Count: {len(data.get('data', []))}")
            if data.get('data'):
                print(f"Latest Entry: {data['data'][-1]}")
        else:
            print(f"⚠ TWSE Response: {data.get('stat')}")
    except Exception as e:
        print(f"❌ TWSE Error: {e}")

def test_tpex():
    print("\nTesting TPEx (Daily Close Quotes)...")
    # URL for 8069 (PChome), Date 114/12/16 (2025/12/16)
    # TPEx uses ROC year
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=114/12/16&o=json"
    print(f"URL: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        res = requests.get(url, timeout=10, verify=False, headers=headers)
        data = res.json()
        # TPEx returns a large object with 'aaData' or 'reportDate'
        if data.get('reportDate') or data.get('aaData'):
            print("✅ TPEx Fetch Success!")
            print(f"Report Date: {data.get('reportDate')}")
            # Check for a specific stock in aaData if possible, but this API returns ALL stocks
            # Wait, daily_close_quotes returns ALL stocks for a specific day.
            count = len(data.get('aaData', []))
            print(f"Total Stocks Fetched: {count}")
        else:
            print("⚠ TPEx Response: No data found")
    except Exception as e:
        print(f"❌ TPEx Error: {e}")

if __name__ == "__main__":
    test_twse()
    test_tpex()
