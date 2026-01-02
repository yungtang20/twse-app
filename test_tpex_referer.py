import requests

url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/daily-quote/daily-trading-info/result.php?l=zh-tw&d=113/12&stkno=8093"
headers = {
    "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/daily-quote/daily-trading-info/index.html",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

try:
    res = requests.get(url, headers=headers, verify=False)
    print(f"Status Code: {res.status_code}")
    print(f"Content Start: {res.text[:200]}")
except Exception as e:
    print(f"Error: {e}")
