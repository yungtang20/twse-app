import requests

url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?d=113/12&stkno=8093"
try:
    res = requests.get(url, allow_redirects=True, verify=False)
    print(f"Final URL: {res.url}")
    print(f"Status Code: {res.status_code}")
    print(f"History: {res.history}")
    if res.status_code == 200:
        print(f"Content Start: {res.text[:200]}")
except Exception as e:
    print(f"Error: {e}")
