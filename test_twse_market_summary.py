import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_twse_market_summary(date_str):
    url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date_str}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    res = requests.get(url, headers=headers, verify=False)
    if res.status_code == 200:
        data = res.json()
        if data.get("stat") == "OK":
            return data.get("data")
    return None

data = fetch_twse_market_summary("20251201")
if data:
    for row in data:
        # Row format: [日期, 成交股數, 成交金額, 成交筆數, 發行量加權股價指數, 漲跌點數]
        print(row)
else:
    print("Failed to fetch data")
