import requests
import json

def fetch_twse_index(date_str):
    url = f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={date_str}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        data = res.json()
        if data.get("stat") == "OK":
            return data.get("data")
    return None

data = fetch_twse_index("20251201")
if data:
    for row in data:
        # Row format: [Date, Open, High, Low, Close]
        # Wait, MI_5MINS_HIST only has OHLC.
        # We need volume and amount too.
        print(row)
else:
    print("Failed to fetch data")
