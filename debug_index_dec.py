import requests
import sqlite3
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_twse_data(api_name, date_str):
    url = f"https://www.twse.com.tw/exchangeReport/{api_name}?response=json&date={date_str}"
    if api_name == "MI_5MINS_HIST":
        url = f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={date_str}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    res = requests.get(url, headers=headers, verify=False)
    return res.json()

date_str = "20251201"
ohlc_json = get_twse_data("MI_5MINS_HIST", date_str)
summary_json = get_twse_data("FMTQIK", date_str)

print("OHLC Stat:", ohlc_json.get("stat"))
print("Summary Stat:", summary_json.get("stat"))

if ohlc_json.get("stat") == "OK" and summary_json.get("stat") == "OK":
    print("Data found for Dec 2025")
    print("OHLC count:", len(ohlc_json.get("data", [])))
    print("Summary count:", len(summary_json.get("data", [])))
    for row in summary_json.get("data", []):
        if "12/19" in row[0]:
            print("Dec 19 Summary:", row)
    for row in ohlc_json.get("data", []):
        if "12/19" in row[0]:
            print("Dec 19 OHLC:", row)
else:
    print("Stat not OK")
    print("OHLC:", ohlc_json)
    print("Summary:", summary_json)
