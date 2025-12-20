import requests
import json

url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"
try:
    res = requests.get(url, timeout=10, verify=False)
    data = res.json()
    if 'data' in data:
        print(f"Total items: {len(data['data'])}")
        codes = [item[0] for item in data['data']]
        print(f"All codes: {codes}")
        if '1240' in codes:
            print("1240 IS in the list!")
        else:
            print("1240 is NOT in the list!")
    else:
        print("No 'data' field in response")
except Exception as e:
    print(f"Error: {e}")
