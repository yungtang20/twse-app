import requests
url = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
r = requests.get(url, timeout=20, verify=False)
data = r.json()
print(f"Records: {len(data)}")
if data:
    print(f"API Date: {data[0].get('Date')}")
