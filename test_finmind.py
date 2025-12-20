import requests
from datetime import datetime

today = datetime.now().strftime("%Y-%m-%d")
yesterday = "2025-12-17"  # 嘗試昨天

# 使用 FINMIND_TOKEN
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMi0xNyAyMjowMzowMiIsInVzZXJfaWQiOiJ5dW5ndGFuZyAiLCJpcCI6IjExMS43MS4yMTIuMjUifQ.fYv38gHAin0IZu5GZZyFFjj5tPU8BCCORDTUTandpDg"

for test_date in [today, yesterday]:
    print(f"\n測試日期: {test_date}")
    params = {
        "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
        "start_date": test_date,
        "end_date": test_date,
        "token": TOKEN
    }
    
    resp = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, verify=False)
    data = resp.json()
    
    print(f"  Status: {data.get('status')}")
    print(f"  Message: {data.get('msg', 'N/A')}")
    print(f"  Records: {len(data.get('data', []))}")
    
    if data.get('data'):
        print(f"  Sample: {data['data'][0]}")
