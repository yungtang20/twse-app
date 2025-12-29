"""
測試 TPEx 法人 OpenAPI 是否有 6730 資料
"""
import requests
import json

url = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
print(f"正在抓取: {url}")

resp = requests.get(url, timeout=30, verify=False)
print(f"狀態碼: {resp.status_code}")

data = resp.json()
print(f"總筆數: {len(data)}")

# 找 6730
for item in data:
    code = item.get('SecuritiesCompanyCode', '')
    if code == '6730':
        print(f"\n找到 6730:")
        print(json.dumps(item, indent=2, ensure_ascii=False))
        break
else:
    print("\n未找到 6730")
    # 顯示前 3 筆資料結構
    print("\n資料結構範例 (前 3 筆):")
    for item in data[:3]:
        print(json.dumps(item, indent=2, ensure_ascii=False))
