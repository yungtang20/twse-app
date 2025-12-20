import requests
import warnings
warnings.filterwarnings("ignore")

print("=" * 60)
print("Step 1 股票清單 API 日期檢查")
print("=" * 60)

apis = [
    # TWSE
    ("TWSE 基本資料 (t187ap03_L)", "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"),
    ("TWSE 行情表 (STOCK_DAY_ALL)", "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"),
    
    # TPEx
    ("TPEx 基本資料 (mopsfin_t187ap03_O)", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"),
    ("TPEx 行情表 (daily_close_quotes)", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"),
]

print(f"{'API 名稱':<35} {'日期':<12} {'筆數'}")
print("-" * 60)

for name, url in apis:
    try:
        r = requests.get(url, timeout=20, verify=False)
        data = r.json()
        
        if isinstance(data, list) and data:
            # 找日期欄位
            date = data[0].get('Date', data[0].get('日期', data[0].get('出表日期', 'N/A')))
            print(f"{name:<35} {date:<12} {len(data)}")
        else:
            print(f"{name:<35} {'N/A':<12} 0")
    except Exception as e:
        print(f"{name:<35} {'錯誤':<12} {str(e)[:20]}")

print("=" * 60)
