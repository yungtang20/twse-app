import requests
import warnings
warnings.filterwarnings("ignore")

# 證交所開放資料 API 列表
apis = [
    # 行情相關
    ("MI_INDEX (每日收盤行情)", "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=20251218&type=ALLBUT0999"),
    ("BWIBBU_ALL (本益比、殖利率)", "https://www.twse.com.tw/exchangeReport/BWIBBU_ALL?response=json"),
    
    # 法人相關
    ("T86 (三大法人買賣超)", "https://www.twse.com.tw/fund/T86?response=json&date=20251218&selectType=ALL"),
    ("MI_MARGN (融資融券彙總)", "https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20251218&selectType=ALL"),
    
    # OpenAPI 參考
    ("STOCK_DAY_ALL (OpenAPI)", "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"),
    ("T86_ALL (OpenAPI 法人)", "https://openapi.twse.com.tw/v1/fund/T86_ALL"),
    ("MI_MARGN (OpenAPI 融資)", "https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN"),
    ("BWIBBU_d (OpenAPI PE/PB)", "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d"),
]

print("=" * 70)
print("證交所開放資料 API 資料日期測試")
print("=" * 70)
print(f"{'API 名稱':<35} {'日期':<12} {'筆數':<8} {'狀態'}")
print("-" * 70)

for name, url in apis:
    try:
        r = requests.get(url, timeout=15, verify=False)
        data = r.json()
        
        # 網頁版 API (有 stat)
        if isinstance(data, dict):
            stat = data.get('stat', 'N/A')
            date = data.get('date', '-')
            
            # 計算資料筆數
            count = 0
            if 'data' in data and isinstance(data['data'], list):
                count = len(data['data'])
            elif 'tables' in data:
                for t in data['tables']:
                    if t.get('data'):
                        count += len(t.get('data', []))
            
            status = "✓" if stat == 'OK' else "⚠"
            print(f"{name:<35} {date:<12} {count:<8} {status}")
        
        # OpenAPI (list 格式)
        elif isinstance(data, list) and data:
            date = data[0].get('Date', 'N/A')
            count = len(data)
            print(f"{name:<35} {date:<12} {count:<8} ✓")
        else:
            print(f"{name:<35} {'-':<12} {0:<8} ⚠ 無資料")
            
    except Exception as e:
        print(f"{name:<35} {'-':<12} {0:<8} ✗ {str(e)[:20]}")

print("=" * 70)
print("\n對比分析: 網頁版 vs OpenAPI")
print("-" * 50)
print("行情: MI_INDEX 20251218 vs STOCK_DAY_ALL 1141217")
print("法人: T86 網頁版 vs T86_ALL OpenAPI")
print("融資: MI_MARGN 網頁版 vs MI_MARGN OpenAPI")
