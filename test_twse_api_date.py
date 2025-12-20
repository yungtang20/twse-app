import requests

# 測試 MI_INDEX 個股行情 (type=ALL 或 ALLBUT0999)
url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=20251218&type=ALLBUT0999"
print(f"測試: MI_INDEX (個股行情)")
try:
    r = requests.get(url, timeout=30, verify=False)
    data = r.json()
    print(f"  stat: {data.get('stat')}")
    print(f"  date: {data.get('date')}")
    
    # 檢查 tables 結構
    if 'tables' in data:
        print(f"  tables 數量: {len(data['tables'])}")
        for i, table in enumerate(data['tables']):
            title = table.get('title', 'N/A')
            rows = len(table.get('data', []))
            print(f"    Table {i}: {title} ({rows} rows)")
            if rows > 0 and '每日收盤行情' in title:
                print(f"      第一筆: {table['data'][0][:6]}")
    
    # 檢查 data9 (有時候個股行情在這裡)
    if 'data9' in data:
        print(f"  data9: {len(data['data9'])} rows")
        if data['data9']:
            print(f"    第一筆: {data['data9'][0][:5]}")
            
except Exception as e:
    print(f"  錯誤: {e}")
