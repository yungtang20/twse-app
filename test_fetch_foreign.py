import requests
import json

def fetch_foreign_holding(date_str):
    url = f"https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?date={date_str}&selectType=ALL&response=json"
    print(f"Fetching from {url}...")
    try:
        res = requests.get(url)
        data = res.json()
        if data.get('stat') == 'OK':
            print("Data fetched successfully!")
            # Print first 2 records to verify structure
            print(json.dumps(data['data'][:2], indent=2, ensure_ascii=False))
            # Check fields
            print("Fields:", data['fields'])
        else:
            print("Failed to fetch data:", data.get('stat'))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Test with yesterday's date (assuming today is 2025-12-27)
    fetch_foreign_holding("20251226")
