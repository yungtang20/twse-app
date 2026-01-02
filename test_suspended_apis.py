import requests
import json
import urllib3

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Endpoints
ENDPOINTS = {
    "TWSE Punished": "https://www.twse.com.tw/rwd/zh/announcement/punish?response=json",
    "TWSE Suspended": "https://www.twse.com.tw/rwd/zh/company/suspendListingCsvAndHtml?response=json",
    "TPEx Punished": "https://www.tpex.org.tw/web/bulletin/announcement/punish_result.php?l=zh-tw&o=json",
    "TPEx Suspended (Halt)": "https://www.tpex.org.tw/web/stock/aftertrading/trading_halt/halt_result.php?l=zh-tw&o=json",
    "TPEx Altered (Altered)": "https://www.tpex.org.tw/web/stock/aftertrading/altered_trading_method/altered_trading_method_result.php?l=zh-tw&o=json",
    "TPEx Terminated": "https://www.tpex.org.tw/web/company/suspend_listing/suspend_listing_result.php?l=zh-tw&o=json"
}

target = '8291'

print(f"Searching for {target} in suspended/punished lists...")

for name, url in ENDPOINTS.items():
    print(f"Testing {name}...")
    try:
        res = requests.get(url, timeout=10, verify=False)
        if res.status_code == 200:
            try:
                data = res.json()
                # Search recursively in json
                found = False
                
                def search_json(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if str(v) == target: return True
                            if search_json(v): return True
                    elif isinstance(obj, list):
                        for item in obj:
                            if str(item) == target: return True
                            if search_json(item): return True
                    elif str(obj) == target:
                        return True
                    return False
                
                if search_json(data):
                    print(f"  [MATCH] Found {target} in {name}!")
                    # Print snippet
                    print(f"  Snippet: {str(data)[:200]}...")
                else:
                    print(f"  Not found in {name}")
                    
            except:
                print(f"  Not JSON. Text length: {len(res.text)}")
                if target in res.text:
                    print(f"  [MATCH] Found {target} in text response of {name}!")
        else:
            print(f"  Status {res.status_code}")
    except Exception as e:
        print(f"  Error: {e}")
