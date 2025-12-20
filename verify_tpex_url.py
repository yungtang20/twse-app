import requests
import json

def verify_tpex_url():
    url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
    print(f"Testing URL: {url}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        res = requests.get(url, timeout=30, verify=False, headers=headers)
        print(f"Status: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            print(f"Data type: {type(data)}")
            if isinstance(data, list) and len(data) > 0:
                import pprint
                pprint.pprint(data[0])
                if 'SecuritiesCompanyCode' in data[0] and 'ListingDate' in data[0]:
                    print("✓ Validation Successful: Keys found.")
                else:
                    print("✗ Validation Failed: Keys missing.")
            else:
                print("✗ Validation Failed: Empty or invalid data.")
        else:
            print("✗ Validation Failed: Non-200 status.")
    except Exception as e:
        print(f"✗ Validation Failed: {e}")

if __name__ == "__main__":
    verify_tpex_url()
