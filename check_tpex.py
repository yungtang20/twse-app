import requests
import json

def check_tpex_api():
    url = "https://www.tpex.org.tw/openapi/v1/t187ap03_O"
    print(f"Checking {url}...")
    try:
        response = requests.get(url, verify=False, timeout=10)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"Data type: {type(data)}")
                if isinstance(data, list):
                    print(f"Count: {len(data)}")
                    if len(data) > 0:
                        print(f"First item keys: {data[0].keys()}")
                        print(f"First item: {data[0]}")
            except Exception as e:
                print(f"JSON Decode Error: {e}")
                print(f"Content snippet: {response.text[:200]}")
        else:
            print(f"Error Content: {response.text[:200]}")
    except Exception as e:
        print(f"Request Error: {e}")

if __name__ == "__main__":
    check_tpex_api()
