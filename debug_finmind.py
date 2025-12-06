import requests
import json

# Token provided by user
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMi0wNiAwODo0NDo1NiIsInVzZXJfaWQiOiJ5dW5ndGFuZyAiLCJpcCI6IjExMS43MS4yMTIuNzIifQ.aiTV3Tn7wjahSEZcHQqSwAJqfx5UHNM2upAVq-LnFmA"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def test_finmind():
    print(f"Testing FinMind API...")
    url = f"{FINMIND_URL}?dataset=TaiwanStockPrice&stock_id=2330&start_date=2024-01-01&token={FINMIND_TOKEN}"
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, timeout=10, verify=False)
        print(f"Status Code: {response.status_code}")
        print(f"Response Text: {response.text[:500]}...") # Print first 500 chars
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 200 or 'data' in data:
                print("✅ FinMind API seems OK")
            else:
                print("❌ FinMind API returned 200 but invalid data structure")
        else:
            print("❌ FinMind API failed")
            
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    test_finmind()
