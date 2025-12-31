import requests
import time

def test_api():
    url = "http://127.0.0.1:8000/api/admin/sync/status"
    print(f"Testing API: {url}")
    try:
        start = time.time()
        response = requests.get(url, timeout=10)
        end = time.time()
        print(f"Status Code: {response.status_code}")
        print(f"Time Taken: {end - start:.2f}s")
        print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
