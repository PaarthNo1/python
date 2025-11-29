import requests
import sys

def test_api():
    # Try to fetch info for a float
    url = "http://localhost:8000/float_fullinfo/1902043"
    print(f"Testing API: {url}")
    
    try:
        resp = requests.get(url)
        print(f"Status Code: {resp.status_code}")
        print(f"Response: {resp.json()}")
        
        if resp.status_code == 200:
            print("✅ API returned success!")
        elif resp.status_code == 404:
            print("✅ API returned 404 (Expected if DB is empty). No 500 Error!")
        else:
            print("❌ API returned unexpected status.")
            
    except Exception as e:
        print(f"❌ Failed to connect to API: {e}")

if __name__ == "__main__":
    test_api()
