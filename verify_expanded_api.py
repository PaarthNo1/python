import requests
import sys

def test_api():
    base_url = "http://localhost:8000"
    float_id = "1902043"
    
    print(f"Testing Full Info API: {base_url}/float_fullinfo/{float_id}")
    try:
        resp = requests.get(f"{base_url}/float_fullinfo/{float_id}")
        if resp.status_code == 200:
            data = resp.json()
            print("✅ Main API Success!")
            print(f"🔗 Links found: {data.get('links')}")
            
            # Test Links
            links = data.get('links', {})
            for key, link in links.items():
                if key == "cycles": continue # Placeholder
                
                full_link = f"http://localhost:8000{link}"
                print(f"   Testing {key}: {full_link}")
                sub_resp = requests.get(full_link)
                if sub_resp.status_code == 200:
                    print(f"   ✅ {key} API Success! (Items: {len(sub_resp.json())})")
                else:
                    print(f"   ❌ {key} API Failed: {sub_resp.status_code}")
        else:
            print(f"❌ Main API Failed: {resp.status_code}")
            
    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    test_api()
