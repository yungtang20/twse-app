"""
Test all scan endpoints
"""
import requests

BASE_URL = "http://localhost:8000/api"

endpoints = [
    ("VP箱型", "/scan/vp?direction=support&limit=5"),
    ("MFI資金", "/scan/mfi?condition=oversold&limit=5"),
    ("均線掃描", "/scan/ma?pattern=bull&limit=5"),
    ("月KD交叉", "/scan/kd-cross?signal=golden&timeframe=month&limit=5"),
    ("VSBC籌碼", "/scan/vsbc?style=steady&limit=5"),
    ("聰明錢", "/scan/smart-money?limit=5"),
    ("2560戰法", "/scan/2560?limit=5"),
    ("五階篩選", "/scan/five-stage?limit=5"),
    ("機構價值", "/scan/institutional-value?limit=5"),
    ("六維共振", "/scan/six-dim?limit=5"),
    ("K線型態", "/scan/patterns?type=morning_star&limit=5"),
    ("量價背離", "/scan/pv-divergence?limit=5"),
]

print("=" * 60)
print("Testing all scan endpoints...")
print("=" * 60)

for name, endpoint in endpoints:
    try:
        res = requests.get(f"{BASE_URL}{endpoint}", timeout=10)
        data = res.json()
        
        if res.status_code == 200 and data.get("success"):
            count = data.get("data", {}).get("count", 0)
            print(f"✅ {name}: {count} 筆結果")
        else:
            error = data.get("detail", "Unknown error")
            print(f"❌ {name}: {error}")
    except Exception as e:
        print(f"❌ {name}: {str(e)}")

print("=" * 60)
print("Testing complete!")
