import requests
r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=5')
data = r.json()

print("First 5 items with holding data:")
for item in data['data']:
    print(f"  {item['code']} {item['name']}:")
    print(f"    foreign_holding_shares={item.get('foreign_holding_shares')}")
    print(f"    foreign_holding_pct={item.get('foreign_holding_pct')}")
    print(f"    trust_holding_shares={item.get('trust_holding_shares')}")
    print(f"    trust_holding_pct={item.get('trust_holding_pct')}")
