import requests

r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=5')
data = r.json()

print("All fields for first 3 stocks:")
for item in data['data'][:3]:
    print(f"\n{item['code']} {item['name']}:")
    for k, v in item.items():
        if 'trust' in k.lower() or 'foreign' in k.lower():
            print(f"  {k}: {v}")
