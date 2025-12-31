import requests
r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=10')
data = r.json()
print(f"Success: {data['success']}")
print(f"Total items returned: {len(data['data'])}")
print(f"Total count: {data['total_count']}")

print("\nAll 10 items:")
for i, item in enumerate(data['data']):
    print(f"{i+1}. {item['code']} {item['name']}: foreign_buy={item.get('foreign_buy')}")
