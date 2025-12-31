import requests
r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=10')
data = r.json()
print(f"Success: {data['success']}")
print(f"Total items returned: {len(data['data'])}")
print(f"Total count: {data['total_count']}")
print(f"Total pages: {data['total_pages']}")
print(f"Data date: {data.get('data_date')}")
if data['data']:
    print("\nFirst 3 items:")
    for item in data['data'][:3]:
        print(f"  {item['code']} {item['name']}: foreign_buy={item.get('foreign_buy')}")
