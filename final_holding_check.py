import requests

r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=10')
data = r.json()

print(f"Data date: {data.get('data_date')}")
print()

for item in data['data']:
    code = item['code']
    name = item['name']
    fhs = item.get('foreign_holding_shares', 0)
    fhp = item.get('foreign_holding_pct', 0)
    ths = item.get('trust_holding_shares', 0)
    thp = item.get('trust_holding_pct', 0)
    
    fhs_str = f"{fhs:,}" if fhs else "-"
    fhp_str = f"{fhp}%" if fhp else "-"
    ths_str = f"{ths:,}" if ths else "-"
    thp_str = f"{thp}%" if thp else "-"
    
    print(f"{code} {name}: 外資={fhs_str} ({fhp_str}), 投信={ths_str} ({thp_str})")
