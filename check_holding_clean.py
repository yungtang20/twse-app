import requests

r = requests.get('http://localhost:8000/api/rankings/institutional?type=foreign&sort=buy&limit=10')
data = r.json()

print(f"Data date: {data.get('data_date')}")
print(f"Total items: {len(data['data'])}")
print("\n" + "="*80)
print("Code    Name       foreign_holding_shares  foreign_holding_pct  trust_holding_shares  trust_holding_pct")
print("="*80)

for item in data['data']:
    code = item['code']
    name = item['name'][:8].ljust(8)
    fhs = str(item.get('foreign_holding_shares', 0)).rjust(20)
    fhp = str(item.get('foreign_holding_pct', 0)).rjust(18)
    ths = str(item.get('trust_holding_shares', 0)).rjust(18)
    thp = str(item.get('trust_holding_pct', 0)).rjust(15)
    print(f"{code}    {name}   {fhs}  {fhp}  {ths}  {thp}")
