import twstock

tse = twstock.Index()
# Get recent data
data = tse.fetch(2025, 12)
if data:
    for d in data:
        print(f"Date: {d.date}, Close: {d.close}, Capacity: {d.capacity}, Turnover: {d.turnover}")
else:
    print("No data found")
