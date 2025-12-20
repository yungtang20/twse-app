import requests
import pandas as pd
import io

url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
print(f"Testing URL: {url}")

try:
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=headers, verify=False, timeout=30)
    print(f"Status: {res.status_code}")
    
    if res.status_code == 200:
        print(f"Content Length: {len(res.content)}")
        try:
            df = pd.read_csv(io.StringIO(res.text))
            print("CSV Loaded Successfully")
            print(f"Columns: {df.columns.tolist()}")
            
            # Check for 2330
            df['證券代號'] = df['證券代號'].astype(str)
            tsmc = df[df['證券代號'] == '2330']
            if not tsmc.empty:
                print(f"Found 2330: {len(tsmc)} rows")
                print(tsmc.head())
            else:
                print("2330 NOT FOUND")
                # Print some sample codes
                print(f"Sample codes: {df['證券代號'].unique()[:10]}")
        except Exception as e:
            print(f"CSV Parse Error: {e}")
            print(f"Content Preview: {res.text[:500]}")
    else:
        print("Failed to download")

except Exception as e:
    print(f"Request Error: {e}")
