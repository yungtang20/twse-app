import requests
import pandas as pd
import io
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
print("Downloading CSV...")
try:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text))
    df['證券代號'] = df['證券代號'].astype(str).str.strip()
    df_2330 = df[df['證券代號'] == '2330']
    with open('tdcc_2330.txt', 'w', encoding='utf-8') as f:
        f.write(df_2330[['持股分級', '人數', '股數', '占集保庫存數比例%']].to_string(index=False))
    print("Saved to tdcc_2330.txt")



except Exception as e:
    print(f"Error: {e}")
