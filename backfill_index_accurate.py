import requests
import sqlite3
import time
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_twse_data(api_name, date_str):
    url = f"https://www.twse.com.tw/exchangeReport/{api_name}?response=json&date={date_str}"
    if api_name == "MI_5MINS_HIST":
        url = f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={date_str}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=10)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print(f"Error fetching {api_name} for {date_str}: {e}")
    return None

def backfill_index():
    conn = sqlite3.connect("taiwan_stock.db")
    cur = conn.cursor()

    # Start from Dec 1, 2025
    start_date = datetime(2025, 12, 1)
    current_date = start_date

    while current_date <= datetime.now():
        # Always use the 1st of the month for TWSE API to get full month data
        date_str = current_date.strftime("%Y%m01")
        print(f"Processing month: {current_date.strftime('%Y/%m')}")

        ohlc_json = get_twse_data("MI_5MINS_HIST", date_str)
        summary_json = get_twse_data("FMTQIK", date_str)

        print(f"OHLC Stat: {ohlc_json.get('stat') if ohlc_json else 'None'}")
        print(f"Summary Stat: {summary_json.get('stat') if summary_json else 'None'}")

        if ohlc_json and ohlc_json.get("stat") == "OK" and summary_json and summary_json.get("stat") == "OK":
            ohlc_data = ohlc_json.get("data", [])
            summary_data = summary_json.get("data", [])

            # Create maps for merging
            # OHLC row: [日期, 開盤指數, 最高指數, 最低指數, 收盤指數]
            # Summary row: [日期, 成交股數, 成交金額, 成交筆數, 發行量加權股價指數, 漲跌點數]
            
            summary_map = {}
            for row in summary_data:
                # Date format in JSON is "112/01/01"
                d_parts = row[0].split('/')
                y = int(d_parts[0]) + 1911
                m = d_parts[1]
                d = d_parts[2]
                key = f"{y}{m}{d}"
                summary_map[key] = {
                    "volume": int(row[1].replace(',', '')),
                    "amount": int(row[2].replace(',', ''))
                }

            for row in ohlc_data:
                d_parts = row[0].split('/')
                y = int(d_parts[0]) + 1911
                m = d_parts[1]
                d = d_parts[2]
                key = f"{y}{m}{d}"
                
                if key in summary_map:
                    date_int = int(key)
                    open_p = float(row[1].replace(',', ''))
                    high_p = float(row[2].replace(',', ''))
                    low_p = float(row[3].replace(',', ''))
                    close_p = float(row[4].replace(',', ''))
                    vol = summary_map[key]["volume"]
                    amt = summary_map[key]["amount"]

                    print(f"Inserting {key}: {close_p}, {vol}, {amt}")
                    cur.execute("""
                        INSERT OR REPLACE INTO stock_history (code, date_int, open, high, low, close, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, ("0000", date_int, open_p, high_p, low_p, close_p, vol, amt))

            conn.commit()
            print(f"Updated {len(ohlc_data)} days for {current_date.strftime('%Y/%m')}")
        else:
            print(f"Failed to get data for {current_date.strftime('%Y/%m')}")

        # Move to next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
        
        time.sleep(2) # Avoid rate limit

    conn.close()

if __name__ == "__main__":
    backfill_index()
