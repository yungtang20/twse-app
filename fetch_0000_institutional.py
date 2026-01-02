
import requests
import time
import datetime
import sys
import os
import urllib3
from pathlib import Path

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.services.db import db_manager

def fetch_taiex_prices(year, month):
    date_str = f"{year}{month:02d}01"
    url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date_str}"
    print(f"Fetching TAIEX prices for {year}/{month}...")
    try:
        res = requests.get(url, verify=False)
        data = res.json()
        if data['stat'] != 'OK':
            print(f"  No price data: {data['stat']}")
            return {}
        
        # Parse data
        # Fields: ["日期", "成交股數", "成交金額", "成交筆數", "發行量加權股價指數", "漲跌點數"]
        prices = {}
        for row in data['data']:
            # Date format: "113/01/01" (ROC year)
            d_parts = row[0].split('/')
            y = int(d_parts[0]) + 1911
            m = int(d_parts[1])
            d = int(d_parts[2])
            date_int = y * 10000 + m * 100 + d
            
            vol = int(row[1].replace(',', ''))
            amt = int(row[2].replace(',', ''))
            close = float(row[4].replace(',', ''))
            change = float(row[5].replace(',', ''))
            # Estimate open/high/low as close (since we only get close/change here, or use simple logic)
            # Actually FMTQIK only gives Close.
            # For better OHLC, we need indicesReport/MI_5MINS_HIST or similar, but that's too granular.
            # Or assume Open=High=Low=Close for Index if detailed data unavailable, or just set Close.
            # Let's set Open=High=Low=Close to avoid 0.
            # Change is available, so PrevClose = Close - Change.
            # We can't easily guess High/Low without other data.
            # But for now, setting all to Close is better than 0.
            
            prices[date_int] = {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": vol,
                "amount": amt
            }
        return prices
    except Exception as e:
        print(f"  Error fetching prices: {e}")
        return {}

def fetch_and_sync_0000(days=60):
    print(f"Fetching TAIEX (0000) data for last {days} days...")
    
    today = datetime.date.today()
    
    # Fetch prices for relevant months
    price_map = {}
    months_fetched = set()
    
    for i in range(days):
        d = today - datetime.timedelta(days=i)
        ym = (d.year, d.month)
        if ym not in months_fetched:
            p = fetch_taiex_prices(d.year, d.month)
            price_map.update(p)
            months_fetched.add(ym)
            time.sleep(2)

    for i in range(days):
        date = today - datetime.timedelta(days=i)
        date_str = date.strftime("%Y%m%d")
        date_int = int(date_str)
        
        print(f"Processing {date_str}...")
        
        # Institutional Data
        url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date_str}&type=day"
        
        try:
            res = requests.get(url, verify=False)
            data = res.json()
            
            dealer_buy = dealer_sell = dealer_net = 0
            trust_buy = trust_sell = trust_net = 0
            foreign_buy = foreign_sell = foreign_net = 0
            has_inst_data = False
            
            if data['stat'] == 'OK':
                has_inst_data = True
                for row in data['data']:
                    name = row[0].strip()
                    buy = int(row[1].replace(',', ''))
                    sell = int(row[2].replace(',', ''))
                    net = int(row[3].replace(',', ''))
                    
                    if '自營商' in name:
                        dealer_buy += buy
                        dealer_sell += sell
                        dealer_net += net
                    elif '投信' in name:
                        trust_buy += buy
                        trust_sell += sell
                        trust_net += net
                    elif '外資' in name:
                        foreign_buy += buy
                        foreign_sell += sell
                        foreign_net += net
            else:
                print(f"  No institutional data: {data['stat']}")

            # Prepare record
            # Get price data if available
            price_data = price_map.get(date_int, {})
            
            if not price_data and not has_inst_data:
                print("  No data found at all.")
                continue

            record = {
                "code": "0000",
                "date_int": date_int,
                "foreign_buy": foreign_net,
                "trust_buy": trust_net,
                "dealer_buy": dealer_net,
                "date": date.strftime("%Y-%m-%d")
            }
            # Merge price data
            if price_data:
                record.update(price_data)
            
            # Upsert to Supabase (stock_history)
            if db_manager.supabase:
                try:
                    # Use upsert now since we have price data (or at least partial)
                    db_manager.supabase.table("stock_history").upsert(record).execute()
                    print(f"  Upserted Supabase stock_history: {date_str}")
                except Exception as e:
                    print(f"  Supabase error: {e}")
            
            # Upsert to SQLite (if local)
            if not db_manager.is_cloud_mode:
                try:
                    cols = ', '.join(record.keys())
                    placeholders = ', '.join(['?'] * len(record))
                    values = tuple(record.values())
                    sql = f"INSERT OR REPLACE INTO stock_history ({cols}) VALUES ({placeholders})"
                    db_manager.execute_update(sql, values)
                    print(f"  Upserted SQLite stock_history: {date_str}")
                except Exception as e:
                    print(f"  SQLite error: {e}")
                    
        except Exception as e:
            print(f"  Error processing {date_str}: {e}")
        
        time.sleep(2)

if __name__ == "__main__":
    fetch_and_sync_0000()
