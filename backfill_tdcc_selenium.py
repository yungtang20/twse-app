#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDCC Backfill Script (Selenium Version)
Target: Goodinfo.tw (EquityDistributionClassHis.asp)
Database: taiwan_stock.db (Table: stock_shareholding_all)
"""

import os
import time
import random
import sqlite3
import pandas as pd
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Database Config
DB_PATH = 'taiwan_stock.db'

# A-Rule Filtering Config
STOCK_LIST_CSV = 'stock_list.csv'

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def init_driver():
    """Initialize Selenium WebDriver"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    # Random User-Agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')
    
    # Disable automation flags
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    try:
        # Try using webdriver-manager first (PC/Standard)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"webdriver-manager failed: {e}")
        print("Attempting to use system chromedriver (Termux/Mobile)...")
        try:
            # Fallback to system chromedriver (Termux: pkg install chromium)
            # Usually in PATH, so no service needed or default service
            driver = webdriver.Chrome(options=options)
            return driver
        except Exception as e2:
            print(f"Failed to initialize system driver: {e2}")
            raise e

def get_a_rule_stocks():
    """
    Get list of stocks matching A-Rule:
    - Market: TWSE, TPEX
    - Exclude: ETF (00xx), Warrants, DR, ETN, Bonds, Preferred, Innovation Board
    """
    if not os.path.exists(STOCK_LIST_CSV):
        print(f"Error: {STOCK_LIST_CSV} not found.")
        return []

    df = pd.read_csv(STOCK_LIST_CSV, dtype={'code': str})
    
    # Filter Market
    df = df[df['market'].isin(['TWSE', 'TPEX'])]
    
    valid_stocks = []
    for _, row in df.iterrows():
        code = row['code']
        name = row['name']
        
        # Exclude 00xx (ETF)
        if code.startswith('00'):
            continue
            
        # Exclude 4-digit checks (Warrants usually 6, but just in case)
        if len(code) != 4:
            continue
            
        # Exclude by Name keywords
        if any(x in name for x in ['-DR', '-創', '特', '債']):
            continue
            
        valid_stocks.append(code)
        
    print(f"Total A-Rule Stocks: {len(valid_stocks)}")
    return valid_stocks

def parse_goodinfo_html(html, stock_id):
    """Parse Goodinfo HTML to extract shareholding distribution"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find the table
    table = soup.find('table', {'id': 'tblDetail'})
    if not table:
        return None
            
    # Use Pandas for easier parsing
    try:
        # Fix FutureWarning by wrapping html in StringIO
        dfs = pd.read_html(StringIO(str(table)))
        if not dfs:
            return None
        
        df = dfs[0]
        # Clean up multi-level columns if any
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() for col in df.columns.values]
            
        # Identify Date column
        date_col = next((c for c in df.columns if '日期' in c), None)
        if not date_col:
            return None
            
        # Level mapping (Flattened column names usually end with the level range)
        # We need to match substrings in the column names
        level_map = {
            '1-999': 1, '1張以下': 1, '1 999': 1, '＜1張': 1,
            '1-5': 2, '1 5張': 2, '≧1張 ≦5張': 2,
            '5-10': 3, '5 10張': 3, '＞5張 ≦10張': 3,
            '10-15': 4, '10 15張': 4, '＞10張 ≦15張': 4,
            '15-20': 5, '15 20張': 5, '＞15張 ≦20張': 5,
            '20-30': 6, '20 30張': 6, '＞20張 ≦30張': 6,
            '30-40': 7, '30 40張': 7, '＞30張 ≦40張': 7,
            '40-50': 8, '40 50張': 8, '＞40張 ≦50張': 8,
            '50-100': 9, '50 100張': 9, '＞50張 ≦100張': 9,
            '100-200': 10, '100 200張': 10, '＞100張 ≦200張': 10,
            '200-400': 11, '200 400張': 11, '＞200張 ≦400張': 11,
            '400-600': 12, '400 600張': 12, '＞400張 ≦600張': 12,
            '600-800': 13, '600 800張': 13, '＞600張 ≦800張': 13,
            '800-1000': 14, '800 1千張': 14, '800 1000張': 14, '＞800張 ≦1千張': 14,
            '1000-': 15, '1000張以上': 15, '1,000張以上': 15, '1千張以上': 15, '＞1千張': 15,
            '合計': 17, 'Total': 17
        }
        
        records = []
        for _, row in df.iterrows():
            date_val = row[date_col]
            if not isinstance(date_val, str) or '/' not in date_val:
                continue
                
            date_int = int(date_val.replace('/', ''))
            
            for level_name, level_id in level_map.items():
                holders = 0
                proportion = 0.0
                shares = 0
                
                # Find matching columns
                # Column names are flattened, e.g., "各持股等級股東之持有比例(%)_1-5張"
                # We search for the level_name in the column string
                cols_for_level = [c for c in df.columns if level_name in str(c)]
                
                if not cols_for_level:
                    continue
                    
                for c in cols_for_level:
                    val = row[c]
                    if isinstance(val, str):
                        val = val.replace(',', '').replace('%', '').strip()
                        if val == '-': val = 0
                    try:
                        val_num = float(val)
                    except:
                        val_num = 0
                        
                    # Guess type based on column name prefix/context
                    # In "持有比例" view, values are usually proportion
                    # But sometimes it includes holders?
                    # Goodinfo "持有比例區間分級一覽" usually ONLY has proportion columns.
                    # So we assume it's proportion.
                    proportion = float(val_num)
                    
                # If we only have proportion, we save it. Holders/Shares will be 0.
                if proportion > 0:
                    records.append((stock_id, date_int, level_id, holders, shares, proportion))

        return records

    except Exception as e:
        print(f"Error parsing table: {e}")
        return None

def save_to_db(records):
    """Insert records into DB"""
    if not records:
        return
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.executemany("""
            INSERT OR REPLACE INTO stock_shareholding_all 
            (code, date_int, level, holders, shares, proportion)
            VALUES (?, ?, ?, ?, ?, ?)
        """, records)
        conn.commit()
        print(f"Saved {len(records)} records.")
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        conn.close()

def get_processed_stocks():
    """Read processed stocks from log file"""
    if not os.path.exists("processed_stocks.txt"):
        return set()
    with open("processed_stocks.txt", "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def mark_as_processed(stock_id):
    """Append stock_id to processed log"""
    with open("processed_stocks.txt", "a", encoding="utf-8") as f:
        f.write(f"{stock_id}\n")

def main():
    all_stocks = get_a_rule_stocks()
    processed = get_processed_stocks()
    
    # Filter out processed stocks
    stocks = [s for s in all_stocks if s not in processed]
    
    # Optional: Still keep the start_stock logic if user wants to skip ahead manually, 
    # but the processed list should handle it. 
    # Let's keep the start logic but prioritize the processed list filter.
    
    # Filter to start from 4107 (or next available) if needed, but mainly rely on processed list
    # If we want to strictly follow "delete updated ones", we just use the filtered list.
    # But let's also respect the "start from" if provided, to be safe.
    start_stock = '4155' # Update to next target
    start_idx = -1
    for i, s in enumerate(stocks):
        if s >= start_stock:
            start_idx = i
            break
            
    if start_idx != -1:
        stocks = stocks[start_idx:]
        print(f"Starting from {stocks[0]} (requested {start_stock}), {len(stocks)} stocks remaining.")
    else:
        print(f"No stocks found >= {start_stock}. Processing all remaining {len(stocks)} stocks.")

    print(f"Target Stocks: {len(stocks)}")
    
    driver = init_driver()
    
    try:
        for i, stock_id in enumerate(stocks):
            print(f"[{i+1}/{len(stocks)}] Processing {stock_id}...")
            
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    url = f"https://goodinfo.tw/tw/EquityDistributionClassHis.asp?STOCK_ID={stock_id}"
                    driver.get(url)
                    
                    # Wait for table
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.ID, "tblDetail"))
                    )
                    
                    # Switch to "Complete" view (15 levels)
                    from selenium.webdriver.support.ui import Select
                    select = Select(driver.find_element(By.ID, "selSheet"))
                    select.select_by_value("持有比例區間分級一覽(完整)")
                    
                    # Wait for table to refresh
                    time.sleep(3) 
                    
                    html = driver.page_source
                    records = parse_goodinfo_html(html, stock_id)
                    
                    if records:
                        save_to_db(records)
                        mark_as_processed(stock_id)
                        break # Success, exit retry loop
                    else:
                        print(f"No data found for {stock_id}")
                        with open(f"debug_failed_{stock_id}.html", "w", encoding="utf-8") as f:
                            f.write(html)
                        print(f"Saved debug_failed_{stock_id}.html")
                        break # No data, don't retry unless it's a network error
                        
                except Exception as e:
                    print(f"Error processing {stock_id} (Attempt {attempt+1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(5)
                        # Re-init driver if it seems dead?
                        # driver = init_driver() 
                    else:
                        print(f"Failed to process {stock_id} after retries.")
            
            time.sleep(random.uniform(10, 20))
            
            # Long sleep every 10 stocks to avoid rate limiting
            if (i + 1) % 10 == 0:
                print("Taking a long break (60s)...")
                time.sleep(60)
            
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
