"""
直接從 TDCC 網站下載 CSV 格式的集保資料
"""

import requests
import sqlite3
import csv
import io
import time
from datetime import datetime, timedelta
import urllib3

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_tdcc_csv_direct():
    """
    從 TDCC 直接下載最新的 CSV 資料
    """
    # TDCC 開放資料下載連結 (每週更新)
    url = "https://www.tdcc.com.tw/smWeb/QryStockAmt.jsp"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }
    
    try:
        res = requests.get(url, headers=headers, verify=False, timeout=60)
        res.raise_for_status()
        print(f"Response status: {res.status_code}")
        print(f"Response length: {len(res.text)}")
        if len(res.text) > 500:
            print(f"First 500 chars: {res.text[:500]}")
        return res.text
    except Exception as e:
        print(f"Error: {e}")
        return None

def fetch_tdcc_week_data(roc_date):
    """
    從 TDCC 抓取特定週的資料
    roc_date: 民國年格式，如 1131220
    """
    url = "https://www.tdcc.com.tw/smWeb/QryStock.jsp"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    
    # 民國年轉西元年
    # 1131220 -> 2024/12/20
    
    data = {
        "REession": "",
        "clession": "",
        "SqlMethod": "StockNo",
        "StockNo": "",
        "scaDates": roc_date,
    }
    
    try:
        res = requests.post(url, headers=headers, data=data, verify=False, timeout=60)
        res.raise_for_status()
        return res.text
    except Exception as e:
        print(f"Error fetching TDCC for {roc_date}: {e}")
        return None

def download_sample_tdcc():
    """
    下載並檢查 TDCC 資料的格式
    """
    print("=== 測試 TDCC 資料下載 ===")
    
    # 嘗試用不同的方式
    test_url = "https://www.tdcc.com.tw/smWeb/QryStock.jsp"
    
    print(f"\n1. 測試首頁...")
    html = fetch_tdcc_csv_direct()
    if html:
        print("OK - 可以存取 TDCC 網站")
    
    print(f"\n2. 測試 API 端點 (需要 POST)...")
    # 使用民國年格式: 1131219 = 2024/12/19
    today = datetime.now()
    roc_year = today.year - 1911
    roc_date = f"{roc_year}{today.month:02d}{today.day:02d}"
    print(f"民國年日期: {roc_date}")
    
    result = fetch_tdcc_week_data(roc_date)
    if result:
        print(f"Response length: {len(result)}")
        # 解析 HTML 中的資料
        if "證券代號" in result:
            print("找到資料表格！")
        else:
            print("沒有找到資料表格")
            print(f"First 1000 chars: {result[:1000] if result else 'None'}")

if __name__ == "__main__":
    download_sample_tdcc()
