#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
雲端自動更新腳本 - 專為 GitHub Actions 設計
每日自動下載台股資料並上傳到 Supabase

使用方式：
    python cloud_update.py

環境變數 (GitHub Secrets)：
    SUPABASE_URL: Supabase 專案 URL
    SUPABASE_KEY: Supabase Service Role Key
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import math

# Supabase
try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except ImportError:
    print("❌ 請安裝 supabase: pip install supabase")
    HAS_SUPABASE = False

# ==============================
# 設定
# ==============================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://gqiyvefcldxslrqpqlri.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# 請求標頭
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ==============================
# 工具函數
# ==============================
def print_flush(msg: str):
    """即時輸出"""
    print(msg, flush=True)

def get_today_date_int() -> int:
    """取得今日日期 (整數格式 YYYYMMDD)"""
    return int(datetime.now().strftime("%Y%m%d"))

def safe_float(val, default=0.0) -> float:
    """安全轉換浮點數"""
    if val is None or val == "" or val == "--" or val == "N/A":
        return default
    try:
        return float(str(val).replace(",", ""))
    except:
        return default

def safe_int(val, default=0) -> int:
    """安全轉換整數"""
    if val is None or val == "" or val == "--" or val == "N/A":
        return default
    try:
        return int(float(str(val).replace(",", "")))
    except:
        return default

# ==============================
# 資料下載函數
# ==============================
def download_twse_stocks() -> List[Dict]:
    """下載 TWSE 上市股票清單"""
    print_flush("📥 下載上市股票清單...")
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        data = r.json()
        
        stocks = []
        if data.get("stat") == "OK" and data.get("data"):
            for row in data["data"]:
                code = str(row[0]).strip()
                name = str(row[1]).strip()
                
                # A規則：只保留普通股
                if not code.isdigit():
                    continue
                if len(code) != 4:
                    continue
                    
                stocks.append({
                    "code": code,
                    "name": name
                })
        
        print_flush(f"  ✓ 上市股票: {len(stocks)} 檔")
        return stocks
    except Exception as e:
        print_flush(f"  ❌ 下載失敗: {e}")
        return []

def download_tpex_stocks() -> List[Dict]:
    """下載 TPEX 上櫃股票清單"""
    print_flush("📥 下載上櫃股票清單...")
    # 使用基本資料 API
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        data = r.json()
        
        stocks = []
        for item in data:
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            name = str(item.get("CompanyName", "")).strip()
            
            # A規則
            if not code.isdigit():
                continue
            if len(code) != 4:
                continue
                
            stocks.append({
                "code": code,
                "name": name
            })
        
        print_flush(f"  ✓ 上櫃股票: {len(stocks)} 檔")
        return stocks
    except Exception as e:
        print_flush(f"  ❌ 下載失敗: {e}")

def download_twse_quotes(date_str: str) -> List[Dict]:
    """下載 TWSE 今日行情"""
    print_flush(f"📥 下載上市行情 ({date_str})...")
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {
        "response": "json",
        "date": date_str,
        "type": "ALLBUT0999"
    }
    
    try:
        time.sleep(3)  # 避免請求過快
        r = requests.get(url, params=params, headers=HEADERS, timeout=60)
        data = r.json()
        
        quotes = []
        if data.get("stat") == "OK":
            # 找到股價資料表
            tables = data.get("tables", [])
            for table in tables:
                if "證券代號" in str(table.get("fields", [])):
                    for row in table.get("data", []):
                        code = str(row[0]).strip()
                        if not code.isdigit() or len(code) != 4:
                            continue
                        
                        # 解析資料
                        date_int = int(date_str)
                        volume = safe_int(row[2])
                        open_price = safe_float(row[5])
                        high = safe_float(row[6])
                        low = safe_float(row[7])
                        close = safe_float(row[8])
                        change = safe_float(row[10])
                        
                        if close <= 0:
                            continue
                            
                        quotes.append({
                            "code": code,
                            "date_int": date_int,
                            "open": open_price,
                            "high": high,
                            "low": low,
                            "close": close,
                            "volume": volume
                        })
        
        print_flush(f"  ✓ 上市行情: {len(quotes)} 筆")
        return quotes
    except Exception as e:
        print_flush(f"  ❌ 下載失敗: {e}")
        return []

def download_tpex_quotes(date_str: str) -> List[Dict]:
    """下載 TPEX 今日行情"""
    # 轉換日期格式 YYYYMMDD -> YYY/MM/DD (民國年)
    year = int(date_str[:4]) - 1911
    month = date_str[4:6]
    day = date_str[6:8]
    roc_date = f"{year}/{month}/{day}"
    
    print_flush(f"📥 下載上櫃行情 ({date_str})...")
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
    params = {"l": "zh-tw", "d": roc_date, "o": "json"}
    
    try:
        time.sleep(3)
        r = requests.get(url, params=params, headers=HEADERS, timeout=60)
        data = r.json()
        
        quotes = []
        if data.get("aaData"):
            date_int = int(date_str)
            for row in data["aaData"]:
                code = str(row[0]).strip()
                if not code.isdigit() or len(code) != 4:
                    continue
                
                close = safe_float(row[2])
                change = safe_float(row[3])
                open_price = safe_float(row[4])
                high = safe_float(row[5])
                low = safe_float(row[6])
                volume = safe_int(row[7])
                
                if close <= 0:
                    continue
                    
                quotes.append({
                    "code": code,
                    "date_int": date_int,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume * 1000  # TPEX 單位是張
                })
        
        print_flush(f"  ✓ 上櫃行情: {len(quotes)} 筆")
        return quotes
    except Exception as e:
        print_flush(f"  ❌ 下載失敗: {e}")
        return []

def download_institutional(date_str: str) -> List[Dict]:
    """下載三大法人買賣超"""
    print_flush(f"📥 下載法人買賣超 ({date_str})...")
    
    # TWSE
    url = "https://www.twse.com.tw/fund/T86"
    params = {"response": "json", "date": date_str, "selectType": "ALLBUT0999"}
    
    data_list = []
    try:
        time.sleep(3)
        r = requests.get(url, params=params, headers=HEADERS, timeout=60)
        data = r.json()
        
        if data.get("stat") == "OK" and data.get("data"):
            date_int = int(date_str)
            for row in data["data"]:
                code = str(row[0]).strip()
                if not code.isdigit() or len(code) != 4:
                    continue
                
                # 外資、投信、自營商
                foreign_buy = safe_int(row[2])
                foreign_sell = safe_int(row[3])
                foreign_net = safe_int(row[4])
                trust_buy = safe_int(row[5])
                trust_sell = safe_int(row[6])
                trust_net = safe_int(row[7])
                dealer_net = safe_int(row[8])
                total_net = safe_int(row[11])
                
                data_list.append({
                    "code": code,
                    "date_int": date_int,
                    "foreign_buy": foreign_buy,
                    "foreign_sell": foreign_sell,
                    "foreign_net": foreign_net,
                    "trust_buy": trust_buy,
                    "trust_sell": trust_sell,
                    "trust_net": trust_net,
                    "dealer_net": dealer_net,
                    "total_net": total_net
                })
    except Exception as e:
        print_flush(f"  ⚠ TWSE 法人資料下載失敗: {e}")
    
    print_flush(f"  ✓ 法人買賣超: {len(data_list)} 筆")
    return data_list

# ==============================
# Supabase 上傳
# ==============================
def upload_to_supabase(supabase: Client, table: str, data: List[Dict], batch_size: int = 1000):
    """批次上傳到 Supabase"""
    if not data:
        print_flush(f"  ⚠ {table}: 無資料")
        return 0
    
    print_flush(f"📤 上傳 {table} ({len(data)} 筆)...")
    
    total_batches = math.ceil(len(data) / batch_size)
    success_count = 0
    
    for i in range(total_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, len(data))
        batch = data[start:end]
        
        try:
            supabase.table(table).upsert(batch).execute()
            success_count += len(batch)
            
            if (i + 1) % 5 == 0 or (i + 1) == total_batches:
                print_flush(f"  進度: {i + 1}/{total_batches} ({success_count}/{len(data)})")
        except Exception as e:
            print_flush(f"  ❌ Batch {i + 1} 失敗: {e}")
    
    print_flush(f"  ✓ {table}: {success_count}/{len(data)} 筆")
    return success_count

# ==============================
# 主程式
# ==============================
def main():
    """主程式"""
    print_flush("=" * 50)
    print_flush("🚀 雲端自動更新開始")
    print_flush(f"⏰ 執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_flush("=" * 50)
    
    # 檢查環境
    if not HAS_SUPABASE:
        sys.exit(1)
    
    if not SUPABASE_KEY:
        print_flush("❌ 缺少 SUPABASE_KEY 環境變數")
        sys.exit(1)
    
    # 初始化 Supabase
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print_flush("✓ Supabase 連線成功")
    except Exception as e:
        print_flush(f"❌ Supabase 連線失敗: {e}")
        sys.exit(1)
    
    # 取得今日日期
    today = datetime.now()
    # 如果是週末，使用上週五
    if today.weekday() == 5:  # 週六
        today = today - timedelta(days=1)
    elif today.weekday() == 6:  # 週日
        today = today - timedelta(days=2)
    
    date_str = today.strftime("%Y%m%d")
    print_flush(f"📅 更新日期: {date_str}")
    
    # Step 1: 下載股票清單
    print_flush("\n[Step 1] 下載股票清單")
    twse_stocks = download_twse_stocks()
    tpex_stocks = download_tpex_stocks()
    all_stocks = twse_stocks + tpex_stocks
    
    # 暫時跳過 stock_data 上傳 (表格結構不相容)
    # if all_stocks:
    #     upload_to_supabase(supabase, "stock_data", all_stocks)
    print_flush(f"  ⚠ 跳過 stock_data 上傳 (表格結構不相容)")
    
    # Step 2: 下載今日行情
    print_flush("\n[Step 2] 下載今日行情")
    twse_quotes = download_twse_quotes(date_str)
    tpex_quotes = download_tpex_quotes(date_str)
    all_quotes = twse_quotes + tpex_quotes
    
    if all_quotes:
        upload_to_supabase(supabase, "stock_history", all_quotes)
    
    # Step 3: 下載法人買賣超
    print_flush("\n[Step 3] 下載法人買賣超")
    institutional = download_institutional(date_str)
    
    if institutional:
        upload_to_supabase(supabase, "institutional_investors", institutional)
    
    # Step 4: 更新同步時間
    print_flush("\n[Step 4] 更新同步時間")
    try:
        sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        supabase.table("sync_status").upsert({
            "id": 1,
            "last_update": sync_time,
            "status": "completed"
        }).execute()
        print_flush(f"✓ 同步時間已更新: {sync_time}")
    except Exception as e:
        print_flush(f"⚠ 更新同步時間失敗: {e}")
    
    # 完成
    print_flush("\n" + "=" * 50)
    print_flush("✅ 雲端自動更新完成!")
    print_flush(f"📊 股票: {len(all_stocks)} 檔")
    print_flush(f"📈 行情: {len(all_quotes)} 筆")
    print_flush(f"🏛️ 法人: {len(institutional)} 筆")
    print_flush("=" * 50)

if __name__ == "__main__":
    main()
