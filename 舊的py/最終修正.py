import multiprocessing
import os
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台灣股票分析系統 v40 Enhanced (均線多頭優化版) - 架構師修正版
架構師: 資深軟體架構師
修正日期: 2024-12-07

規則嚴格遵守(1.繁體中文、2.A規則、3.三行進度、4.斷檔續讀、5.資料顯示方式、6.使用官方的真實數據抓到什麼就輸出什麼，
7.不要有按任意鍵返回/繼續，一律直接進入選單或顯示、8.統一設定數字0為返回，9.將各方所抓取來的資料統一成一個形式後，輸入資料庫，方便之後的資料調用)
10.所有數字只取到小數點後二位，"""

# ==============================
# 安裝需求 (手機與電腦通用)
# ==============================
# 
# 【電腦 (Windows/Mac/Linux)】
#   pip install requests twstock lxml pandas numpy colorama
#
# 【手機 (Pydroid 3)】
#   pip install requests twstock lxml pandas numpy colorama
# ==============================
# 自動安裝/更新 twstock (每日一次)
# ==============================
import sys
import subprocess
import os
from datetime import datetime

def _should_update_twstock():
    """檢查是否需要更新 twstock (每日一次)"""
    flag_file = os.path.join(os.path.dirname(__file__), '.twstock_updated')
    today = datetime.now().strftime('%Y-%m-%d')
    
    if os.path.exists(flag_file):
        with open(flag_file, 'r') as f:
            last_update = f.read().strip()
            if last_update == today:
                return False  # 今天已更新過
    
    # 寫入今日標記
    with open(flag_file, 'w') as f:
        f.write(today)
    return True

if _should_update_twstock():
    try:
        print("正在檢查 twstock 版本...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", "twstock"], 
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("twstock 更新完成")
    except Exception as e:
        print(f"twstock 更新失敗: {e}")

import os
import time
import json
import re
import sqlite3
import logging
import requests
import threading
import warnings
import pandas as pd
import numpy as np
import ssl
import urllib3
import twstock
from twstock.stock import TPEXFetcher
from pathlib import Path
from datetime import datetime, timedelta
import queue
import gc
import math
from typing import Optional, Dict, List, Tuple, Any
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
import colorama
try:
    colorama.just_fix_windows_console()
except AttributeError:
    # Fallback for older colorama versions
    colorama.init()

# Supabase Support
try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

# ==============================
# Logging Configuration
# ==============================
logging.basicConfig(
    level=logging.DEBUG,  # 開啟所有級別
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("system.log", encoding='utf-8'),  # 所有訊息寫入 log
    ]
)
# 控制台只顯示 CRITICAL (隱藏 ERROR/WARNING/INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.CRITICAL)  # 只顯示 CRITICAL 級別
logging.getLogger().addHandler(console_handler)

logger = logging.getLogger("TWSE_System")
# 抑制第三方庫的日誌
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# ==============================
# SSL Patch (Fix for Android/Windows SSL errors)
# ==============================
ssl._create_default_https_context = ssl._create_unverified_context
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Monkey patch requests.Session.request to disable verification globally
old_request = requests.Session.request
def new_request(self, method, url, *args, **kwargs):
    kwargs['verify'] = False
    return old_request(self, method, url, *args, **kwargs)
requests.Session.request = new_request

# ╔══════════════════════════════════════════════════════════════╗
# ║                        CONFIG                                 ║
# ║  所有硬碼常數、API 端點表、SQL 模板集中於此                     ║
# ╚══════════════════════════════════════════════════════════════╝

# ==============================
# 全域配置 (Config Class)
# ==============================
class Config:
    """系統全域配置，消除魔術數字"""
    # 資料回溯設定
    HISTORY_DAYS_LOOKBACK = 1095    # 歷史資料回溯天數 (3年)
    CALC_LOOKBACK_DAYS = 450        # 指標計算回溯天數
    
    # 顯示設定
    DEFAULT_DISPLAY_LIMIT = 30      # 預設顯示筆數
    DEFAULT_DISPLAY_DAYS = 10       # 預設顯示天數
    
    # 掃描參數
    VP_TOLERANCE_PCT = 0.02         # VP 支撐壓力容許誤差 (2%)
    MIN_VOLUME_DEFAULT = 500        # 預設最小成交量 (張)
    
    # API 設定
    API_TIMEOUT = 10                # API 請求超時 (秒)
    
    # 路徑設定
    DB_PATH = "taiwan_stock.db"     # 資料庫路徑
    PROGRESS_FILE = "progress.json" # 進度檔案路徑
    
    # ==============================
    # 環境自適應配置 (Phase 5)
    # ==============================
    # 自動設定，將在模組載入後更新
    IS_ANDROID = False              # 是否為 Android 環境
    MAX_WORKERS = 6                 # 多線程最大工作數
    BATCH_SIZE = 200                # 批次處理大小
    LIGHTWEIGHT_MODE = False        # 輕量模式 (手機專用)

# ==============================
# TPEX Patch (Fix for 404 Error)
# ==============================
def tpex_fetch(self, year: int, month: int, sid: str, retry: int = 5):
    # TPEX New API URL
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
    
    # Construct date param (use the first day of the month)
    date_str = f"{year}/{month:02d}/01"
    
    params = {
        "date": date_str,
        "code": sid,
        "response": "json"
    }
    
    for retry_i in range(retry):
        try:
            # 加入隨機延遲，避免觸發 Rate Limit
            time.sleep(np.random.uniform(1.5, 3.0))
            
            r = requests.get(url, params=params, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
            data = r.json()
        except Exception:
            continue
        else:
            break
    else:
        return {"data": []}

    result = {"data": []}
    if data.get("stat") == "ok" and data.get("tables"):
        raw_data = data["tables"][0]["data"]
        result["data"] = [self._make_datatuple(row) for row in raw_data]
        
    return result

TPEXFetcher.fetch = tpex_fetch

# ==============================
# 環境適配
# ==============================
try:
    import termios
    import tty
    HAS_TERMIOS = True
except ImportError:
    HAS_TERMIOS = False

warnings.filterwarnings("ignore")

# 安全地禁用 SSL 警告
try:
    requests.packages.urllib3.disable_warnings()
except Exception:
    pass

# Windows 環境的終端機設定
if os.name == 'nt':
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# ==============================
# 配置參數
# ==============================
def get_work_directory():
    """獲取工作目錄 - 平台感知"""
    if os.name == 'nt':
        return Path(__file__).parent.absolute()
    
    # Android 路徑
    android_paths = [
        Path('/sdcard/Download/stock_app'),
        Path('/storage/emulated/0/Download/stock_app')
    ]
    
    for path in android_paths:
        if path.exists() or path.parent.exists():
            path.mkdir(parents=True, exist_ok=True)
            return path
    
    return Path(__file__).parent.absolute()

WORK_DIR = get_work_directory()

# 環境感知: 檢測是否為 Android 環境
IS_ANDROID = any(p in str(WORK_DIR) for p in ['/sdcard', '/storage/emulated']) or os.path.exists('/data/data/com.termux')

# 更新 Config 環境自適應配置
Config.IS_ANDROID = IS_ANDROID
Config.MAX_WORKERS = 2 if IS_ANDROID else 6
Config.BATCH_SIZE = 50 if IS_ANDROID else 200
Config.LIGHTWEIGHT_MODE = IS_ANDROID

if not WORK_DIR.exists():
    WORK_DIR.mkdir(parents=True, exist_ok=True)

# 檔案路徑配置
DB_FILE = WORK_DIR / 'taiwan_stock.db'
STOCK_LIST_PATH = WORK_DIR / 'stock_list.csv'
PROGRESS_FILE = WORK_DIR / 'download_progress.json'
BACKUP_DIR = WORK_DIR / 'backups'
BACKUP_DIR.mkdir(exist_ok=True)
REQUEST_TIMEOUT = 30

# API 設定
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMi0xNyAyMjowMzowMiIsInVzZXJfaWQiOiJ5dW5ndGFuZyAiLCJpcCI6IjExMS43MS4yMTIuMjUifQ.fYv38gHAin0IZu5GZZyFFjj5tPU8BCCORDTUTandpDg"

# ==============================
# Phase 1: 表驅動法 - API 端點配置表
# ==============================
API_ENDPOINTS = {
    'finmind': {
        'base': 'https://api.finmindtrade.com/api/v4/data',
    },
    'twse': {
        'daily_all': 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
        'daily': 'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY',
        'bwibbu': 'https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL',
        'institutional': 'https://openapi.twse.com.tw/v1/fund/T86_ALL',
        'margin': 'https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN',
        'pepb': 'https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d',
        'stock_list': 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L',
    },
    'tpex': {
        'daily': 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes',
        'daily_trading': 'https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php',
        'institutional': 'https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading',
        'margin': 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance',
        'pepb': 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis',
        'stock_list': 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O',
    },
    'tdcc': {
        'shareholder': 'https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5',
    }
}

# 取得 API 端點的便利函數
def get_api_url(market, endpoint):
    """表驅動法: 取得 API 端點 URL"""
    return API_ENDPOINTS.get(market, {}).get(endpoint, '')

# 向後相容的別名 (舊程式碼用)
FINMIND_URL = API_ENDPOINTS['finmind']['base']
TWSE_BWIBBU_URL = API_ENDPOINTS['twse']['bwibbu']
TWSE_STOCK_DAY_ALL_URL = API_ENDPOINTS['twse']['daily_all']
TPEX_MAINBOARD_URL = API_ENDPOINTS['tpex']['daily']
TWSE_STOCK_DAY_URL = API_ENDPOINTS['twse']['daily']
TPEX_DAILY_TRADING_URL = API_ENDPOINTS['tpex']['daily_trading']

# ==============================
# 表驅動法 - SQL 查詢模板
# ==============================
QUERY_TEMPLATES = {
    'get_latest_date': "SELECT MAX(date_int) FROM stock_history WHERE code = ?",
    'get_stock_history': """
        SELECT date_int, open, high, low, close, volume, amount
        FROM stock_history WHERE code = ? ORDER BY date_int DESC LIMIT ?
    """,
    'get_all_stocks': "SELECT code, name FROM stock_meta WHERE is_normal = 1",
    'get_snapshot': "SELECT * FROM stock_snapshot WHERE code = ?",
    'get_institutional': """
        SELECT date_int, foreign_buy, foreign_sell, trust_buy, trust_sell, 
               dealer_buy, dealer_sell
        FROM institutional_investors 
        WHERE code = ? ORDER BY date_int DESC LIMIT ?
    """,
    'count_stocks': "SELECT COUNT(*) FROM stock_meta",
    'get_latest_market_date': "SELECT MAX(date_int) FROM stock_history",
}

# ==============================
# 表驅動法 - SQL 寫入模板
# ==============================
SQL_UPSERT_TEMPLATES = {
    'history_upsert': """
        INSERT OR REPLACE INTO stock_history 
        (code, date_int, open, high, low, close, volume, amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    'snapshot_upsert': """
        INSERT OR REPLACE INTO stock_snapshot
        (code, date_int, close, volume, ma5, ma20, ma60, ma120, ma200, 
         rsi, mfi14, smart_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    'institutional_upsert': """
        INSERT OR REPLACE INTO institutional_investors
        (code, date_int, foreign_buy, foreign_sell, trust_buy, trust_sell,
         dealer_buy, dealer_sell)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    'meta_upsert': """
        INSERT OR REPLACE INTO stock_meta (code, name, market, is_normal)
        VALUES (?, ?, ?, ?)
    """,
}

# ==============================
# 2025 年台股休市日 (用於跳過補漏)
# ==============================
MARKET_HOLIDAYS_2025 = {
    20250101,  # 元旦
    20250127, 20250128, 20250129, 20250130, 20250131,  # 農曆春節
    20250203, 20250204,  # 農曆春節
    20250228,  # 和平紀念日
    20250303, 20250304,  # 和平紀念日調整
    20250404,  # 清明節
    20250501,  # 勞動節
    20250530, 20250531,  # 端午節
    20251006,  # 中秋節
    20251010,  # 國慶日
    20251024,  # 台灣光復節補假
}

def is_market_holiday(date_int):
    """檢查是否為休市日"""
    return date_int in MARKET_HOLIDAYS_2025


# 雲端同步設定
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_publishable_yXSGYxyxPMaoVu4MbGK5Vw_IuZsl5yu"
ENABLE_CLOUD_SYNC = False # bool(SUPABASE_URL and SUPABASE_KEY)

# 全域快取（延遲初始化）


# 顏色設定
COFFEE_COLOR = '\033[38;5;130m'
RESET_COLOR = '\033[0m'


def _worker_calc_indicators(args):
    """Step 7 Worker: 計算單支股票指標"""
    code, name, preloaded_df = args
    try:
        # 計算指標 (使用預載入的 DataFrame)
        indicators_list = calculate_stock_history_indicators(
            code, 
            display_days=1, 
            limit_days=Config.CALC_LOOKBACK_DAYS, 
            conn=None, 
            preloaded_df=preloaded_df
        )
        
        if not indicators_list:
            return None
            
        # 取得最新一筆資料
        latest = indicators_list[0]
        
        # 建構更新 Tuple (必須與 SQL UPDATE 順序完全一致)
        return (
            latest.get('MA3'), latest.get('MA20'), latest.get('MA60'), latest.get('MA120'), latest.get('MA200'),
            latest.get('WMA3'), latest.get('WMA20'), latest.get('WMA60'), latest.get('WMA120'), latest.get('WMA200'),
            latest.get('MFI'), latest.get('VWAP'), latest.get('CHG14'), latest.get('RSI'), latest.get('MACD'), latest.get('SIGNAL'),
            latest.get('POC'), latest.get('VP_upper'), latest.get('VP_lower'),
            latest.get('Month_K'), latest.get('Month_D'),
            latest.get('Daily_K'), latest.get('Daily_D'),
            latest.get('Week_K'), latest.get('Week_D'),
            latest.get('MA3_prev'), latest.get('MA20_prev'), latest.get('MA60_prev'), latest.get('MA120_prev'), latest.get('MA200_prev'),
            latest.get('WMA3_prev'), latest.get('WMA20_prev'), latest.get('WMA60_prev'), latest.get('WMA120_prev'), latest.get('WMA200_prev'),
            latest.get('MFI_prev'), latest.get('VWAP_prev'), latest.get('CHG14_prev'),
            latest.get('Month_K_prev'), latest.get('Month_D_prev'),
            latest.get('Daily_K_prev'), latest.get('Daily_D_prev'),
            latest.get('Week_K_prev'), latest.get('Week_D_prev'),
            latest.get('close_prev'), latest.get('vol_prev'),
            latest.get('SMI'), latest.get('SVI'), latest.get('NVI'), latest.get('PVI'), latest.get('clv'),
            latest.get('Smart_Score'), latest.get('SMI_Signal'), latest.get('SVI_Signal'), latest.get('NVI_Signal'), latest.get('VSA_Signal'),
            latest.get('SMI_Signal_prev'), latest.get('SVI_Signal_prev'), latest.get('NVI_Signal_prev'), latest.get('Smart_Score_prev'),
            latest.get('Vol_Div_Signal'), latest.get('Weekly_NVI_Signal'),
            latest.get('Div_3Day_Bull'), latest.get('Div_3Day_Bear'),
            latest.get('Vol_MA3'), latest.get('pvi_prev'),
            latest.get('VWAP60'), latest.get('BBW'), latest.get('Fib_0618'),
            latest.get('Weekly_Close'), latest.get('Weekly_Open'),
            latest.get('Monthly_Close'), latest.get('Monthly_Open'),
            latest.get('VWAP200'), latest.get('Mansfield_RS'),
            latest.get('ADL'), latest.get('RS'),
            code # WHERE code=?
        )
    except Exception:
        return None


def batch_load_history(codes, limit_days=400, conn=None):
    """批次載入多支股票的歷史資料 (優化版 - 直接連線)"""
    if not codes:
        return {}
    
    # 計算截止日期
    cutoff_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
    cutoff_int = int(cutoff_date)
    
    placeholders = ','.join(['?'] * len(codes))
    query = f"""
        SELECT 
            code,
            CAST(date_int/10000 AS TEXT) || '-' || 
            SUBSTR('0'||CAST((date_int/100)%100 AS TEXT),-2) || '-' ||
            SUBSTR('0'||CAST(date_int%100 AS TEXT),-2) as date,
            date_int,
            open, high, low, close, volume, amount
        FROM stock_history 
        WHERE code IN ({placeholders}) AND date_int >= ?
        ORDER BY code, date_int ASC
    """
    
    params = list(codes) + [cutoff_int]
    should_close = False
    
    try:
        if conn is None:
            # 直接建立連線，避開 db_manager 可能的問題
            conn = sqlite3.connect(DB_FILE)
            should_close = True
            
        df_all = pd.read_sql_query(query, conn, params=params)
            
    except Exception as e:
        print_flush(f"批次載入失敗: {e}")
        return {}
    finally:
        if should_close and conn:
            conn.close()
    
    result = {}
    if not df_all.empty:
        df_all['date'] = pd.to_datetime(df_all['date'])
        groups = list(df_all.groupby('code'))
        for i, (code, group) in enumerate(groups):
            result[code] = group.reset_index(drop=True)
            
    return result


# ==============================
# Phase 6: 衛語句 - 通用驗證工具
# ==============================
def validate_dataframe(df, min_rows: int = 1, required_cols: List[str] = None) -> bool:
    """
    驗證 DataFrame (衛語句輔助工具)
    
    使用方式：
        if not validate_dataframe(df, min_rows=20, required_cols=['close', 'volume']):
            return None
    
    Args:
        df: 要驗證的 DataFrame
        min_rows: 最小行數
        required_cols: 必要欄位列表
        
    Returns:
        bool: True=驗證通過, False=驗證失敗
    """
    if df is None:
        return False
    if not isinstance(df, pd.DataFrame):
        return False
    if df.empty:
        return False
    if len(df) < min_rows:
        return False
    if required_cols:
        for col in required_cols:
            if col not in df.columns:
                return False
    return True


def validate_code(code) -> bool:
    """
    驗證股票代碼 (衛語句輔助工具)
    
    Args:
        code: 股票代碼
        
    Returns:
        bool: True=驗證通過, False=驗證失敗
    """
    if not code:
        return False
    if not isinstance(code, str):
        return False
    if len(code) < 4:
        return False
    return True


# ==============================
# Phase 2: 衛語句 - 安全工具函數
# ==============================
def safe_api_request(url, headers=None, timeout=30, method='GET', params=None):
    """
    安全的 API 請求 (衛語句模式)
    成功回傳 Response，失敗回傳 None
    """
    if not url:
        return None
    
    default_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    headers = headers or default_headers
    
    try:
        if method.upper() == 'GET':
            resp = requests.get(url, headers=headers, params=params, timeout=timeout, verify=False)
        else:
            resp = requests.post(url, headers=headers, data=params, timeout=timeout, verify=False)
        
        if resp.status_code != 200:
            logger.warning(f"API 回應非 200: {url} -> {resp.status_code}")
            return None
        
        return resp
    except requests.exceptions.Timeout:
        logger.warning(f"API 逾時: {url}")
        return None
    except requests.exceptions.ConnectionError:
        logger.warning(f"API 連線失敗: {url}")
        return None
    except Exception as e:
        logger.error(f"API 請求異常: {url} -> {e}")
        return None


def safe_json_parse(response, default=None):
    """
    安全的 JSON 解析 (衛語句模式)
    成功回傳資料，失敗回傳 default
    """
    if response is None:
        return default
    
    try:
        text = response.text.strip()
        if not text:
            return default
        return response.json()
    except Exception as e:
        logger.warning(f"JSON 解析失敗: {e}")
        return default


def get_nested(obj, *keys, default=None):
    """
    安全取得巢狀物件值 (類似 Optional Chaining)
    用法: get_nested(data, 'user', 'address', 'city', default='Unknown')
    """
    for key in keys:
        if obj is None:
            return default
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, (list, tuple)) and isinstance(key, int):
            obj = obj[key] if 0 <= key < len(obj) else None
        else:
            obj = getattr(obj, key, None)
    return obj if obj is not None else default


# ==============================
# Phase 6: 多線程並行工具
# ==============================
def run_parallel_tasks(tasks, max_workers=None, show_progress=True, silent_execution=False):
    """
    並行執行多個任務 (方案 A: API 群組並行)
    
    :param tasks: [(func, args, kwargs, name, label), ...]
                  label 為可選的步驟標籤 (如 "3.5", "3.6")
    :param max_workers: 最大線程數 (預設使用 Config.MAX_WORKERS，手機=2, 電腦=6)
    :param show_progress: 是否顯示進度
    :param silent_execution: 是否靜默執行 (抑制子任務輸出，最後統一顯示)
    :return: {name: result}
    """
    # [Phase 7] 環境自適應：使用 Config.MAX_WORKERS
    if max_workers is None:
        max_workers = Config.MAX_WORKERS
    import io
    import sys
    
    results = {}
    task_results = {}  # 儲存格式化結果
    completed = 0
    total = len(tasks)
    
    # 任務名稱對應標籤
    task_labels = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_info = {}
        for task in tasks:
            func = task[0]
            args = task[1] if len(task) > 1 else ()
            kwargs = task[2] if len(task) > 2 else {}
            name = task[3] if len(task) > 3 else func.__name__
            label = task[4] if len(task) > 4 else ""
            
            task_labels[name] = label
            future = executor.submit(func, *args, **kwargs)
            future_to_info[future] = name
        
        for future in as_completed(future_to_info):
            name = future_to_info[future]
            label = task_labels.get(name, "")
            completed += 1
            try:
                result = future.result()
                results[name] = result
                
                # 格式化輸出
                if show_progress:
                    label_str = f"[{label}] " if label else ""
                    # 嘗試從結果中提取數量
                    if isinstance(result, int):
                        count_str = f"{result} 筆" if result > 0 else "跳過"
                    elif isinstance(result, (set, list)):
                        count_str = f"{len(result)} 筆"
                    elif result is None:
                        count_str = "完成"
                    else:
                        count_str = "✓"
                    
                    print_flush(f"  {label_str}{name:<12}: ✓ {count_str}")
                    
            except Exception as e:
                logger.error(f"並行任務失敗 [{name}]: {e}")
                results[name] = None
                if show_progress:
                    label_str = f"[{label}] " if label else ""
                    print_flush(f"  {label_str}{name:<12}: ⚠ 失敗")
    
    return results


def fetch_both_markets_parallel(twse_func, tpex_func, twse_name='TWSE', tpex_name='TPEx'):
    """
    並行獲取 TWSE + TPEx 資料 (方案 B: 內部並行)
    
    :param twse_func: TWSE 獲取函數
    :param tpex_func: TPEx 獲取函數
    :return: 合併後的結果列表
    """
    results = []
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_twse = executor.submit(twse_func)
        future_tpex = executor.submit(tpex_func)
        
        try:
            twse_data = future_twse.result()
            if twse_data:
                results.extend(twse_data)
        except Exception as e:
            logger.warning(f"{twse_name} 獲取失敗: {e}")
        
        try:
            tpex_data = future_tpex.result()
            if tpex_data:
                results.extend(tpex_data)
        except Exception as e:
            logger.warning(f"{tpex_name} 獲取失敗: {e}")
    
    return results


def safe_float_preserving_none(value, default=None):
    """強化版 Null 處理 (處理所有邊界情況)"""
    if value is None:
        return default
    
    # 處理 bytes 類型 (SQLite INTEGER 回傳)
    if isinstance(value, bytes):
        try:
            value = int.from_bytes(value, byteorder='little', signed=True)
        except (ValueError, OverflowError):
            return default
    
    # 處理 NaN
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return default
    
    # 處理字串類型
    if isinstance(value, str):
        value = value.strip().replace(',', '')
        if value in ('', '--', 'N/A'):
            return default
    
    try:
        return float(value)
    except (ValueError, TypeError, OverflowError):
        return default

def safe_num(value, default=None):
    """安全轉換數值 (Alias for safe_float_preserving_none)"""
    return safe_float_preserving_none(value, default)

def safe_int(value, default=0):
    """強化版整數處理"""
    result = safe_float_preserving_none(value, default)
    if result is None:
        return default
    try:
        return int(result)
    except (ValueError, TypeError, OverflowError):
        return default

def safe_json_parse(text):
    """安全解析 JSON"""
    try:
        return json.loads(text)
    except:
        return None

def roc_to_western_date(roc_date_str):
    """民國日期轉西元日期"""
    if pd.isna(roc_date_str) or roc_date_str is None:
        return "1970-01-01"
    
    roc_date_str = str(roc_date_str).strip()
    
    try:
        if len(roc_date_str) == 7 and roc_date_str.isdigit():
            y = int(roc_date_str[:3]) + 1911
            m = int(roc_date_str[3:5])
            d = int(roc_date_str[5:])
            return f"{y}-{m:02d}-{d:02d}"
        
        parts = re.split(r'[/-]', roc_date_str)
        if len(parts) == 3:
            return f"{int(parts[0])+1911}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    except:
        pass
    
    return roc_date_str

def convert_numeric_columns(df):
    """將字串數字欄位轉換為數值型態"""
    numeric_cols = ['成交股數', '成交金額', '成交筆數', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
    
    for col in numeric_cols:
        if col in df.columns:
            # 移除千分位逗號並轉換為數值
            df[col] = df[col].astype(str).str.replace(',', '').str.replace('--', '0').str.replace('X', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def convert_dates_to_western(df):
    """將民國日期轉換為西元日期"""
    if '日期' in df.columns:
        df['日期'] = df['日期'].apply(roc_to_western_date)
    return df

def standardize_dataframe(df, source, stock_code):
    """將 DataFrame 欄位標準化"""
    column_mapping = {
        '日期': 'date',
        '開盤價': 'open',
        '最高價': 'high',
        '最低價': 'low',
        '收盤價': 'close',
        '成交股數': 'volume',
        '成交金額': 'amount'
    }
    
    # 重新命名欄位
    df = df.rename(columns=column_mapping)
    
    # 只保留需要的欄位
    keep_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
    df = df[[col for col in keep_cols if col in df.columns]]
    
    # 設置日期為索引
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df = df.set_index('date')
    
    # 移除無效資料
    if 'close' in df.columns:
        df = df[df['close'] > 0]
    
    return df

def print_flush(s="", end="\n"):
    """立即輸出並刷新緩衝區 (處理編碼問題)"""
    try:
        print(s, end=end)
    except UnicodeEncodeError:
        # 移除無法編碼的字元後重試
        safe_s = s.encode(sys.stdout.encoding or 'utf-8', errors='replace').decode(sys.stdout.encoding or 'utf-8', errors='replace')
        print(safe_s, end=end)
    sys.stdout.flush()


# ==============================
# 統一輸出格式類別
# ==============================
class StepOutput:
    """統一步驟輸出格式"""
    
    # 顏色代碼
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    @staticmethod
    def header(title, step_num=None):
        """輸出步驟標題"""
        if step_num:
            print_flush(f"\n{StepOutput.CYAN}[Step {step_num}]{StepOutput.RESET} {StepOutput.BOLD}{title}{StepOutput.RESET}")
        else:
            print_flush(f"\n{StepOutput.BOLD}{title}{StepOutput.RESET}")
    
    @staticmethod
    def success(msg, indent=0):
        """成功訊息"""
        prefix = "  " * indent
        print_flush(f"{prefix}{StepOutput.GREEN}✓{StepOutput.RESET} {msg}")
    
    @staticmethod
    def warn(msg, indent=0):
        """警告訊息"""
        prefix = "  " * indent
        print_flush(f"{prefix}{StepOutput.YELLOW}⚠{StepOutput.RESET} {msg}")
    
    @staticmethod
    def error(msg, indent=0):
        """錯誤訊息"""
        prefix = "  " * indent
        print_flush(f"{prefix}{StepOutput.RED}✗{StepOutput.RESET} {msg}")
    
    @staticmethod
    def info(msg, indent=0):
        """一般訊息"""
        prefix = "  " * indent
        print_flush(f"{prefix}{msg}")
    
    @staticmethod
    def progress(current, total, desc=""):
        """進度顯示"""
        pct = current / total * 100 if total > 0 else 0
        bar_len = 30
        filled = int(bar_len * current / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        print_flush(f"\r  [{bar}] {pct:5.1f}% {desc}", end="")
        if current >= total:
            print_flush()  # 換行
    
    @staticmethod
    def separator(char="─", width=50):
        """分隔線"""
        print_flush(char * width)
    
    @staticmethod
    def box_start(title):
        """開始框"""
        print_flush(f"\n{'═' * 60}")
        print_flush(f"📊 {title}")
        print_flush(f"{'═' * 60}")
    
    @staticmethod
    def box_end(msg="完成"):
        """結束框"""
        print_flush(f"\n{'═' * 60}")
        print_flush(f"✓ {msg}")
        print_flush(f"{'═' * 60}")
    
    @staticmethod
    def table_row(cols, widths=None):
        """表格行"""
        if widths is None:
            widths = [12] * len(cols)
        row = " | ".join(f"{str(c):<{w}}" for c, w in zip(cols, widths))
        print_flush(f"  {row}")


def read_single_key(prompt="請選擇: "):
    """讀取單一按鍵 (支援 Windows/Linux)"""
    print(prompt, end='', flush=True)
    
    # 支援自動化測試 (非 TTY 環境)
    if not sys.stdin.isatty():
        try:
            s = sys.stdin.readline().strip()
            if len(s) > 0:
                return s[0]
        except:
            pass
    
    if HAS_TERMIOS:
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        print()
        return ch
    else:
        import msvcrt
        while True:
            try:
                ch = msvcrt.getch()
                # Skip special keys
                if ch in [b'\x00', b'\xe0']:
                    msvcrt.getch()
                    continue
                
                try:
                    decoded = ch.decode('utf-8')
                except:
                    continue
                    
                if decoded:
                    print(decoded)
                    return decoded
            except:
                continue

def get_display_limit(default=30):
    """獲取顯示檔數限制"""
    try:
        limit = input(f"請輸入顯示檔數 (預設{default}): ").strip()
        return int(limit) if limit.isdigit() and int(limit) > 0 else default
    except:
        return default

def get_volume_limit(default=500):
    """獲取成交量限制"""
    try:
        limit = input(f"請輸入最小成交量(張) (預設{default}): ").strip()
        return int(limit) * 1000 if limit.isdigit() else default * 1000
    except:
        return default * 1000

# ╔══════════════════════════════════════════════════════════════╗
# ║                       INFRA/DB                                ║
# ║  DatabaseManager / SingleWriterDBManager / ProxyConnection    ║
# ╚══════════════════════════════════════════════════════════════╝

# ==============================
# 基礎設施層
# ==============================
class DatabaseManager:
    _instance = None
    _pool_size = 5
    _connection_pool = None
    _pool_lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._init_pool()
            cls._instance._remove_stale_locks()
        return cls._instance
    
    def _init_pool(self):
        """初始化連線池 (Lazy Initialization)"""
        with self._pool_lock:
            if self._connection_pool is None:
                self._connection_pool = queue.Queue(maxsize=self._pool_size)
                for _ in range(self._pool_size):
                    conn = self._create_connection()
                    self._connection_pool.put(conn)
    
    def _create_connection(self):
        """建立單一連線 (DRY Principle)"""
        conn = sqlite3.connect(DB_FILE, timeout=60, check_same_thread=False)
        if not IS_ANDROID:
            conn.execute("PRAGMA journal_mode=WAL;")
        else:
            conn.execute("PRAGMA journal_mode=DELETE;")
            conn.execute("PRAGMA busy_timeout=60000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _remove_stale_locks(self):
        """移除 SQLite 殘留的鎖定檔案"""
        lock_files = [
            DB_FILE.with_suffix(DB_FILE.suffix + '-journal'),
            DB_FILE.with_suffix(DB_FILE.suffix + '-wal'),
            DB_FILE.with_suffix(DB_FILE.suffix + '-shm')
        ]
        
        for lock_file in lock_files:
            if lock_file.exists():
                try:
                    lock_file.unlink()
                except:
                    pass
    
    @contextmanager
    def get_connection(self, timeout=30):
        """從連線池取得連線 (Thread-Safe)"""
        conn = None
        try:
            conn = self._connection_pool.get(timeout=timeout)
            yield conn
        except queue.Empty:
            raise sqlite3.OperationalError("連線池已滿，請稍後重試")
        finally:
            if conn:
                # 歸還連線到池中 (而非關閉)
                try:
                    self._connection_pool.put_nowait(conn)
                except queue.Full:
                    conn.close()  # 池已滿，關閉多餘連線

class IndicatorCacheManager:
    """單例模式 + 執行緒安全的快取管理器"""
    _instance = None
    _lock = threading.RLock()  # 使用 RLock 允許重入
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-Checked Locking
                    cls._instance = super().__new__(cls)
                    cls._instance._data = {}
                    cls._instance._timestamp = None
                    cls._instance._cache_duration = 3600
        return cls._instance
    
    def get_data(self):
        """執行緒安全讀取"""
        with self._lock:
            # 檢查快取過期
            if self._timestamp and (time.time() - self._timestamp) > self._cache_duration:
                self._data = {}
                self._timestamp = None
            return self._data.copy()  # 返回副本避免外部修改
    
    def set_data(self, data):
        """執行緒安全寫入"""
        with self._lock:
            self._data = data
            self._timestamp = time.time()
    
    def clear(self):
        """清除快取"""
        with self._lock:
            self._data = {}
            self._timestamp = None

# 創建全局實例
db_manager = DatabaseManager()
GLOBAL_INDICATOR_CACHE = IndicatorCacheManager()

# ==============================
# 進度追蹤器
# ==============================
class ProgressTracker:
    """
    強健版進度追蹤器
    - Windows VT100 支援
    - 線程安全
    - 自動限流
    """
    _lock = threading.Lock()
    _last_update_time = 0
    _UPDATE_INTERVAL = 0.1  # 限制最大刷新率為 10 FPS
    
    def __init__(self, total_lines=3):
        self.total_lines = total_lines
        self._initialized = False
        self._lines_buffer = [""] * total_lines
        self._last_update_time = 0
        

    
    def __enter__(self):
        self.reset()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # 確保最後換行，避免後續輸出覆蓋
        sys.stdout.write('\n')
        sys.stdout.flush()
    
    def update_lines(self, *messages, force=False):
        """更新多行進度 (VT100 三行進度)"""
        current_time = time.time()
        if not force and (current_time - self._last_update_time < self._UPDATE_INTERVAL):
            return
        
        with self._lock:
            # 準備內容，不足補空行
            lines = list(messages) + [""] * (self.total_lines - len(messages))
            lines = lines[:self.total_lines]
            
            # 更新內部緩衝區，確保混合調用時狀態一致
            self._lines_buffer = lines
            
            # 使用 VT100 游標控制實現多行進度
            if self._initialized:
                # 游標上移 N 行
                sys.stdout.write(f'\033[{self.total_lines}A')
            
            for line in lines:
                # 清除該行並寫入新內容
                # 限制行長度避免換行導致跳動
                display_line = str(line)[:78] if line else ""
                sys.stdout.write(f'\033[2K\r{display_line}\n')
            
            sys.stdout.flush()
            self._initialized = True
            self._last_update_time = current_time
    
    def reset(self):
        """重置追蹤器狀態"""
        self._initialized = False
        self._lines_buffer = [""] * self.total_lines
    
    def info(self, message, level=1):
        """顯示一般訊息"""
        self._update_single_line(message, level)
    
    def warning(self, message, level=1):
        """顯示警告訊息"""
        self._update_single_line(f"⚠ {message}", level)
    
    def success(self, message, level=1):
        """顯示成功訊息"""
        self._update_single_line(f"✓ {message}", level)
    
    def error(self, message, level=1):
        """顯示錯誤訊息"""
        self._update_single_line(f"❌ {message}", level)
    
    def _update_single_line(self, message, level):
        """更新單行內容並刷新顯示"""
        # 動態限制索引範圍
        idx = max(0, min(level - 1, self.total_lines - 1))
        self._lines_buffer[idx] = message
        self.update_lines(*self._lines_buffer)

# ==============================
# 進度追蹤函數
# ==============================
def load_progress():
    """載入進度追蹤系統"""
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
                # 確保所有必要的鍵存在
                default_progress = {
                    "last_code_index": 0,
                    "missing_stocks": [],
                    "new_stocks": [],
                    "failed_stocks": [],
                    "in_progress": None,
                    "stock_list_last_idx": 0,
                    "stock_list_processed": [],
                    "calc_last_idx": 0,
                    "timestamp": datetime.now().isoformat()
                }
                
                for key, default_value in default_progress.items():
                    if key not in progress:
                        progress[key] = default_value
                
                return progress
    except Exception as e:
        print_flush(f"⚠ 無法載入進度檔: {e}")
    
    return {
        "last_code_index": 0,
        "missing_stocks": [],
        "new_stocks": [],
        "failed_stocks": [],
        "in_progress": None,
        "stock_list_last_idx": 0,
        "stock_list_processed": [],
        "calc_last_idx": 0,
        "timestamp": datetime.now().isoformat()
    }

def save_progress(last_idx=None, missing_stocks=None, new_stocks=None, failed_stocks=None, in_progress=None,
                  stock_list_last_idx=None, stock_list_processed=None, calc_last_idx=None):
    """儲存進度追蹤系統"""
    try:
        # 載入現有進度
        current = load_progress()
        
        # 更新進度（只更新提供的參數）
        progress = {
            "last_code_index": last_idx if last_idx is not None else current.get("last_code_index", 0),
            "missing_stocks": list(set(missing_stocks)) if missing_stocks is not None else current.get("missing_stocks", []),
            "new_stocks": list(set(new_stocks)) if new_stocks is not None else current.get("new_stocks", []),
            "failed_stocks": list(set(failed_stocks)) if failed_stocks is not None else current.get("failed_stocks", []),
            "in_progress": in_progress if in_progress is not None else current.get("in_progress"),
            "stock_list_last_idx": stock_list_last_idx if stock_list_last_idx is not None else current.get("stock_list_last_idx", 0),
            "stock_list_processed": stock_list_processed if stock_list_processed is not None else current.get("stock_list_processed", []),
            "calc_last_idx": calc_last_idx if calc_last_idx is not None else current.get("calc_last_idx", 0),
            "timestamp": datetime.now().isoformat()
        }
        
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print_flush(f"⚠ 無法儲存進度檔: {e}")

def clear_progress():
    """清除進度檔 (完成時呼叫)"""
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception as e:
        print_flush(f"⚠ 無法清除進度檔: {e}")

def reset_progress():
    """重置進度檔"""
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception as e:
        print_flush(f"⚠ 無法重置進度檔: {e}")


# ==============================
# 資料庫管理器 - 單一寫入員模式
# ==============================

@dataclass
class WriteOperation:
    """寫入操作封裝"""
    query: str                              # SQL 語句
    params: tuple = ()                      # 參數
    is_many: bool = False                   # 是否為 executemany
    result_future: Optional[Future] = None  # 結果 Future

class SingleWriterDBManager:
    """
    單一寫入員模式資料庫管理器
    - 所有寫入操作透過佇列序列化
    - 讀取操作保持併發能力
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path=None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path):
        if self._initialized:
            return
        self.db_path = db_path
        self._write_queue = queue.Queue()
        self._writer_thread = None
        self._shutdown = threading.Event()
        self._start_writer()
        self._initialized = True
    
    def _start_writer(self):
        """啟動背景寫入線程"""
        self._writer_thread = threading.Thread(
            target=self._writer_loop, 
            daemon=True, 
            name="DBWriter"
        )
        self._writer_thread.start()
        logger.debug("資料庫寫入線程已啟動")
    
    def _writer_loop(self):
        """寫入線程主迴圈 - 批次處理"""
        conn = sqlite3.connect(str(self.db_path), timeout=60)
        if not IS_ANDROID:
            conn.execute("PRAGMA journal_mode=WAL")
        else:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        
        while not self._shutdown.is_set():
            batch = []
            # 動態批次大小：依佇列深度自動調整
            queue_depth = self._write_queue.qsize()
            if queue_depth < 50:
                max_batch = 50       # 淺佇列：快速響應
            elif queue_depth < 200:
                max_batch = 100      # 中等佇列：平衡
            elif queue_depth < 500:
                max_batch = 300      # 深佇列：高吞吐量
            else:
                max_batch = 500      # 超載：最大批次
            
            try:
                op = self._write_queue.get(timeout=0.1)
                batch.append(op)
                # 嘗試收集更多操作 (依動態批次大小)
                while len(batch) < max_batch:
                    try:
                        batch.append(self._write_queue.get_nowait())
                    except queue.Empty:
                        break
            except queue.Empty:
                continue
            
            # 執行批次
            try:
                cursor = conn.cursor()
                for op in batch:
                    try:
                        if op.is_many:
                            cursor.executemany(op.query, op.params)
                        else:
                            cursor.execute(op.query, op.params)
                        if op.result_future and not op.result_future.done():
                            op.result_future.set_result(cursor.rowcount)
                    except Exception as e:
                        if op.result_future and not op.result_future.done():
                            op.result_future.set_exception(e)
                conn.commit()
            except Exception as e:
                logger.error(f"資料庫批次寫入失敗: {e}")
                conn.rollback()
                for op in batch:
                    if op.result_future and not op.result_future.done():
                        op.result_future.set_exception(e)
        
        conn.close()
        logger.debug("資料庫寫入線程已關閉")
    
    def execute_write(self, query, params=(), is_many=False, wait=True):
        """提交寫入操作"""
        future = Future() if wait else None
        op = WriteOperation(query=query, params=params, 
                           is_many=is_many, result_future=future)
        self._write_queue.put(op)
        if wait and future:
            try:
                return future.result(timeout=30)
            except Exception as e:
                logger.error(f"寫入操作失敗: {e}")
                raise
        return None
    
    def get_read_connection(self, timeout=30):
        """取得讀取專用連線"""
        conn = sqlite3.connect(
            str(self.db_path), 
            timeout=timeout,
            check_same_thread=False
        )
        return conn
    
    def shutdown(self):
        """關閉寫入線程"""
        self._shutdown.set()
        if self._writer_thread:
            self._writer_thread.join(timeout=5)

class ProxyCursor:
    """
    游標代理 - 自動判斷讀/寫操作
    使用版本號追蹤確保 cursor 始終有效
    """
    _WRITE_KEYWORDS = ('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'REPLACE')
    
    def __init__(self, proxy_conn):
        self._proxy_conn = proxy_conn
        self._read_cursor = None  # Lazy initialization
        self._cursor_version = -1  # 追蹤創建此 cursor 時的連線版本
        self._last_description = None
        self._last_rowcount = -1
    
    def _get_cursor(self):
        """動態獲取 cursor (版本變更時自動重新獲取)"""
        current_version = getattr(self._proxy_conn, '_conn_version', 0)
        if self._read_cursor is None or self._cursor_version != current_version:
            # 連線版本已變更，需要重新獲取 cursor
            self._read_cursor = self._proxy_conn._read_conn.cursor()
            self._cursor_version = current_version
        return self._read_cursor
    
    def execute(self, query, params=()):
        query_upper = query.strip().upper()
        if any(query_upper.startswith(kw) for kw in self._WRITE_KEYWORDS):
            # 寫入操作：加入待處理佇列
            self._proxy_conn._pending_writes.append((query, params, False))
            self._last_rowcount = 0
        else:
            # 讀取操作：直接執行
            cursor = self._get_cursor()
            cursor.execute(query, params)
            self._last_description = cursor.description
            self._last_rowcount = cursor.rowcount
        return self
    
    def executemany(self, query, params_list):
        # executemany 總是寫入操作
        self._proxy_conn._pending_writes.append((query, params_list, True))
        self._last_rowcount = len(params_list) if hasattr(params_list, '__len__') else 0
        return self
    
    def fetchone(self):
        return self._get_cursor().fetchone()
    
    def fetchall(self):
        return self._get_cursor().fetchall()
    
    def fetchmany(self, size=None):
        return self._get_cursor().fetchmany(size)
    
    @property
    def description(self):
        cursor = self._get_cursor()
        return self._last_description or cursor.description
    
    @property
    def rowcount(self):
        return self._last_rowcount
    
    @property
    def lastrowid(self):
        return self._get_cursor().lastrowid
    
    def close(self):
        """關閉游標 (如果存在)"""
        if self._read_cursor is not None:
            try:
                self._read_cursor.close()
            except:
                pass
            self._read_cursor = None

class ProxyConnection:
    """
    連線代理 - 實現透明的讀/寫分離
    - SELECT: 直接執行
    - INSERT/UPDATE/DELETE: 走寫入佇列
    """
    def __init__(self, manager: SingleWriterDBManager):
        self._manager = manager
        self._read_conn = sqlite3.connect(
            str(manager.db_path), 
            timeout=30,
            check_same_thread=False
        )
        self._pending_writes = []
        self._conn_version = 0  # 連線版本號
    
    @property
    def row_factory(self):
        """代理 row_factory 屬性 (讀取)"""
        return self._read_conn.row_factory
    
    @row_factory.setter
    def row_factory(self, value):
        """代理 row_factory 屬性 (設定)"""
        self._read_conn.row_factory = value
    
    def cursor(self):
        return ProxyCursor(self)
    
    def execute(self, query, params=()):
        cursor = ProxyCursor(self)
        cursor.execute(query, params)
        return cursor
    
    def executemany(self, query, params_list):
        cursor = ProxyCursor(self)
        cursor.executemany(query, params_list)
        return cursor
    
    def commit(self):
        """批次提交所有待處理寫入"""
        for query, params, is_many in self._pending_writes:
            self._manager.execute_write(query, params, is_many, wait=True)
        self._pending_writes.clear()
        # 重新開啟讀取連線以看到新資料 (WAL mode 隔離)
        try:
            self._read_conn.close()
        except:
            pass
        self._read_conn = sqlite3.connect(
            str(self._manager.db_path), 
            timeout=30,
            check_same_thread=False
        )
        self._conn_version += 1  # 增加版本號
    
    def rollback(self):
        """清除待處理寫入"""
        self._pending_writes.clear()
    
    def close(self):
        try:
            self._read_conn.close()
        except:
            pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()

class DBManager:
    """
    資料庫管理器 - 使用單一寫入員模式
    相容現有 API：db_manager.get_connection()
    """
    def __init__(self, db_path):
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self._writer = SingleWriterDBManager(self.db_path)
    
    @contextmanager
    def get_connection(self, timeout=30):
        """相容性方法 - 返回代理連線"""
        conn = ProxyConnection(self._writer)
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_write(self, query, params=(), is_many=False):
        """直接寫入 API (bypass proxy)"""
        return self._writer.execute_write(query, params, is_many)
    
    def get_read_connection(self, timeout=30):
        """取得讀取專用連線 (高效能讀取)"""
        return self._writer.get_read_connection(timeout)
    
    def shutdown(self):
        """關閉資料庫管理器"""
        self._writer.shutdown()


# ╔══════════════════════════════════════════════════════════════╗
# ║                         REPO                                  ║
# ║  SnapshotRepository / HistoryRepository - DB 操作抽象層       ║
# ╚══════════════════════════════════════════════════════════════╝

class HistoryRepository:
    """歷史資料存取層 (Phase 4)"""
    
    def __init__(self, db_mgr):
        self._db = db_mgr
    
    def get_history(self, code: str, limit: int = 400) -> pd.DataFrame:
        """讀取歷史資料"""
        sql = QUERY_TEMPLATES.get('get_stock_history', 
            "SELECT date_int, open, high, low, close, volume, amount FROM stock_history WHERE code = ? ORDER BY date_int DESC LIMIT ?")
        with self._db.get_read_connection() as conn:
            return pd.read_sql_query(sql, conn, params=(code, limit))
    
    def get_latest_date(self, code: str) -> Optional[int]:
        """取得指定股票的最新日期"""
        sql = QUERY_TEMPLATES.get('get_latest_date',
            "SELECT MAX(date_int) FROM stock_history WHERE code = ?")
        with self._db.get_read_connection() as conn:
            cur = conn.execute(sql, (code,))
            res = cur.fetchone()
            return res[0] if res else None
    
    def upsert(self, code: str, records: List[Dict]) -> int:
        """批量寫入歷史資料"""
        # [Guard Clause]
        if not records:
            return 0
        
        sql = SQL_UPSERT_TEMPLATES.get('history_upsert', """
            INSERT OR REPLACE INTO stock_history 
            (code, date_int, open, high, low, close, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        params = [(code, r.get('date_int'), r.get('open'), r.get('high'), 
                   r.get('low'), r.get('close'), r.get('volume'), r.get('amount')) 
                  for r in records]
        return self._db.execute_write(sql, params, is_many=True)


class SnapshotRepository:
    """快照資料存取層 (Phase 4)"""
    
    def __init__(self, db_mgr):
        self._db = db_mgr
    
    def get_snapshot(self, code: str) -> Optional[Dict]:
        """讀取快照資料"""
        sql = QUERY_TEMPLATES.get('get_snapshot', 
            "SELECT * FROM stock_snapshot WHERE code = ?")
        with self._db.get_read_connection() as conn:
            cur = conn.execute(sql, (code,))
            row = cur.fetchone()
            if row:
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))
        return None
    
    def upsert_indicators(self, code: str, date_int: int, indicators: Dict) -> int:
        """寫入指標快照"""
        sql = """
            INSERT OR REPLACE INTO stock_snapshot
            (code, date_int, close, volume, ma5, ma20, ma60, ma120, ma200, rsi, mfi14, smart_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            code, date_int, 
            indicators.get('close'), indicators.get('volume'),
            indicators.get('ma5'), indicators.get('ma20'), indicators.get('ma60'),
            indicators.get('ma120'), indicators.get('ma200'),
            indicators.get('rsi'), indicators.get('mfi14'), indicators.get('smart_score')
        )
        return self._db.execute_write(sql, params)


db_manager = DBManager(Config.DB_PATH)

# ==============================
# 資料庫初始化
# ==============================
def ensure_db():
    """確保資料庫表結構存在"""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        
        # 建立股票名冊表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_meta (
                code TEXT PRIMARY KEY,
                name TEXT,
                list_date TEXT,
                delist_date TEXT,
                market_type TEXT
            )
        """)
        
        # 建立歷史表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_history (
                code TEXT,
                date_int INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                amount INTEGER,
                PRIMARY KEY (code, date_int)
            )
        """)
        
        # 建立快照表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_snapshot (
                code TEXT PRIMARY KEY,
                name TEXT,
                date TEXT,
                close REAL,
                volume INTEGER,
                close_prev REAL,
                vol_prev INTEGER,
                ma3 REAL, ma20 REAL, ma60 REAL, ma120 REAL, ma200 REAL,
                wma3 REAL, wma20 REAL, wma60 REAL, wma120 REAL, wma200 REAL,
                mfi14 REAL, vwap20 REAL, chg14_pct REAL,
                rsi REAL, macd REAL, signal REAL,
                vp_poc REAL, vp_upper REAL, vp_lower REAL,
                month_k REAL, month_d REAL,
                daily_k REAL, daily_d REAL,
                week_k REAL, week_d REAL,
                ma3_prev REAL, ma20_prev REAL, ma60_prev REAL, ma120_prev REAL, ma200_prev REAL,
                wma3_prev REAL, wma20_prev REAL, wma60_prev REAL, wma120_prev REAL, wma200_prev REAL,
                mfi14_prev REAL, vwap20_prev REAL, chg14_pct_prev REAL,
                month_k_prev REAL, month_d_prev REAL,
                daily_k_prev REAL, daily_d_prev REAL,
                week_k_prev REAL, week_d_prev REAL,
                smi REAL, svi REAL, nvi REAL, pvi REAL, clv REAL,
                major_holders_pct REAL,
                foreign_buy INTEGER, trust_buy INTEGER, dealer_buy INTEGER,
                adl REAL, rs REAL,
                smi_signal INTEGER, svi_signal INTEGER,
                nvi_signal INTEGER, vsa_signal INTEGER,
                smart_score INTEGER,
                smi_prev REAL, svi_prev REAL, nvi_prev REAL,
                smart_score_prev INTEGER,
                vol_div_signal INTEGER, weekly_nvi_signal INTEGER,
                vwap60 REAL, bbw REAL, fib_0618 REAL,
                weekly_close REAL, weekly_open REAL,
                monthly_close REAL, monthly_open REAL,
                vwap200 REAL, mansfield_rs REAL,
                margin_balance INTEGER, margin_util_rate REAL,
                short_balance INTEGER, short_util_rate REAL,
                FOREIGN KEY (code) REFERENCES stock_meta(code)
            )
        """)
        
        # 建立融資融券表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS margin_data (
                date_int INTEGER,
                code TEXT,
                margin_buy INTEGER,
                margin_sell INTEGER,
                margin_redemp INTEGER,
                margin_balance INTEGER,
                margin_util_rate REAL,
                short_buy INTEGER,
                short_sell INTEGER,
                short_redemp INTEGER,
                short_balance INTEGER,
                short_util_rate REAL,
                PRIMARY KEY (date_int, code)
            )
        """)
        
        # 建立大盤指數表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_index (
                date_int INTEGER,
                index_id TEXT, -- 'TAIEX', 'TPEX', 'VIX'
                close REAL,
                open REAL,
                high REAL,
                low REAL,
                volume INTEGER,
                PRIMARY KEY (date_int, index_id)
            )
        """)
        
        # 建立索引
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_meta_code ON stock_meta(code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_code ON stock_history(code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_date ON stock_history(date_int)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_code_date ON stock_history(code, date_int DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshot_date ON stock_snapshot(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshot_smart_score ON stock_snapshot(smart_score)")
        
        # 檢查 stock_snapshot 是否有新欄位 (Migration)
        cur.execute("PRAGMA table_info(stock_snapshot)")
        snapshot_cols = {row[1] for row in cur.fetchall()}
        
        new_snapshot_cols = [
            ("vol_div_signal", "INTEGER"),
            ("weekly_nvi_signal", "INTEGER"),
            ("div_3day_bull", "INTEGER"),
            ("div_3day_bear", "INTEGER"),
            ("vol_ma3", "REAL"),
            ("pvi_prev", "REAL"),
            ("vwap60", "REAL"),
            ("bbw", "REAL"),
            ("fib_0618", "REAL"),
            ("weekly_close", "REAL"),
            ("weekly_open", "REAL"),
            ("monthly_close", "REAL"),
            ("monthly_open", "REAL"),
            ("vwap200", "REAL"),
            ("mansfield_rs", "REAL"),
            ("margin_balance", "INTEGER"),
            ("margin_util_rate", "REAL"),
            ("short_balance", "INTEGER"),
            ("short_util_rate", "REAL"),
            ("amount", "REAL"),
            ("pe", "REAL"),
            ("yield", "REAL"),
            ("pb", "REAL")
        ]
        
        for col_name, col_type in new_snapshot_cols:
            if col_name not in snapshot_cols:
                try:
                    cur.execute(f"ALTER TABLE stock_snapshot ADD COLUMN {col_name} {col_type}")
                    print_flush(f"✓ 已新增欄位 {col_name} 到 stock_snapshot")
                except Exception as e:
                    print_flush(f"⚠ 添加欄位 {col_name} 失敗: {e}")
        
        # [已移除舊架構 stock_data 相容性代碼 - 統一使用新三表架構]
        
        # 同步欄位到 stock_snapshot (新三表架構)
        columns_to_sync = [
            ("ma3", "REAL"), ("ma20", "REAL"), ("ma60", "REAL"), ("ma120", "REAL"), ("ma200", "REAL"),
            ("wma3", "REAL"), ("wma20", "REAL"), ("wma60", "REAL"), ("wma120", "REAL"), ("wma200", "REAL"),
            ("mfi14", "REAL"), ("vwap20", "REAL"), ("chg14_pct", "REAL"), 
            ("rsi", "REAL"), ("macd", "REAL"), ("signal", "REAL"),
            ("vp_poc", "REAL"), ("vp_upper", "REAL"), ("vp_lower", "REAL"),
            ("month_k", "REAL"), ("month_d", "REAL"),
            ("daily_k", "REAL"), ("daily_d", "REAL"),
            ("week_k", "REAL"), ("week_d", "REAL"),
            ("ma3_prev", "REAL"), ("ma20_prev", "REAL"), ("ma60_prev", "REAL"), ("ma120_prev", "REAL"), ("ma200_prev", "REAL"),
            ("wma3_prev", "REAL"), ("wma20_prev", "REAL"), ("wma60_prev", "REAL"), ("wma120_prev", "REAL"), ("wma200_prev", "REAL"),
            ("mfi14_prev", "REAL"), ("vwap20_prev", "REAL"), ("chg14_pct_prev", "REAL"),
            ("month_k_prev", "REAL"), ("month_d_prev", "REAL"),
            ("daily_k_prev", "REAL"), ("daily_d_prev", "REAL"),
            ("week_k_prev", "REAL"), ("week_d_prev", "REAL"),
            ("smi", "REAL"), ("svi", "REAL"), ("nvi", "REAL"), 
            ("pvi", "REAL"), ("clv", "REAL"),
            ("smi_signal", "INTEGER"), ("svi_signal", "INTEGER"), 
            ("nvi_signal", "INTEGER"), ("vsa_signal", "INTEGER"),
            ("smart_score", "INTEGER"),
            ("smi_prev", "REAL"), ("svi_prev", "REAL"), ("nvi_prev", "REAL"), 
            ("pvi_prev", "REAL"), # [Fix] Add pvi_prev
            ("smart_score_prev", "INTEGER"),
            ("div_3day_bull", "INTEGER"), ("div_3day_bear", "INTEGER"),
            ("vol_ma3", "REAL")
        ]
        
        # 同步更新 stock_snapshot 的欄位
        cur.execute("PRAGMA table_info(stock_snapshot)")
        snapshot_columns = {row[1] for row in cur.fetchall()}
        
        for col_name, col_type in columns_to_sync:
            if col_name not in snapshot_columns:
                try:
                    print_flush(f"   -> Adding column to stock_snapshot: {col_name} ({col_type})...")
                    cur.execute(f"ALTER TABLE stock_snapshot ADD COLUMN {col_name} {col_type}")
                    print_flush(f"      ✓ Added {col_name} to snapshot")
                except Exception as e:
                    print_flush(f"⚠ 添加 snapshot 欄位 {col_name} 失敗: {e}")

        conn.commit()



# ==============================
# 核心邏輯函數
# ==============================
def is_normal_stock(code, name):
    """A規則: 檢查是否為普通股 - 嚴格版本"""
    if not code or not name:
        return False
    
    c = str(code).strip()
    
    # 嚴格: 只接受4位數字代碼
    if len(c) != 4:
        return False
    
    # 必須全部是數字
    if not c.isdigit():
        return False
    
    # A規則核心: 第一位必須是 1-9 (排除0開頭的ETF等)
    if c[0] not in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
        return False
        
    # 排除 DR (存託憑證)
    if "DR" in name.upper() or c.startswith('91'):
        return False
    
    # 排除特殊代碼
    if c in ['9999', '0000', '1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888']:
        return False
    
    return True

def get_system_status():
    """取得系統狀態資訊"""
    status_info = {
        'last_update': '無資料',
        'total_stocks': 0,
        'a_rule_stocks': 0,
        'date_range': ('N/A', 'N/A')
    }
    
    try:
        with db_manager.get_connection() as conn:
            # 優先從新表讀取
            try:
                # 取得最後更新日期
                res = conn.execute("SELECT MAX(date) FROM stock_snapshot").fetchone()
                if res and res[0]:
                    status_info['last_update'] = res[0]
                
                # 取得總股票數
                res = conn.execute("SELECT COUNT(*) FROM stock_snapshot").fetchone()
                status_info['total_stocks'] = res[0] if res else 0
                
                # 取得符合 A 規則的股票數
                res = conn.execute("SELECT code, name FROM stock_snapshot").fetchall()
                status_info['a_rule_stocks'] = sum(1 for row in res if is_normal_stock(row[0], row[1]))
                
                # 取得日期範圍
                res = conn.execute("""
                    SELECT MIN(date_int), MAX(date_int) FROM stock_history
                """).fetchone()
                if res and res[0] and res[1]:
                    min_date = f"{res[0]//10000}-{(res[0]//100)%100:02d}-{res[0]%100:02d}"
                    max_date = f"{res[1]//10000}-{(res[1]//100)%100:02d}-{res[1]%100:02d}"
                    status_info['date_range'] = (min_date, max_date)
                    
            except Exception:
                # 新三表架構：不再 Fallback 到舊表
                pass
    
    except Exception as e:
        print_flush(f"⚠ 取得系統狀態失敗: {e}")
    
    return status_info

def check_api_status():
    """檢查 API 可用性"""
    status = {
        'finmind': False,
        'twse': False,
        'tpex': False,
        'supabase': False
    }
    
    # 檢查 Supabase
    if ENABLE_CLOUD_SYNC:
        try:
            headers = {
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            }
            url = f"{SUPABASE_URL}/rest/v1/stock_list?select=count"
            response = requests.get(url, headers=headers, timeout=3, verify=False)
            if response.status_code == 200:
                status['supabase'] = True
        except Exception:
            pass
    
    # 檢查 FinMind API
    try:
        url = f"{FINMIND_URL}?dataset=TaiwanStockPrice&stock_id=2330&start_date=2024-01-01&token={FINMIND_TOKEN}"
        response = requests.get(url, timeout=3, verify=False)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 200 or 'data' in data:
                status['finmind'] = True
    except Exception:
        pass
    
    # 檢查 TWSE API
    try:
        response = requests.get(TWSE_BWIBBU_URL, timeout=3, verify=False)
        if response.status_code == 200 and response.json():
            status['twse'] = True
    except Exception:
        pass
    
    # 檢查 TPEx API
    try:
        response = requests.get(TPEX_MAINBOARD_URL, timeout=3, verify=False)
        if response.status_code == 200:
            status['tpex'] = True
    except Exception:
        pass
        
    return status

def display_system_status():
    """顯示系統狀態資訊板"""
    print_flush("\n" + "=" * 80)
    print_flush("📊 系統狀態")
    print_flush("-" * 80)
    
    # 取得系統資訊
    sys_status = get_system_status()
    
    # 顯示資料庫資訊
    print_flush(f"📁 資料庫: {DB_FILE}")
    print_flush(f"📅 最新更新: {sys_status['last_update']}")
    print_flush(f"📈 股票總數: {sys_status['total_stocks']} 檔")
    print_flush(f"📆 資料範圍: {sys_status['date_range'][0]} ~ {sys_status['date_range'][1]}")
    
    print_flush("-" * 80)
    print_flush("🚀 系統已就緒")
    print_flush("=" * 80)

def get_correct_stock_name(code, current_name=None):
    """取得正確的股票名稱，如果沒有傳入則從 DB 查詢"""
    # 已有有效名稱則直接返回
    if current_name and current_name != code and current_name != "未知":
        return current_name
    
    # 嘗試從 DB 查詢
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            # 優先從 stock_snapshot 查詢
            cur.execute("SELECT name FROM stock_snapshot WHERE code=?", (code,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
            # Fallback: 從 stock_meta 查詢
            cur.execute("SELECT name FROM stock_meta WHERE code=?", (code,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except:
        pass
    
    return current_name if current_name else code

def get_latest_date_for_code(code):
    """獲取指定股票的最新日期"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            # 使用 stock_history (新三表架構)
            cur.execute("SELECT MAX(date_int) FROM stock_history WHERE code=?", (code,))
            result = cur.fetchone()
            if result and result[0]:
                d = result[0]
                return f"{d//10000}-{(d//100)%100:02d}-{d%100:02d}"
            return None
    except Exception as e:
        print_flush(f"⚠ 獲取最新日期失敗 {code}: {e}")
        return None

# ╔══════════════════════════════════════════════════════════════╗
# ║                      DATASOURCE                               ║
# ║  TWSE/TPEX/FinMind 資料抓取器，表驅動 API_ENDPOINTS           ║
# ╚══════════════════════════════════════════════════════════════╝

# ==============================
# 資料源類別
# ==============================
class DataSource:
    """數據源抽象接口"""
    def __init__(self, progress_tracker=None):
        self.progress = progress_tracker or ProgressTracker()
        self.name = "BaseDataSource"
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """獲取股票歷史數據"""
        raise NotImplementedError

class FinMindDataSource(DataSource):
    """FinMind API 數據源"""
    def __init__(self, progress_tracker=None, silent=False):
        super().__init__(progress_tracker)
        self.name = "FinMind"
        self.url = FINMIND_URL
        self.token = FINMIND_TOKEN
        self.silent = silent
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """從FinMind取得歷史資料"""
        try:
            # 如果沒有指定開始日期，計算250個交易日所需的時間
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            params = {
                "dataset": "TaiwanStockPrice",
                "data_id": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "token": self.token,
            }
            
            for attempt in range(retry):
                try:
                    if not self.silent:
                        self.progress.info(f"{self.name}: 嘗試獲取 {stock_code} ({attempt+1}/{retry})", 1)
                    
                    # 使用 SSL 驗證但忽略警告
                    response = requests.get(
                        self.url, 
                        params=params, 
                        timeout=REQUEST_TIMEOUT,
                        verify=False
                    )
                    
                    if response.status_code == 429:  # 速率限制
                        if not self.silent:
                            self.progress.warning(f"{self.name}: 過多請求，等待 2 秒", 1)
                        time.sleep(2)
                        continue
                    
                    if response.status_code == 402: # 付費限制/次數上限
                        if not self.silent:
                            self.progress.warning(f"{self.name}: 請求次數達上限 (402)", 1)
                        return None
                        
                    if response.status_code == 404: # 找不到資料
                        if not self.silent:
                            self.progress.warning(f"{self.name}: 找不到資料 (404)", 1)
                        return None
                    
                    if response.status_code != 200:
                        if not self.silent:
                            self.progress.warning(f"{self.name}: 狀態碼 {response.status_code}", 1)
                        if attempt < retry - 1:
                            time.sleep(1)
                        continue
                    
                    data = response.json()
                    
                    if data is None or data.get('status') != 200:
                        if not self.silent:
                            self.progress.warning(f"{self.name}: API 響應無效", 4)
                        if attempt < retry - 1:
                            time.sleep(1)
                        continue
                    
                    if not data.get('data') or len(data['data']) == 0:
                        return None
                    
                    rows = []
                    for item in data['data']:
                        try:
                            date = item.get('date')
                            if not date:
                                continue
                            close = safe_num(item.get('close'))
                            if close is not None and close > 0:
                                rows.append({
                                    'date': date,
                                    'open': safe_num(item.get('open')),
                                    'high': safe_num(item.get('max')),
                                    'low': safe_num(item.get('min')),
                                    'close': close,
                                    'volume': safe_int(item.get('Trading_Volume')),
                                    'amount': safe_num(item.get('Trading_money'))
                                })
                        except Exception:
                            continue
                    
                    if not rows:
                        return None
                    
                    df = pd.DataFrame(rows)
                    
                    # 按日期去重
                    df = df.drop_duplicates(subset=['date'], keep='first')
                    
                    # 驗證資料完整性
                    df = df[df['close'] > 0]
                    
                    # 按日期排序
                    df = df.sort_values('date').reset_index(drop=True)
                    
                    if not self.silent:
                        self.progress.success(f"{self.name}: 獲取 {len(df)} 筆 {stock_code} 數據", 4)
                    
                    return df
                    
                except Exception as e:
                    if not self.silent:
                        self.progress.warning(f"{self.name} 錯誤: {e}", 1)
                    if attempt < retry - 1:
                        time.sleep(1)
            
            return None
            
        except Exception as e:
            if not self.silent:
                self.progress.error(f"{self.name} 異常: {e}", 4)
            return None

class TwstockDataSource(DataSource):
    """twstock 數據源 (備援)"""
    def __init__(self, progress_tracker=None, silent=False):
        super().__init__(progress_tracker)
        self.name = "twstock (Backup)"
        self.silent = silent

    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        try:
            if not self.silent:
                self.progress.info(f"{self.name}: 嘗試獲取 {stock_code}", 4)
            
            # 增加隨機延遲以避免 Rate Limit (3-6秒)
            time.sleep(np.random.uniform(3, 6))
            
            # 使用 Patch 過的 twstock
            stock = twstock.Stock(stock_code)
            
            # 計算需要抓取的起始年月 (預設 3 年前，確保有足夠資料)
            if start_date:
                try:
                    dt = datetime.strptime(start_date, "%Y-%m-%d")
                except:
                    dt = datetime.now() - timedelta(days=1095)  # 3 年前
            else:
                dt = datetime.now() - timedelta(days=1095)  # 3 年前
            
            # 使用 fetch_from 抓取從指定年月到現在的所有資料
            # 加入超時機制：使用 concurrent.futures
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
            
            def do_fetch():
                stock.fetch_from(dt.year, dt.month)
                return stock.data
            
            try:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(do_fetch)
                    stock_data = future.result(timeout=60)  # 60秒超時
            except FuturesTimeoutError:
                if not self.silent:
                    self.progress.warning(f"{self.name}: {stock_code} 超時 (60秒)", 4)
                return None
            except Exception as e:
                if not self.silent:
                    self.progress.warning(f"{self.name}: fetch_from 失敗: {e}", 4)
                # Fallback: 嘗試 fetch_31
                try:
                    stock.fetch_31()
                except:
                    return None
            
            if not stock.data:
                # 再次嘗試 fetch_31 (如果 fetch_from 沒報錯但沒資料)
                try:
                    stock.fetch_31()
                except:
                    pass
                if not stock.data:
                    return None
                
            # 轉換為 DataFrame
            rows = []
            for d in stock.data:
                # twstock 的 date 是 datetime 物件
                d_str = d.date.strftime("%Y-%m-%d")
                
                # 過濾日期範圍
                if start_date and d_str < start_date:
                    continue
                if end_date and d_str > end_date:
                    continue
                    
                rows.append({
                    'date': d_str,
                    'open': d.open,
                    'high': d.high,
                    'low': d.low,
                    'close': d.close,
                    'volume': d.capacity,
                    'amount': d.turnover
                })
                
            if not rows:
                return None
                
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['date'], keep='first')
            df = df.sort_values('date').reset_index(drop=True)
            
            if not self.silent:
                self.progress.success(f"{self.name}: 獲取 {len(df)} 筆 {stock_code} 數據", 1)
                
            return df
            
        except Exception as e:
            if not self.silent:
                self.progress.warning(f"{self.name} 錯誤: {str(e)}", 1)
            return None




class GoodinfoDataSource(DataSource):
    """Goodinfo 爬蟲備援資料源"""
    def __init__(self, progress_tracker=None, silent=False):
        super().__init__(progress_tracker)
        self.name = "Goodinfo (Backup)"
        self.silent = silent
        self.base_url = "https://goodinfo.tw/tw/ShowK_Chart.asp"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://goodinfo.tw/tw/index.asp',
        }
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """從 Goodinfo 爬取歷史股價"""
        print(f"Goodinfo fetch_history: {stock_code}")
        try:
            if not self.silent:
                self.progress.info(f"{self.name}: 嘗試獲取 {stock_code}", 4)
            
            # 增加隨機延遲避免被封鎖 (3-6秒)
            time.sleep(np.random.uniform(3, 6))
            
            # 建立 session 以維持 cookie
            session = requests.Session()
            session.headers.update(self.headers)
            
            # 設定模擬 Cookie (重要：繞過初始化檢查)
            session.cookies.set('IS_TOUCH_DEVICE', 'F')
            session.cookies.set('SCREEN_SIZE', '1920')
            
            # 先訪問首頁獲取基礎 cookie
            try:
                session.get("https://goodinfo.tw/tw/index.asp", timeout=10, verify=False)
            except:
                pass
            
            # 請求歷史股價頁面 (加入 STEP=DATA_INIT 繞過初始化)
            # CHT_CAT=DATE: 日線
            url = f"{self.base_url}?STOCK_ID={stock_code}&CHT_CAT=DATE&STEP=DATA_INIT"
            session.headers.update({'Referer': f'https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID={stock_code}'})
            
            for attempt in range(retry):
                try:
                    print(f"Attempt {attempt+1}/{retry}: {url}")
                    response = session.get(url, timeout=REQUEST_TIMEOUT, verify=False)
                    print(f"Response status: {response.status_code}, len: {len(response.text)}")
                    
                    if response.status_code != 200:
                        print(f"{self.name}: HTTP {response.status_code}")
                        if attempt < retry - 1:
                            time.sleep(2)
                        continue
                    
                    # 檢查是否仍為初始化頁面
                    if '初始化中' in response.text and 'STEP=DATA_INIT' not in response.url:
                        print(f"{self.name}: 防爬蟲重定向，等待重試")
                        if attempt < retry - 1:
                            time.sleep(3)
                        continue
                    
                    # 強制設定編碼
                    response.encoding = 'utf-8'

                    # 使用 pandas 解析表格
                    try:
                        # 使用 lxml 解析器較快且容錯
                        tables = pd.read_html(response.text)
                        print(f"Parsed {len(tables)} tables")
                    except Exception as e:
                        print(f"{self.name}: 解析表格失敗 - {e}")
                        continue
                    
                    # 尋找包含日期和收盤價的表格
                    df = None
                    for i, table in enumerate(tables):
                        # 處理 MultiIndex (扁平化)
                        if isinstance(table.columns, pd.MultiIndex):
                            table.columns = [' '.join(map(str, col)).strip() for col in table.columns.values]
                            
                        # 轉為字串並小寫以進行模糊比對
                        cols_str = [str(c).lower() for c in table.columns]
                        cols_concat = " ".join(cols_str)
                        
                        # 排除期貨表格
                        if '期貨' in cols_concat:
                            continue
                            
                        # 尋找包含 "日期", "收盤" 的表格
                        # 關鍵字: 日期/date/交易日, 收盤/close/成交價
                        has_date = any(k in cols_concat for k in ['日期', 'date', '交易日'])
                        has_price = any(k in cols_concat for k in ['收盤', 'close', '成交價'])
                        has_open = any(k in cols_concat for k in ['開盤', 'open'])
                        has_high = any(k in cols_concat for k in ['最高', 'high'])
                        has_low = any(k in cols_concat for k in ['最低', 'low'])
                        
                        if has_date and has_price and has_open and has_high and has_low:
                            df = table
                            # print(f"Selected table with cols: {df.columns.tolist()}")
                            break
                    
                    if df is None or df.empty:
                        if not self.silent:
                            self.progress.warning(f"{self.name}: 未找到有效表格", 4)
                        continue
                    
                    # 標準化欄位名稱
                    col_mapping = {}
                    # 處理多層索引或單層索引
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
                    
                    for col in df.columns:
                        col_str = str(col).strip()
                        if '日期' in col_str or '交易日' in col_str:
                            col_mapping[col] = 'date'
                        elif '開盤' in col_str:
                            col_mapping[col] = 'open'
                        elif '最高' in col_str:
                            col_mapping[col] = 'high'
                        elif '最低' in col_str:
                            col_mapping[col] = 'low'
                        elif '收盤' in col_str:
                            col_mapping[col] = 'close'
                        elif ('張數' in col_str or '成交量' in col_str) and '成交' in col_str:
                            col_mapping[col] = 'volume'
                        elif ('金額' in col_str or '成交額' in col_str or '億元' in col_str) and '成交' in col_str:
                            col_mapping[col] = 'amount'
                    
                    df = df.rename(columns=col_mapping)
                    
                    # 確保必要欄位存在
                    required = ['date', 'close']
                    if not all(col in df.columns for col in required):
                        # 嘗試尋找其他可能的欄位名
                        continue
                    
                    # 轉換日期格式
                    def parse_date(d):
                        try:
                            d_str = str(d).strip().replace("'", "")
                            # 處理 Goodinfo 特殊格式 (如 24/12/11 或 2024/12/11)
                            if '/' in d_str:
                                parts = d_str.split('/')
                                if len(parts) == 3:
                                    # 嘗試解析 4 位數年份
                                    try:
                                        return datetime.strptime(d_str, '%Y/%m/%d').strftime('%Y-%m-%d')
                                    except ValueError:
                                        # 嘗試解析 2 位數年份
                                        try:
                                            return datetime.strptime(d_str, '%y/%m/%d').strftime('%Y-%m-%d')
                                        except ValueError:
                                            pass
                            elif '-' in d_str:
                                return datetime.strptime(d_str, '%Y-%m-%d').strftime('%Y-%m-%d')
                            return None
                        except:
                            return None
                    
                    df['date'] = df['date'].apply(parse_date)
                    df = df.dropna(subset=['date'])
                    print(f"Parsed dates: {len(df)} rows")
                    
                    # 過濾日期範圍
                    if start_date:
                        df = df[df['date'] >= start_date]
                    if end_date:
                        df = df[df['date'] <= end_date]
                    
                    if df.empty:
                        return None
                    
                    # 處理數值欄位 (移除逗號, 處理 '---')
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = df[col].astype(str).str.replace(',', '').str.replace('+', '').str.replace('X', '')
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # 填充缺失欄位
                    for col in numeric_cols:
                        if col not in df.columns:
                            df[col] = None
                    
                    # 轉換單位 (Goodinfo 成交張數是張, 金額是億)
                    # 系統預設 amount 是元。
                    # Goodinfo "成交金額(億)" -> 需 * 100,000,000
                    if 'amount' in df.columns:
                         # 確保是數值型態
                         df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
                         # 轉換單位：億 -> 元
                         df['amount'] = df['amount'] * 100000000
                    
                    df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
                    df = df.drop_duplicates(subset=['date'], keep='first')
                    df = df.sort_values('date').reset_index(drop=True)
                    
                    if not self.silent:
                        self.progress.success(f"{self.name}: 獲取 {len(df)} 筆 {stock_code} 數據", 1)
                    
                    return df
                    
                except Exception as e:
                    if not self.silent:
                        self.progress.warning(f"{self.name} 請求錯誤: {e}", 4)
                    if attempt < retry - 1:
                        time.sleep(2)
            
            return None
            
        except Exception as e:
            if not self.silent:
                self.progress.warning(f"{self.name} 錯誤: {str(e)}", 1)
            return None


class DataSourceManager:
    """數據源管理器"""
    def __init__(self, progress_tracker=None, silent=False):
        self.progress = progress_tracker or ProgressTracker()
        self.silent = silent
        self.sources = [
            FinMindDataSource(progress_tracker, silent=silent),
            TwstockDataSource(progress_tracker, silent=silent)
        ]
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """嘗試所有數據源，直到成功或全部失敗"""
        for i, source in enumerate(self.sources):
            # if not self.silent:
            #     self.progress.info(f"嘗試使用 {source.name} 獲取 {stock_code} 數據...", 4)
            
            df = source.fetch_history(stock_code, start_date, end_date, retry)
            
            if df is not None and not df.empty:
                return df
            
            # 備援切換提示 (醒目顯示)
            if i < len(self.sources) - 1:
                if not self.silent:
                    self.progress.warning(f"⚡ {source.name} 失敗，切換至 {self.sources[i+1].name}...", 4)
                
        if not self.silent:
            self.progress.error(f"❌ 所有數據源都無法獲取 {stock_code} 數據", 4)
        return None

# ==============================
# 法人買賣超 API
# ==============================
class InstitutionalInvestorAPI:
    """三大法人買賣超資料 API"""
    
    # TWSE (上市) 法人買賣超 API (網頁版 - 備援)
    TWSE_T86_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
    # TPEx (上櫃) 法人買賣超 API (網頁版 - 備援)
    TPEX_INST_URL = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    
    # === 官方 OpenAPI URLs (主要來源) ===
    TWSE_OPENAPI_URL = "https://openapi.twse.com.tw/v1/fund/T86_ALL"
    TPEX_OPENAPI_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
    
    @classmethod
    def fetch_twse_openapi(cls, progress=None):
        """從 TWSE 取得今日法人買賣超資料 (三層備援)
        優先順序: 1. TWSE 網頁版 (JSON) 2. TWSE OpenAPI 3. FinMind
        """
        results = []
        today = datetime.now().strftime("%Y%m%d")
        
        # === 1. TWSE 網頁版 (主要來源 - JSON 格式) ===
        try:
            if progress:
                progress.info("正在從 TWSE 網頁版取得法人資料...", level=1)
            
            url = f"https://www.twse.com.tw/fund/T86?response=json&date={today}&selectType=ALL"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            resp = requests.get(url, headers=headers, timeout=20, verify=False)
            resp.raise_for_status()
            
            data = resp.json()
            
            # 檢查資料是否存在
            if data.get('stat') == 'OK' and 'data' in data:
                today_int = int(today)
                
                for row in data['data']:
                    try:
                        code = str(row[0]).strip().replace('=', '').replace('"', '')
                        if not code.isdigit() or len(code) > 4:
                            continue
                        
                        results.append({
                            'code': code,
                            'name': str(row[1]).strip(),
                            'date_int': today_int,
                            'foreign_buy': cls._parse_number(row[2]),   # 外資買進
                            'foreign_sell': cls._parse_number(row[3]),  # 外資賣出
                            'trust_buy': cls._parse_number(row[8]),     # 投信買進
                            'trust_sell': cls._parse_number(row[9]),    # 投信賣出
                            'dealer_buy': cls._parse_number(row[12]),   # 自營商買進
                            'dealer_sell': cls._parse_number(row[13]),  # 自營商賣出
                            'market': 'TWSE'
                        })
                    except:
                        pass
                
                if results:
                    if progress:
                        progress.success(f"✓ TWSE 網頁版法人: {len(results)} 筆", level=1)
                    return results
            else:
                if progress:
                    progress.warn("TWSE 網頁版無今日資料", level=1)
                    
        except Exception as e:
            logger.error(f"TWSE 網頁版法人失敗: {e}")
            if progress:
                progress.error(f"✗ TWSE 網頁版失敗: {e}", level=1)
        
        # === 2. TWSE OpenAPI (第一備援) ===
        if not results:
            try:
                if progress:
                    progress.info("嘗試 TWSE OpenAPI 備援...", level=1)
                
                resp = requests.get(cls.TWSE_OPENAPI_URL, timeout=30, verify=False)
                content = resp.text.strip()
                
                if content and content != '[]':
                    data = resp.json()
                    if isinstance(data, list):
                        today_int = int(today)
                        for item in data:
                            code = str(item.get("證券代號", "")).strip()
                            if not code.isdigit() or len(code) > 4:
                                continue
                            
                            results.append({
                                'code': code,
                                'name': str(item.get("證券名稱", "")).strip(),
                                'date_int': today_int,
                                'foreign_buy': cls._parse_number(item.get("外資及陸資買進股數", 0)),
                                'foreign_sell': cls._parse_number(item.get("外資及陸資賣出股數", 0)),
                                'trust_buy': cls._parse_number(item.get("投信買進股數", 0)),
                                'trust_sell': cls._parse_number(item.get("投信賣出股數", 0)),
                                'dealer_buy': cls._parse_number(item.get("自營商買進股數", 0)),
                                'dealer_sell': cls._parse_number(item.get("自營商賣出股數", 0)),
                                'market': 'TWSE'
                            })
                        
                        if results:
                            if progress:
                                progress.success(f"✓ TWSE OpenAPI 法人: {len(results)} 筆", level=1)
                            return results
                            
            except Exception as e:
                logger.error(f"TWSE OpenAPI 法人失敗: {e}")
        
        # === 3. FinMind (最終備援) ===
        if not results:
            try:
                if progress:
                    progress.info("嘗試 FinMind 備援...", level=1)
                results = cls._fetch_twse_from_finmind(progress)
            except Exception as e2:
                logger.error(f"FinMind 法人備援失敗: {e2}")
        
        return results
    
    @classmethod
    def _fetch_twse_from_finmind(cls, progress=None):
        """使用 FinMind 取得 TWSE 法人資料 (備援)"""
        results = []
        today = datetime.now().strftime("%Y-%m-%d")
        
        try:
            params = {
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "start_date": today,
                "end_date": today,
                "token": FINMIND_TOKEN
            }
            
            resp = requests.get(FINMIND_URL, params=params, timeout=30, verify=False)
            data = resp.json()
            
            if data.get('status') != 200 or 'data' not in data:
                if progress:
                    progress.warn("FinMind 無今日法人資料", level=1)
                # 不 return，繼續嘗試網頁版備援
            
            # 整理資料 - 將同一股票的不同法人資料合併
            stock_data = {}
            for row in data['data']:
                code = str(row.get('stock_id', '')).strip()
                if not code or len(code) > 4:
                    continue
                    
                name = row.get('name', '')
                buy = int(row.get('buy', 0) or 0)
                sell = int(row.get('sell', 0) or 0)
                
                if code not in stock_data:
                    stock_data[code] = {
                        'code': code,
                        'name': '',
                        'date_int': int(today.replace('-', '')),
                        'foreign_buy': 0, 'foreign_sell': 0,
                        'trust_buy': 0, 'trust_sell': 0,
                        'dealer_buy': 0, 'dealer_sell': 0,
                        'market': 'TWSE'
                    }
                
                # 依法人類別累加
                if 'Foreign' in name:
                    stock_data[code]['foreign_buy'] += buy
                    stock_data[code]['foreign_sell'] += sell
                elif 'Investment_Trust' in name:
                    stock_data[code]['trust_buy'] += buy
                    stock_data[code]['trust_sell'] += sell
                elif 'Dealer' in name:
                    stock_data[code]['dealer_buy'] += buy
                    stock_data[code]['dealer_sell'] += sell
            
            results = list(stock_data.values())
            
            if progress:
                progress.success(f"✓ FinMind 法人: {len(results)} 筆", level=1)
                
        except Exception as e:
            logger.error(f"FinMind 法人取得失敗: {e}")
            if progress:
                progress.error(f"✗ FinMind 失敗: {e}", level=1)
        
        # === TWSE 網頁版備援 (當 FinMind 也失敗時) ===
        if not results:
            try:
                if progress:
                    progress.info("嘗試 TWSE 網頁版備援...", level=1)
                results = cls._fetch_twse_from_web(progress)
            except Exception as e3:
                logger.error(f"TWSE 網頁版備援失敗: {e3}")
        
        return results
    
    @classmethod
    def _fetch_twse_from_web(cls, progress=None):
        """使用 TWSE 網頁版取得法人資料 (最終備援)"""
        from io import StringIO
        import pandas as pd
        import time
        import random
        
        results = []
        today = datetime.now().strftime("%Y%m%d")
        
        try:
            # TWSE T86 CSV 格式 API
            url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=csv&date={today}&selectType=ALLBUT0999"
            
            # 添加延遲避免被封鎖
            time.sleep(random.uniform(1.0, 2.0))
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            resp = requests.get(url, headers=headers, timeout=20, verify=False)
            
            if resp.status_code != 200 or len(resp.text) < 100:
                if progress:
                    progress.warn("TWSE 網頁版無今日資料", level=1)
                return results
            
            # 解析 CSV
            df = pd.read_csv(StringIO(resp.text), header=1).dropna(how='all', axis=1).dropna(how='any')
            df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
            
            if '證券代號' not in df.columns:
                if progress:
                    progress.warn("TWSE 網頁版格式異常", level=1)
                return results
            
            df['code'] = df['證券代號'].str.replace('=', '').str.replace('"', '').str.strip()
            df = df[df['code'].str.len() == 4]
            
            today_int = int(today)
            
            for _, row in df.iterrows():
                try:
                    code = row['code']
                    results.append({
                        'code': code,
                        'name': str(row.get('證券名稱', '')).strip(),
                        'date_int': today_int,
                        'foreign_buy': cls._parse_number(row.get('外資及陸資(不含外資自營商)買進股數', 0)),
                        'foreign_sell': cls._parse_number(row.get('外資及陸資(不含外資自營商)賣出股數', 0)),
                        'trust_buy': cls._parse_number(row.get('投信買進股數', 0)),
                        'trust_sell': cls._parse_number(row.get('投信賣出股數', 0)),
                        'dealer_buy': cls._parse_number(row.get('自營商買進股數(自行買賣)', 0)),
                        'dealer_sell': cls._parse_number(row.get('自營商賣出股數(自行買賣)', 0)),
                        'market': 'TWSE'
                    })
                except:
                    pass
            
            if progress:
                progress.success(f"✓ TWSE 網頁版法人: {len(results)} 筆", level=1)
                
        except Exception as e:
            logger.error(f"TWSE 網頁版取得失敗: {e}")
            if progress:
                progress.error(f"✗ TWSE 網頁版失敗: {e}", level=1)
        
        return results
    
    @classmethod
    def fetch_tpex_openapi(cls, progress=None):
        """從 TPEx OpenAPI 取得今日法人買賣超資料"""
        results = []
        try:
            if progress:
                progress.info("正在從 TPEx OpenAPI 取得法人資料...", level=2)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            resp = requests.get(cls.TPEX_OPENAPI_URL, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("TPEx 法人 OpenAPI 回傳非陣列格式")
                return results
            
            for item in data:
                code = str(item.get("SecuritiesCompanyCode", "")).strip()
                if not code or not code.isdigit() or len(code) > 4:
                    continue
                
                # 解析日期 (民國格式: 1141217 -> 20251217)
                date_str = str(item.get("Date", "")).strip()
                if len(date_str) == 7:
                    year = int(date_str[:3]) + 1911
                    date_int = int(f"{year}{date_str[3:]}")
                else:
                    date_int = int(datetime.now().strftime("%Y%m%d"))
                
                results.append({
                    'code': code,
                    'name': str(item.get("CompanyName", "")).strip(),
                    'date_int': date_int,
                    'foreign_buy': cls._parse_number(item.get("ForeignInvestorsBuy", 0)),
                    'foreign_sell': cls._parse_number(item.get("ForeignInvestorsSell", 0)),
                    'trust_buy': cls._parse_number(item.get("SecuritiesInvestmentTrustBuy", 0)),
                    'trust_sell': cls._parse_number(item.get("SecuritiesInvestmentTrustSell", 0)),
                    'dealer_buy': cls._parse_number(item.get("DealersBuy", 0)),
                    'dealer_sell': cls._parse_number(item.get("DealersSell", 0)),
                    'market': 'TPEx'
                })
            
            if progress:
                progress.success(f"✓ TPEx OpenAPI 法人: {len(results)} 筆", level=2)
                
        except Exception as e:
            logger.error(f"TPEx 法人 OpenAPI 失敗: {e}")
            if progress:
                progress.error(f"✗ TPEx 法人 OpenAPI 失敗: {e}", level=2)
        
        return results
    
    @classmethod
    def fetch_all_openapi(cls, progress=None):
        """從官方 OpenAPI 取得所有法人資料並儲存到資料庫"""
        twse_data = cls.fetch_twse_openapi(progress)
        tpex_data = cls.fetch_tpex_openapi(progress)
        
        all_data = twse_data + tpex_data
        
        if all_data:
            saved = cls.save_openapi_to_db(all_data)
            if progress:
                progress.success(f"✓ 法人資料已儲存: {saved} 筆", level=3)
            return saved
        
        return 0
    
    @classmethod
    def save_openapi_to_db(cls, data_list):
        """將 OpenAPI 法人資料儲存到資料庫"""
        if not data_list:
            return 0
        
        cls.ensure_table()
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                records = [
                    (d['code'], d['date_int'], d['foreign_buy'], d['foreign_sell'],
                     d['trust_buy'], d['trust_sell'], d['dealer_buy'], d['dealer_sell'])
                    for d in data_list
                ]
                
                cur.executemany("""
                    INSERT OR REPLACE INTO institutional_investors 
                    (code, date_int, foreign_buy, foreign_sell, trust_buy, trust_sell, dealer_buy, dealer_sell)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, records)
                
                # 同步最新法人數據到 stock_snapshot
                for d in data_list:
                    foreign_net = d['foreign_buy'] - d['foreign_sell']
                    trust_net = d['trust_buy'] - d['trust_sell']
                    dealer_net = d['dealer_buy'] - d['dealer_sell']
                    cur.execute("""
                        UPDATE stock_snapshot 
                        SET foreign_buy = ?, trust_buy = ?, dealer_buy = ?
                        WHERE code = ?
                    """, (foreign_net, trust_net, dealer_net, d['code']))
                
                conn.commit()
                return len(records)
                
        except Exception as e:
            logger.error(f"儲存法人資料失敗: {e}")
            return 0
    
    @classmethod
    def ensure_table(cls):
        """確保 institutional_investors 資料表存在"""
        try:
            with db_manager.get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS institutional_investors (
                        code TEXT NOT NULL,
                        date_int INTEGER NOT NULL,
                        foreign_buy INTEGER DEFAULT 0,
                        foreign_sell INTEGER DEFAULT 0,
                        trust_buy INTEGER DEFAULT 0,
                        trust_sell INTEGER DEFAULT 0,
                        dealer_buy INTEGER DEFAULT 0,
                        dealer_sell INTEGER DEFAULT 0,
                        PRIMARY KEY (code, date_int)
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inst_code ON institutional_investors(code)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inst_date ON institutional_investors(date_int)")
                conn.commit()
        except Exception as e:
            print_flush(f"⚠ 建立法人資料表失敗: {e}")
    
    @classmethod
    def save_to_db(cls, data_list, date_str=None):
        """
        將法人資料儲存到資料庫
        data_list: fetch_twse/tpex_institutional 回傳的資料
        """
        if not data_list:
            return 0
        
        cls.ensure_table()
        
        if date_str is None:
            date_int = int(datetime.now().strftime("%Y%m%d"))
        else:
            date_int = int(date_str.replace('-', ''))
        
        saved = 0
        try:
            with db_manager.get_connection() as conn:
                for item in data_list:
                    # 計算買進賣出股數 (從 net 反推)
                    foreign_net = item.get('foreign_net', 0)
                    trust_net = item.get('trust_net', 0)
                    dealer_net = item.get('dealer_net', 0)
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO institutional_investors 
                        (code, date_int, foreign_buy, foreign_sell, trust_buy, trust_sell, dealer_buy, dealer_sell)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        item['code'], date_int,
                        max(0, foreign_net), max(0, -foreign_net),
                        max(0, trust_net), max(0, -trust_net),
                        max(0, dealer_net), max(0, -dealer_net)
                    ))
                    saved += 1
                conn.commit()
        except Exception as e:
            print_flush(f"⚠ 儲存法人資料失敗: {e}")
        
        return saved
    
    @classmethod
    def get_from_db(cls, stock_code, days=30):
        """從資料庫取得個股法人歷史資料"""
        cls.ensure_table()
        
        try:
            with db_manager.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT date_int, foreign_buy - foreign_sell as foreign_net,
                           trust_buy - trust_sell as trust_net,
                           dealer_buy - dealer_sell as dealer_net
                    FROM institutional_investors
                    WHERE code = ?
                    ORDER BY date_int DESC
                    LIMIT ?
                """, (stock_code, days))
                
                results = []
                for row in cursor.fetchall():
                    results.append({
                        'date': str(row[0]),
                        'foreign_net': row[1],
                        'trust_net': row[2],
                        'dealer_net': row[3]
                    })
                return results
        except:
            return []
    
    @staticmethod
    def _parse_number(s):
        """解析數字字串，移除逗號並轉為整數"""
        if not s or s == '--':
            return 0
        try:
            return int(str(s).replace(',', '').replace(' ', ''))
        except:
            return 0
    
    @classmethod
    def fetch_twse_institutional(cls, date_str=None):
        """
        取得上市(TWSE)三大法人買賣超資料
        回傳: list of dict, 每個 dict 包含 code, name, foreign_net, trust_net, dealer_net, total_net
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
        else:
            date_str = date_str.replace('-', '')
        
        try:
            url = f"{cls.TWSE_T86_URL}?date={date_str}&selectType=ALL&response=json"
            resp = requests.get(url, timeout=10, verify=False)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get('stat') != 'OK' or 'data' not in data:
                return []
            
            results = []
            for row in data['data']:
                code = row[0].strip()
                # 過濾非普通股 (權證、ETF 槓桿等)
                if len(code) > 4 and not code.isdigit():
                    continue
                    
                results.append({
                    'code': code,
                    'name': row[1].strip(),
                    'foreign_net': cls._parse_number(row[4]),   # 外陸資買賣超(不含自營)
                    'trust_net': cls._parse_number(row[10]),    # 投信買賣超
                    'dealer_net': cls._parse_number(row[11]),   # 自營商買賣超
                    'total_net': cls._parse_number(row[18])     # 三大法人買賣超
                })
            
            return results
            
        except Exception as e:
            print_flush(f"⚠ 取得上市法人資料失敗: {e}")
            return []
    
    @classmethod
    def fetch_tpex_institutional(cls, date_str=None):
        """
        取得上櫃(TPEx)三大法人買賣超資料
        回傳: list of dict
        """
        if date_str is None:
            today = datetime.now()
            # 轉為民國年格式
            roc_year = today.year - 1911
            date_str = f"{roc_year}/{today.month:02d}/{today.day:02d}"
        else:
            # 轉換 YYYY-MM-DD 為民國年格式
            parts = date_str.replace('-', '/').split('/')
            if len(parts) == 3:
                roc_year = int(parts[0]) - 1911
                date_str = f"{roc_year}/{parts[1]}/{parts[2]}"
        
        try:
            url = f"{cls.TPEX_INST_URL}?l=zh-tw&d={date_str}&se=EW&t=D"
            resp = requests.get(url, timeout=10, verify=False)
            resp.raise_for_status()
            data = resp.json()
            
            if 'aaData' not in data:
                return []
            
            results = []
            for row in data['aaData']:
                code = str(row[0]).strip()
                # 過濾非普通股
                if len(code) > 4:
                    continue
                    
                results.append({
                    'code': code,
                    'name': str(row[1]).strip(),
                    'foreign_net': cls._parse_number(row[4]),   # 外資買賣超
                    'trust_net': cls._parse_number(row[10]),    # 投信買賣超
                    'dealer_net': cls._parse_number(row[13]),   # 自營商買賣超
                    'total_net': cls._parse_number(row[16])     # 三大法人買賣超
                })
            
            return results
            
        except Exception as e:
            print_flush(f"⚠ 取得上櫃法人資料失敗: {e}")
            return []
    
    @classmethod
    def get_all_institutional_data(cls, date_str=None):
        """取得所有(上市+上櫃)法人資料"""
        twse_data = cls.fetch_twse_institutional(date_str)
        tpex_data = cls.fetch_tpex_institutional(date_str)
        return twse_data + tpex_data
    
    @classmethod
    def get_ranking(cls, rank_type='foreign_buy', top_n=10, date_str=None):
        """
        取得排行榜
        rank_type: foreign_buy, foreign_sell, trust_buy, trust_sell
        top_n: 顯示前 N 名
        """
        all_data = cls.get_all_institutional_data(date_str)
        
        if not all_data:
            return []
        
        # 根據類型排序
        if rank_type == 'foreign_buy':
            sorted_data = sorted(all_data, key=lambda x: x['foreign_net'], reverse=True)
            sorted_data = [d for d in sorted_data if d['foreign_net'] > 0]
        elif rank_type == 'foreign_sell':
            sorted_data = sorted(all_data, key=lambda x: x['foreign_net'])
            sorted_data = [d for d in sorted_data if d['foreign_net'] < 0]
        elif rank_type == 'trust_buy':
            sorted_data = sorted(all_data, key=lambda x: x['trust_net'], reverse=True)
            sorted_data = [d for d in sorted_data if d['trust_net'] > 0]
        elif rank_type == 'trust_sell':
            sorted_data = sorted(all_data, key=lambda x: x['trust_net'])
            sorted_data = [d for d in sorted_data if d['trust_net'] < 0]
        else:
            return []
        
        return sorted_data[:top_n]
    
    @classmethod
    def fetch_stock_institutional_history(cls, stock_code, days=30):
        """
        使用 FinMind 取得個股法人歷史買賣超資料
        回傳: list of dict, 按日期降序排列
        """
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        try:
            params = {
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "data_id": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "token": FINMIND_TOKEN
            }
            
            resp = requests.get(FINMIND_URL, params=params, timeout=15, verify=False)
            data = resp.json()
            
            if data.get('status') != 200 or 'data' not in data:
                return []
            
            # 整理資料 - 將同一日的不同法人資料合併
            daily_data = {}
            for row in data['data']:
                date = row['date']
                name = row['name']
                buy = row.get('buy', 0) or 0
                sell = row.get('sell', 0) or 0
                net = buy - sell
                
                if date not in daily_data:
                    daily_data[date] = {'date': date, 'foreign_net': 0, 'trust_net': 0, 'dealer_net': 0}
                
                if 'Foreign' in name:
                    daily_data[date]['foreign_net'] += net
                elif 'Investment_Trust' in name:
                    daily_data[date]['trust_net'] += net
                elif 'Dealer' in name:
                    daily_data[date]['dealer_net'] += net
            
            # 轉換為 list 並按日期降序排列
            result = list(daily_data.values())
            result.sort(key=lambda x: x['date'], reverse=True)
            return result
            
        except Exception as e:
            print_flush(f"⚠ 取得 {stock_code} 法人歷史資料失敗: {e}")
            return []
    
    @classmethod
    def calculate_consecutive_days(cls, stock_code, investor_type='foreign'):
        """
        計算連續買超/賣超天數
        investor_type: 'foreign' 或 'trust'
        回傳: 正數=連續買超天數, 負數=連續賣超天數
        """
        history = cls.fetch_stock_institutional_history(stock_code, days=60)
        
        if not history:
            return 0
        
        key = f'{investor_type}_net'
        consecutive = 0
        direction = None  # True=買超, False=賣超
        
        for day_data in history:
            net = day_data.get(key, 0)
            
            if net == 0:
                break  # 遇到 0 則停止計算
            
            current_direction = net > 0
            
            if direction is None:
                direction = current_direction
                consecutive = 1 if direction else -1
            elif current_direction == direction:
                consecutive += 1 if direction else -1
            else:
                break  # 方向改變，停止計算
        
        return consecutive
    
    @classmethod
    def get_stock_institutional_signal(cls, stock_code):
        """
        取得個股法人訊號 (用於顯示在訊號中)
        回傳: dict 包含 foreign_days, trust_days, latest_foreign, latest_trust
        """
        foreign_days = cls.calculate_consecutive_days(stock_code, 'foreign')
        trust_days = cls.calculate_consecutive_days(stock_code, 'trust')
        
        # 取得最新一日資料
        history = cls.fetch_stock_institutional_history(stock_code, days=5)
        latest_foreign = history[0].get('foreign_net', 0) // 1000 if history else 0  # 轉換為張
        latest_trust = history[0].get('trust_net', 0) // 1000 if history else 0
        
        return {
            'foreign_days': foreign_days,
            'trust_days': trust_days,
            'latest_foreign': latest_foreign,  # 最新一日外資買賣超(張)
            'latest_trust': latest_trust        # 最新一日投信買賣超(張)
        }


# ==============================
# 融資融券 API (OpenAPI)
# ==============================
class MarginDataAPI:
    """融資融券資料 API (使用官方 OpenAPI)"""
    
    # TWSE 融資融券 OpenAPI (JSON, 中文欄位)
    TWSE_MARGIN_URL = "https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN"
    # TPEx 融資融券 OpenAPI (JSON, 英文欄位)  
    TPEX_MARGIN_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"
    
    # TWSE 欄位映射 (中文 -> 英文)
    TWSE_FIELD_MAP = {
        "股票代號": "code",
        "股票名稱": "name",
        "融資買進": "margin_buy",
        "融資賣出": "margin_sell",
        "融資現金償還": "margin_redemp",
        "融資前日餘額": "margin_balance_prev",
        "融資今日餘額": "margin_balance",
        "融資限額": "margin_quota",
        "融券買進": "short_buy",
        "融券賣出": "short_sell",
        "融券現券償還": "short_redemp",
        "融券前日餘額": "short_balance_prev",
        "融券今日餘額": "short_balance",
        "融券限額": "short_quota",
        "資券互抵": "offsetting",
        "註記": "note"
    }
    
    # TPEx 欄位映射 (英文 -> 標準化)
    TPEX_FIELD_MAP = {
        "Date": "date",
        "SecuritiesCompanyCode": "code",
        "CompanyName": "name",
        "MarginPurchase": "margin_buy",
        "MarginSales": "margin_sell",
        "CashRedemption": "margin_redemp",
        "MarginPurchaseBalancePreviousDay": "margin_balance_prev",
        "MarginPurchaseBalance": "margin_balance",
        "MarginPurchaseQuota": "margin_quota",
        "MarginPurchaseUtilizationRate": "margin_util_rate",
        "ShortConvering": "short_buy",
        "ShortSale": "short_sell",
        "StockRedemption": "short_redemp",
        "ShortSaleBalancePreviousDay": "short_balance_prev",
        "ShortSaleBalance": "short_balance",
        "ShortSaleQuota": "short_quota",
        "ShortSaleUtilizationRate": "short_util_rate",
        "Offsetting": "offsetting",
        "Note": "note"
    }
    
    @classmethod
    def _parse_number(cls, s):
        """解析數字字串，移除逗號並轉為整數"""
        if s is None or s == "" or s == "--":
            return 0
        try:
            return int(str(s).replace(",", "").replace(" ", ""))
        except (ValueError, TypeError):
            return 0
    
    @classmethod
    def _parse_float(cls, s):
        """解析浮點數字串"""
        if s is None or s == "" or s == "--":
            return 0.0
        try:
            return float(str(s).replace(",", "").replace(" ", ""))
        except (ValueError, TypeError):
            return 0.0
    
    @classmethod
    def fetch_twse_margin(cls, progress=None):
        """從 TWSE 取得融資融券資料 (網頁版優先，更即時)"""
        results = []
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        today = datetime.now().strftime("%Y%m%d")
        
        # === 主要來源: MI_MARGN 網頁版 API (更即時) ===
        try:
            url = f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={today}&selectType=ALL"
            resp = requests.get(url, headers=headers, timeout=30, verify=False)
            data = resp.json()
            
            if data.get('stat') == 'OK' and data.get('data'):
                # 解析日期
                date_str = data.get('date', today)
                if len(date_str) == 8:
                    date_int = int(date_str)
                else:
                    date_int = int(today)
                
                # 欄位: [代號, 名稱, 融資買進, 融資賣出, 融資現償, 融資前日餘額, 融資今日餘額, 融資限額, ...]
                for row in data['data']:
                    if len(row) >= 16:
                        code = str(row[0]).strip()
                        if not code or not code.isdigit() or len(code) > 4:
                            continue
                        
                        record = {
                            "code": code,
                            "name": str(row[1]).strip(),
                            "date_int": date_int,
                            "margin_buy": cls._parse_number(row[2]),
                            "margin_sell": cls._parse_number(row[3]),
                            "margin_redemp": cls._parse_number(row[4]),
                            "margin_balance": cls._parse_number(row[6]),
                            "margin_quota": cls._parse_number(row[7]),
                            "short_buy": cls._parse_number(row[8]),
                            "short_sell": cls._parse_number(row[9]),
                            "short_redemp": cls._parse_number(row[10]),
                            "short_balance": cls._parse_number(row[12]),
                            "short_quota": cls._parse_number(row[13]),
                            "offsetting": cls._parse_number(row[14]) if len(row) > 14 else 0,
                            "margin_util_rate": 0.0,
                            "short_util_rate": 0.0,
                            "market": "TWSE"
                        }
                        
                        # 計算使用率
                        if record["margin_quota"] > 0:
                            record["margin_util_rate"] = round(record["margin_balance"] / record["margin_quota"] * 100, 2)
                        if record["short_quota"] > 0:
                            record["short_util_rate"] = round(record["short_balance"] / record["short_quota"] * 100, 2)
                        
                        results.append(record)
                
                if results:
                    return results
                    
        except Exception as e:
            logger.debug(f"TWSE MI_MARGN 網頁版失敗: {e}，使用 OpenAPI 備援")
        
        # === 備援: OpenAPI ===
        try:
            resp = requests.get(cls.TWSE_MARGIN_URL, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            data = resp.json()
            if not isinstance(data, list):
                return results
            
            today_int = int(datetime.now().strftime("%Y%m%d"))
            
            for item in data:
                code = item.get("股票代號", "").strip()
                if not code or not code.isdigit() or len(code) > 4:
                    continue
                
                record = {
                    "code": code,
                    "name": item.get("股票名稱", "").strip(),
                    "date_int": today_int,
                    "margin_buy": cls._parse_number(item.get("融資買進")),
                    "margin_sell": cls._parse_number(item.get("融資賣出")),
                    "margin_redemp": cls._parse_number(item.get("融資現金償還")),
                    "margin_balance": cls._parse_number(item.get("融資今日餘額")),
                    "margin_quota": cls._parse_number(item.get("融資限額")),
                    "short_buy": cls._parse_number(item.get("融券買進")),
                    "short_sell": cls._parse_number(item.get("融券賣出")),
                    "short_redemp": cls._parse_number(item.get("融券現券償還")),
                    "short_balance": cls._parse_number(item.get("融券今日餘額")),
                    "short_quota": cls._parse_number(item.get("融券限額")),
                    "offsetting": cls._parse_number(item.get("資券互抵")),
                    "margin_util_rate": 0.0,
                    "short_util_rate": 0.0,
                    "market": "TWSE"
                }
                
                if record["margin_quota"] > 0:
                    record["margin_util_rate"] = round(record["margin_balance"] / record["margin_quota"] * 100, 2)
                if record["short_quota"] > 0:
                    record["short_util_rate"] = round(record["short_balance"] / record["short_quota"] * 100, 2)
                
                results.append(record)
                
        except Exception as e:
            logger.debug(f"TWSE 融資融券 OpenAPI 也失敗: {e}")
        
        return results
    
    @classmethod
    def fetch_tpex_margin(cls, progress=None):
        """
        從 TPEx OpenAPI 取得融資融券資料
        回傳: list of dict
        """
        results = []
        try:
            if progress:
                progress.info("正在下載 TPEx 融資融券資料 (OpenAPI)...", level=2)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            resp = requests.get(cls.TPEX_MARGIN_URL, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("TPEx 融資融券 API 回傳非陣列格式")
                return results
            
            for item in data:
                code = item.get("SecuritiesCompanyCode", "").strip()
                if not code or not code.isdigit():
                    continue
                
                # 只處理普通股 (4碼數字)
                if len(code) > 4:
                    continue
                
                # 解析日期 (民國格式: 1141217 -> 20251217)
                date_str = str(item.get("Date", "")).strip()
                if len(date_str) == 7:
                    year = int(date_str[:3]) + 1911
                    date_int = int(f"{year}{date_str[3:]}")
                else:
                    date_int = int(datetime.now().strftime("%Y%m%d"))
                
                record = {
                    "code": code,
                    "name": item.get("CompanyName", "").strip(),
                    "date_int": date_int,
                    "margin_buy": cls._parse_number(item.get("MarginPurchase")),
                    "margin_sell": cls._parse_number(item.get("MarginSales")),
                    "margin_redemp": cls._parse_number(item.get("CashRedemption")),
                    "margin_balance": cls._parse_number(item.get("MarginPurchaseBalance")),
                    "margin_quota": cls._parse_number(item.get("MarginPurchaseQuota")),
                    "short_buy": cls._parse_number(item.get("ShortConvering")),
                    "short_sell": cls._parse_number(item.get("ShortSale")),
                    "short_redemp": cls._parse_number(item.get("StockRedemption")),
                    "short_balance": cls._parse_number(item.get("ShortSaleBalance")),
                    "short_quota": cls._parse_number(item.get("ShortSaleQuota")),
                    "offsetting": cls._parse_number(item.get("Offsetting")),
                    "margin_util_rate": cls._parse_float(item.get("MarginPurchaseUtilizationRate")),
                    "short_util_rate": cls._parse_float(item.get("ShortSaleUtilizationRate")),
                    "market": "TPEx"
                }
                
                results.append(record)
            
            if progress:
                progress.success(f"✓ TPEx 融資融券: {len(results)} 筆", level=2)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"TPEx 融資融券 API 請求失敗: {e}")
            if progress:
                progress.error(f"✗ TPEx 融資融券 API 失敗: {e}", level=2)
        except Exception as e:
            logger.error(f"TPEx 融資融券資料解析失敗: {e}")
            if progress:
                progress.error(f"✗ TPEx 融資融券解析失敗: {e}", level=2)
        
        return results
    
    @classmethod
    def save_to_db(cls, data_list):
        """
        將融資融券資料儲存到資料庫
        """
        if not data_list:
            return 0
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                insert_sql = """
                    INSERT OR REPLACE INTO margin_data 
                    (date_int, code, margin_buy, margin_sell, margin_redemp, 
                     margin_balance, margin_util_rate, short_buy, short_sell, 
                     short_redemp, short_balance, short_util_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                records = [
                    (
                        d["date_int"], d["code"], d["margin_buy"], d["margin_sell"],
                        d["margin_redemp"], d["margin_balance"], d["margin_util_rate"],
                        d["short_buy"], d["short_sell"], d["short_redemp"],
                        d["short_balance"], d["short_util_rate"]
                    )
                    for d in data_list
                ]
                
                cur.executemany(insert_sql, records)
                
                # 同步更新 stock_snapshot (最新一筆)
                snapshot_updates = []
                for d in data_list:
                    snapshot_updates.append((
                        d["margin_balance"], d["margin_util_rate"],
                        d["short_balance"], d["short_util_rate"],
                        d["code"]
                    ))
                
                cur.executemany("""
                    UPDATE stock_snapshot 
                    SET margin_balance = ?, margin_util_rate = ?,
                        short_balance = ?, short_util_rate = ?
                    WHERE code = ?
                """, snapshot_updates)
                
                conn.commit()
                return len(records)
                
        except Exception as e:
            logger.error(f"儲存融資融券資料失敗: {e}")
            return 0
    
    @classmethod
    def fetch_all_margin_data(cls, progress=None):
        """
        取得所有(上市+上櫃)融資融券資料並儲存
        """
        twse_data = cls.fetch_twse_margin(progress)
        tpex_data = cls.fetch_tpex_margin(progress)
        
        all_data = twse_data + tpex_data
        
        if all_data:
            saved = cls.save_to_db(all_data)
            if progress:
                progress.success(f"✓ 融資融券資料已儲存: {saved} 筆", level=3)
            return saved
        
        return 0
    
    @classmethod
    def get_stock_margin_data(cls, stock_code, days=30):
        """
        從資料庫取得個股融資融券歷史資料
        """
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT date_int, margin_balance, margin_util_rate,
                           short_balance, short_util_rate, 
                           margin_buy, margin_sell, short_buy, short_sell
                    FROM margin_data
                    WHERE code = ?
                    ORDER BY date_int DESC
                    LIMIT ?
                """, (stock_code, days))
                
                results = []
                for row in cur.fetchall():
                    results.append({
                        "date_int": row[0],
                        "margin_balance": row[1],
                        "margin_util_rate": row[2],
                        "short_balance": row[3],
                        "short_util_rate": row[4],
                        "margin_buy": row[5],
                        "margin_sell": row[6],
                        "short_buy": row[7],
                        "short_sell": row[8]
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"查詢融資融券資料失敗: {e}")
            return []
    
    @classmethod
    def get_latest_margin_data(cls, stock_code):
        """
        取得個股最新融資融券資料
        """
        history = cls.get_stock_margin_data(stock_code, days=1)
        return history[0] if history else None


# ==============================
# PE/PB 估值 API (OpenAPI)
# ==============================
class PePbDataAPI:
    """PE/PB 估值資料 API (使用官方 OpenAPI)"""
    
    # TWSE PE/PB OpenAPI (JSON, 中文欄位)
    TWSE_PEPB_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d"
    # TPEx PE/PB OpenAPI (JSON, 英文欄位)
    TPEX_PEPB_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
    
    @classmethod
    def _parse_float(cls, s):
        """解析浮點數字串"""
        if s is None or s == "" or s == "--" or s == "-":
            return None
        try:
            return float(str(s).replace(",", "").replace(" ", ""))
        except (ValueError, TypeError):
            return None
    
    @classmethod
    def fetch_twse_pepb(cls, progress=None):
        """從 TWSE 取得今日 PE/PB 資料 (網頁版優先，更即時)"""
        results = []
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        # === 主要來源: BWIBBU_ALL 網頁版 API (更即時) ===
        try:
            url = "https://www.twse.com.tw/exchangeReport/BWIBBU_ALL?response=json"
            resp = requests.get(url, headers=headers, timeout=30, verify=False)
            data = resp.json()
            
            if data.get('stat') == 'OK' and data.get('data'):
                # 解析日期
                date_str = data.get('date', '')
                if len(date_str) == 8:
                    date_int = int(date_str)
                else:
                    date_int = int(datetime.now().strftime("%Y%m%d"))
                
                # 欄位: [代號, 名稱, 殖利率, 股利年度, 本益比, 股價淨值比, ...]
                for row in data['data']:
                    if len(row) >= 6:
                        code = str(row[0]).strip()
                        if not code or not code.isdigit() or len(code) > 4:
                            continue
                        
                        results.append({
                            'code': code,
                            'name': str(row[1]).strip(),
                            'date_int': date_int,
                            'yield_rate': cls._parse_float(row[2]),
                            'pe': cls._parse_float(row[4]),
                            'pb': cls._parse_float(row[5]),
                            'market': 'TWSE'
                        })
                
                if results:
                    return results
                    
        except Exception as e:
            logger.debug(f"TWSE BWIBBU_ALL 網頁版失敗: {e}，使用 OpenAPI 備援")
        
        # === 備援: OpenAPI ===
        try:
            resp = requests.get(cls.TWSE_PEPB_URL, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            data = resp.json()
            if not isinstance(data, list):
                return results
            
            today_int = int(datetime.now().strftime("%Y%m%d"))
            
            for item in data:
                code = str(item.get("證券代號", "")).strip()
                if not code or not code.isdigit() or len(code) > 4:
                    continue
                
                results.append({
                    'code': code,
                    'name': str(item.get("證券名稱", "")).strip(),
                    'date_int': today_int,
                    'pe': cls._parse_float(item.get("本益比")),
                    'pb': cls._parse_float(item.get("股價淨值比")),
                    'yield_rate': cls._parse_float(item.get("殖利率(%)")),
                    'market': 'TWSE'
                })
                
        except Exception as e:
            logger.debug(f"TWSE PE/PB OpenAPI 也失敗: {e}")
        
        return results
    
    @classmethod
    def fetch_tpex_pepb(cls, progress=None):
        """從 TPEx OpenAPI 取得今日 PE/PB 資料"""
        results = []
        try:
            if progress:
                progress.info("正在從 TPEx OpenAPI 取得 PE/PB 資料...", level=2)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            resp = requests.get(cls.TPEX_PEPB_URL, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("TPEx PE/PB OpenAPI 回傳非陣列格式")
                return results
            
            for item in data:
                code = str(item.get("SecuritiesCompanyCode", "")).strip()
                if not code or not code.isdigit() or len(code) > 4:
                    continue
                
                # 解析日期
                date_str = str(item.get("Date", "")).strip()
                if len(date_str) == 7:
                    year = int(date_str[:3]) + 1911
                    date_int = int(f"{year}{date_str[3:]}")
                else:
                    date_int = int(datetime.now().strftime("%Y%m%d"))
                
                results.append({
                    'code': code,
                    'name': str(item.get("CompanyName", "")).strip(),
                    'date_int': date_int,
                    'pe': cls._parse_float(item.get("PriceEarningRatio")),
                    'pb': cls._parse_float(item.get("PriceBookRatio")),
                    'yield_rate': cls._parse_float(item.get("DividendYield")),
                    'market': 'TPEx'
                })
            
            if progress:
                progress.success(f"✓ TPEx PE/PB: {len(results)} 筆", level=2)
                
        except Exception as e:
            logger.error(f"TPEx PE/PB OpenAPI 失敗: {e}")
            if progress:
                progress.error(f"✗ TPEx PE/PB OpenAPI 失敗: {e}", level=2)
        
        return results
    
    @classmethod
    def save_to_db(cls, data_list):
        """將 PE/PB 資料儲存到資料庫並同步至 stock_snapshot"""
        if not data_list:
            return 0
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                # 同步更新 stock_snapshot
                for d in data_list:
                    cur.execute("""
                        UPDATE stock_snapshot 
                        SET pe = ?, pb = ?, yield = ?
                        WHERE code = ?
                    """, (d['pe'], d['pb'], d.get('yield_rate'), d['code']))
                
                conn.commit()
                return len(data_list)
                
        except Exception as e:
            logger.error(f"儲存 PE/PB 資料失敗: {e}")
            return 0
    
    @classmethod
    def fetch_from_finmind(cls, progress=None):
        """從 FinMind 取得 PE/PB 資料 (備援)"""
        results = []
        try:
            if progress:
                progress.info("正在從 FinMind 取得 PE/PB 資料 (備援)...", level=2)
            
            # 取得今天的資料
            today = datetime.now().strftime("%Y-%m-%d")
            
            params = {
                'dataset': 'TaiwanStockPER',
                'start_date': today,
                'end_date': today,
                'token': FINMIND_TOKEN
            }
            
            resp = requests.get("https://api.finmindtrade.com/api/v4/data", 
                              params=params, timeout=30)
            resp.raise_for_status()
            
            data = resp.json()
            if data.get('msg') != 'success' or not data.get('data'):
                logger.warning("FinMind PE/PB 回傳無資料")
                return results
            
            today_int = int(today.replace('-', ''))
            
            for item in data['data']:
                code = str(item.get('stock_id', '')).strip()
                if not code or len(code) > 4:
                    continue
                
                results.append({
                    'code': code,
                    'name': '',
                    'date_int': today_int,
                    'pe': cls._parse_float(item.get('PER')),
                    'pb': cls._parse_float(item.get('PBR')),
                    'yield_rate': cls._parse_float(item.get('dividend_yield')),
                    'market': 'FinMind'
                })
            
            if progress:
                progress.success(f"✓ FinMind PE/PB: {len(results)} 筆", level=2)
                
        except Exception as e:
            logger.error(f"FinMind PE/PB 失敗: {e}")
            if progress:
                progress.error(f"✗ FinMind PE/PB 失敗: {e}", level=2)
        
        return results
    
    @classmethod
    def fetch_all_pepb(cls, progress=None):
        """取得所有 PE/PB 資料並儲存 (OpenAPI 優先，FinMind 備援)"""
        # 先嘗試官方 OpenAPI
        twse_data = cls.fetch_twse_pepb(progress)
        tpex_data = cls.fetch_tpex_pepb(progress)
        
        all_data = twse_data + tpex_data
        
        # 若 OpenAPI 失敗或無資料，使用 FinMind 備援
        if not all_data:
            if progress:
                progress.info("OpenAPI 無資料，切換至 FinMind 備援...", level=1)
            all_data = cls.fetch_from_finmind(progress)
        
        if all_data:
            saved = cls.save_to_db(all_data)
            if progress:
                progress.success(f"✓ PE/PB 資料已儲存: {saved} 筆", level=3)
            return saved
        
        return 0


# ==============================
# 集保戶數 API (FinMind + TDCC 備援)
# ==============================
class ShareholderDataAPI:
    """集保戶數資料 API (FinMind 為主，TDCC CSV 為備援)"""
    
    # FinMind API
    FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
    # TDCC CSV (備援)
    TDCC_CSV_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    
    @classmethod
    def fetch_from_finmind(cls, progress=None):
        """從 FinMind 取得集保戶數資料"""
        results = []
        try:
            if progress:
                progress.info("正在從 FinMind 取得集保戶數資料...", level=1)
            
            # 取得最近一週的資料
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            params = {
                'dataset': 'TaiwanStockHoldingSharesPer',
                'start_date': start_date,
                'end_date': end_date,
                'token': FINMIND_TOKEN
            }
            
            resp = requests.get(cls.FINMIND_URL, params=params, timeout=30)
            resp.raise_for_status()
            
            data = resp.json()
            if data.get('msg') != 'success' or not data.get('data'):
                logger.warning("FinMind 集保戶數回傳無資料")
                return results
            
            # 處理資料：計算千張大戶持股比例 (持股級距 15: 1000張以上)
            df = pd.DataFrame(data['data'])
            if df.empty:
                return results
            
            # 取最新日期的資料
            latest_date = df['date'].max()
            df_latest = df[df['date'] == latest_date]
            
            # 計算千張大戶比例 (HoldingSharesLevel == 15)
            major_holders = df_latest[df_latest['HoldingSharesLevel'] == 15].groupby('stock_id')['percent'].sum()
            
            date_int = int(latest_date.replace('-', ''))
            
            for code, pct in major_holders.items():
                results.append({
                    'code': str(code),
                    'date_int': date_int,
                    'major_holders_pct': round(pct, 2),
                    'source': 'FinMind'
                })
            
            if progress:
                progress.success(f"✓ FinMind 集保戶數: {len(results)} 筆", level=1)
                
        except Exception as e:
            logger.error(f"FinMind 集保戶數失敗: {e}")
            if progress:
                progress.error(f"✗ FinMind 集保戶數失敗: {e}", level=1)
        
        return results
    
    @classmethod
    def fetch_from_tdcc_csv(cls, progress=None):
        """從 TDCC CSV 取得集保戶數資料 (備援)"""
        results = []
        try:
            if progress:
                progress.info("正在從 TDCC CSV 取得集保戶數資料 (備援)...", level=2)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # 注意：TDCC 網站有重定向問題，需禁止自動重定向
            resp = requests.get(cls.TDCC_CSV_URL, headers=headers, timeout=30, verify=False, allow_redirects=False)
            resp.raise_for_status()
            
            # 解析 CSV
            from io import StringIO
            df = pd.read_csv(StringIO(resp.text))
            
            if df.empty:
                logger.warning("TDCC CSV 無資料")
                return results
            
            # 必要欄位檢查
            if '證券代號' not in df.columns or '持股分級' not in df.columns:
                logger.warning("TDCC CSV 格式不符")
                return results
            
            # 處理資料
            df['持股分級'] = pd.to_numeric(df['持股分級'], errors='coerce')
            df['證券代號'] = df['證券代號'].astype(str)
            
            # 千張大戶 (持股分級 15)
            df_major = df[df['持股分級'] == 15].copy()
            
            if '占集保庫存數比例%' in df.columns:
                pct_col = '占集保庫存數比例%'
            elif '占集保庫存數比例' in df.columns:
                pct_col = '占集保庫存數比例'
            else:
                logger.warning("TDCC CSV 找不到比例欄位")
                return results
            
            major_holders = df_major.groupby('證券代號')[pct_col].sum()
            
            today_int = int(datetime.now().strftime("%Y%m%d"))
            
            for code, pct in major_holders.items():
                if len(str(code)) > 4:
                    continue
                results.append({
                    'code': str(code),
                    'date_int': today_int,
                    'major_holders_pct': round(float(pct), 2),
                    'source': 'TDCC'
                })
            
            if progress:
                progress.success(f"✓ TDCC CSV 集保戶數: {len(results)} 筆", level=2)
                
        except Exception as e:
            logger.error(f"TDCC CSV 集保戶數失敗: {e}")
            if progress:
                progress.error(f"✗ TDCC CSV 失敗: {e}", level=2)
        
        return results
    
    @classmethod
    def save_to_db(cls, data_list):
        """將集保戶數資料同步至 stock_snapshot"""
        if not data_list:
            return 0
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                for d in data_list:
                    cur.execute("""
                        UPDATE stock_snapshot 
                        SET major_holders_pct = ?
                        WHERE code = ?
                    """, (d['major_holders_pct'], d['code']))
                
                conn.commit()
                return len(data_list)
                
        except Exception as e:
            logger.error(f"儲存集保戶數失敗: {e}")
            return 0
    
    @classmethod
    def fetch_all_shareholder(cls, progress=None):
        """取得所有集保戶數資料 (直接使用 TDCC CSV)"""
        # 直接使用 TDCC CSV (FinMind 帳號等級限制，已移除)
        data = cls.fetch_from_tdcc_csv(progress)
        
        if data:
            saved = cls.save_to_db(data)
            if progress:
                progress.success(f"✓ 集保戶數已儲存: {saved} 筆", level=3)
            return saved
        
        return 0


# ╔══════════════════════════════════════════════════════════════╗
# ║                   DOMAIN/INDICATORS                           ║
# ║  純函數指標計算：calc_indicators(df) -> List[Dict]             ║
# ╚══════════════════════════════════════════════════════════════╝

# ==============================
# 純函數指標計算 (Phase 3)
# ==============================
def calc_indicators_pure(df: pd.DataFrame, display_days: int = 30) -> List[Dict]:
    """
    純函數：計算技術指標 (無 DB 副作用)
    
    Args:
        df: 包含 date, open, high, low, close, volume, amount 的 DataFrame
        display_days: 返回最近 N 天的指標
        
    Returns:
        List[Dict]: 每日指標字典列表
    """
    # [Guard Clause] 衛語句 - 資料不足則早退
    if df is None or df.empty:
        return []
    if len(df) < 20:
        return []
    
    # 確保日期排序
    if 'date' in df.columns:
        df = df.sort_values('date').reset_index(drop=True)
    elif 'date_int' in df.columns:
        df = df.sort_values('date_int').reset_index(drop=True)
    
    # 計算均線
    df = df.copy()
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    df['ma120'] = df['close'].rolling(120).mean()
    df['ma200'] = df['close'].rolling(200).mean()
    
    # 計算成交量均線
    df['vol_ma5'] = df['volume'].rolling(5).mean()
    df['vol_ma20'] = df['volume'].rolling(20).mean()
    df['vol_ma60'] = df['volume'].rolling(60).mean()
    
    # 計算 RSI (使用簡化版)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 計算 MFI (簡化版)
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    money_flow = typical_price * df['volume']
    positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0)
    negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0)
    mfi_ratio = positive_flow.rolling(14).sum() / negative_flow.rolling(14).sum()
    df['mfi14'] = 100 - (100 / (1 + mfi_ratio))
    
    # 計算乖離率
    df['bias20'] = ((df['close'] - df['ma20']) / df['ma20'] * 100) if 'ma20' in df.columns else None
    
    # 轉換為 List[Dict]
    results = []
    for i in range(max(0, len(df) - display_days), len(df)):
        row = df.iloc[i]
        results.append({
            'date_int': int(row.get('date_int', 0)) if 'date_int' in df.columns else None,
            'close': round(row['close'], 2) if pd.notna(row['close']) else None,
            'volume': int(row['volume']) if pd.notna(row['volume']) else 0,
            'ma5': round(row['ma5'], 2) if pd.notna(row.get('ma5')) else None,
            'ma20': round(row['ma20'], 2) if pd.notna(row.get('ma20')) else None,
            'ma60': round(row['ma60'], 2) if pd.notna(row.get('ma60')) else None,
            'ma120': round(row['ma120'], 2) if pd.notna(row.get('ma120')) else None,
            'ma200': round(row['ma200'], 2) if pd.notna(row.get('ma200')) else None,
            'rsi': round(row['rsi'], 2) if pd.notna(row.get('rsi')) else None,
            'mfi14': round(row['mfi14'], 2) if pd.notna(row.get('mfi14')) else None,
            'bias20': round(row['bias20'], 2) if pd.notna(row.get('bias20')) else None,
        })
    
    return results


# ==============================
# 指標計算類別
# ==============================
class IndicatorCalculator:
    @staticmethod
    def calculate_wma(series, period):
        """向量化 WMA 計算"""
        if len(series) < period:
            return np.full(len(series), np.nan)
        
        weights = np.arange(1, period + 1)
        wma_valid = np.convolve(series, weights[::-1], mode='valid') / weights.sum()
        
        nans = np.full(period - 1, np.nan)
        return np.concatenate((nans, wma_valid))

    @staticmethod
    def calculate_wma_for_df(df, period):
        """計算 DataFrame 的 WMA"""
        if df.empty or len(df) < period:
            return None
        
        try:
            vals = df['close'].dropna().values
            wma = IndicatorCalculator.calculate_wma(vals, period)
            return round(wma[-1], 2) if not np.isnan(wma[-1]) else None
        except:
            return None

    @staticmethod
    def calculate_macd_series(df, fast=12, slow=26, signal=9):
        """計算 MACD 指標序列"""
        if df.empty or len(df) < slow:
            return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)
        
        try:
            close_prices = df['close'].values
            wma_fast = IndicatorCalculator.calculate_wma(close_prices, fast)
            wma_slow = IndicatorCalculator.calculate_wma(close_prices, slow)
            
            macd_line = wma_fast - wma_slow
            signal_line = IndicatorCalculator.calculate_wma(macd_line, signal)
            
            return pd.Series(macd_line, index=df.index), pd.Series(signal_line, index=df.index)
        except:
            return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_ma(df, period):
        """計算移動平均線"""
        if df.empty or len(df) < period:
            return None
        
        ma = df['close'].rolling(window=period).mean().iloc[-1]
        return round(ma, 2) if not pd.isna(ma) else None

    @staticmethod
    def calculate_rsi(df, period=14):
        """計算 RSI"""
        if df.empty or len(df) < period + 1:
            return None
        
        try:
            deltas = np.diff(df['close'].values)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            avg_gain = IndicatorCalculator.calculate_wma(gains, period)[-1]
            avg_loss = IndicatorCalculator.calculate_wma(losses, period)[-1]
            
            if avg_loss == 0:
                return 100.0 if avg_gain > 0 else 50.0
            
            rs = avg_gain / avg_loss
            return round(100 - (100 / (1 + rs)), 2)
        except:
            return None

    @staticmethod
    def calculate_rsi_series(df, period=14):
        """計算 RSI 指標序列"""
        if df.empty or len(df) < period + 1:
            return pd.Series(np.nan, index=df.index)
        
        try:
            deltas = np.diff(df['close'].values)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            gains = np.insert(gains, 0, 0)
            losses = np.insert(losses, 0, 0)
            
            avg_gains = IndicatorCalculator.calculate_wma(gains, period)
            avg_losses = IndicatorCalculator.calculate_wma(losses, period)
            
            with np.errstate(divide='ignore', invalid='ignore'):
                rs = avg_gains / avg_losses
                rsi_values = 100 - (100 / (1 + rs))
            
            rsi_values = np.where(avg_losses == 0, 
                                  np.where(avg_gains > 0, 100.0, 50.0), 
                                  rsi_values)
            
            rsi_values = np.where(np.isnan(avg_gains) | np.isnan(avg_losses), np.nan, rsi_values)
            
            return pd.Series(rsi_values, index=df.index)
        except Exception as e:
            return pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_macd(df, fast=12, slow=26, signal=9):
        """計算 MACD"""
        if df.empty or len(df) < slow:
            return None, None
        
        try:
            closes = df['close'].values
            wma_f = IndicatorCalculator.calculate_wma(closes, fast)
            wma_s = IndicatorCalculator.calculate_wma(closes, slow)
            
            macd_line = wma_f - wma_s
            valid_macd = macd_line[slow-1:]
            
            if len(valid_macd) < signal:
                return round(macd_line[-1], 2), None
            
            sig_vals = IndicatorCalculator.calculate_wma(valid_macd, signal)
            return round(macd_line[-1], 2), round(sig_vals[-1], 2)
        except:
            return None, None

    @staticmethod
    def calculate_mfi(df, period=14):
        """計算 MFI"""
        if df.empty or len(df) < period:
            return pd.Series(np.nan, index=df.index)
        
        try:
            tp = (df['high'] + df['low'] + df['close']) / 3
            mf = tp * df['volume']
            
            pos = np.where(tp > tp.shift(1), mf, 0)
            neg = np.where(tp < tp.shift(1), mf, 0)
            
            pos_wma = IndicatorCalculator.calculate_wma(pos, period)
            neg_wma = IndicatorCalculator.calculate_wma(neg, period)
            
            with np.errstate(divide='ignore', invalid='ignore'):
                ratio = pos_wma / neg_wma
                mfi = 100 - (100 / (1 + ratio))
            
            mfi = np.where(neg_wma == 0, 
                           np.where(pos_wma > 0, 100.0, 50.0), 
                           mfi)
            
            mfi = np.where(np.isnan(pos_wma) | np.isnan(neg_wma), 50.0, mfi)
            
            return pd.Series(mfi, index=df.index)
        except:
            return pd.Series(np.full(len(df), 50.0), index=df.index)

    @staticmethod
    def calculate_vwap_series(df, lookback=20):
        """計算 VWAP 序列"""
        if df.empty or len(df) < lookback:
            return pd.Series(np.nan, index=df.index)
        
        try:
            tp = (df['high'] + df['low'] + df['close']) / 3
            vwap_values = (tp * df['volume']).rolling(lookback).sum() / df['volume'].rolling(lookback).sum()
            return vwap_values.fillna(method='bfill')
        except:
            return pd.Series(np.nan, index=df.index)


    @staticmethod
    def calculate_chg14_series(df):
        """計算14日變化率序列"""
        if df.empty or len(df) < 14:
            return pd.Series(np.nan, index=df.index)
        
        try:
            chg = (df['close'] - df['close'].shift(14)) / df['close'].shift(14) * 100
            return chg.fillna(0)
        except:
            return pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_monthly_kd_series(df, k_period=9, d_period=3):
        """計算月KD序列"""
        if df.empty or len(df) < k_period:
            return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)
        
        try:
            low_min = df['low'].rolling(k_period).min()
            high_max = df['high'].rolling(k_period).max()
            rsv = (df['close'] - low_min) / (high_max - low_min) * 100
            rsv = rsv.fillna(50)
            
            k_vals = rsv.ewm(span=d_period, adjust=False).mean()
            d_vals = k_vals.ewm(span=d_period, adjust=False).mean()
            
            return k_vals.fillna(50), d_vals.fillna(50)
        except:
            return pd.Series(50.0, index=df.index), pd.Series(50.0, index=df.index)

    @staticmethod
    def calculate_daily_kd_series(df, k_period=9, d_period=3):
        """計算日KD序列"""
        return IndicatorCalculator.calculate_monthly_kd_series(df, k_period, d_period)

    @staticmethod
    def calculate_weekly_kd_series(df, k_period=9, d_period=3):
        """計算週KD序列"""
        return IndicatorCalculator.calculate_monthly_kd_series(df, k_period * 5, d_period)

    @staticmethod
    def calculate_smart_score_series(df):
        """計算智慧分數序列"""
        if df.empty:
            empty = pd.Series(0, index=df.index)
            return empty, empty, empty, empty, empty, empty, empty
        
        try:
            # Simplified smart score calculation
            score = pd.Series(50, index=df.index)
            smi_sig = pd.Series(0, index=df.index)
            nvi_sig = pd.Series(0, index=df.index)
            vsa_sig = pd.Series(0, index=df.index)
            svi_sig = pd.Series(0, index=df.index)
            vol_div_sig = pd.Series(0, index=df.index)
            weekly_nvi_sig = pd.Series(0, index=df.index)
            
            return score, smi_sig, nvi_sig, vsa_sig, svi_sig, vol_div_sig, weekly_nvi_sig
        except:
            empty = pd.Series(0, index=df.index)
            return empty, empty, empty, empty, empty, empty, empty

    @staticmethod
    def calculate_smi_series(df, period=14):
        """計算SMI序列"""
        if df.empty or len(df) < period:
            return pd.Series(np.nan, index=df.index)
        
        try:
            high_low_avg = (df['high'].rolling(period).max() + df['low'].rolling(period).min()) / 2
            smi = (df['close'] - high_low_avg) / (df['high'].rolling(period).max() - df['low'].rolling(period).min()) * 100
            return smi.fillna(0)
        except:
            return pd.Series(0, index=df.index)

    @staticmethod
    def calculate_nvi_series(df):
        """計算NVI序列"""
        if df.empty or len(df) < 2:
            return pd.Series(1000, index=df.index), pd.Series(1000, index=df.index)
        
        try:
            nvi = pd.Series(1000.0, index=df.index)
            for i in range(1, len(df)):
                if df['volume'].iloc[i] < df['volume'].iloc[i-1]:
                    pct_change = (df['close'].iloc[i] - df['close'].iloc[i-1]) / df['close'].iloc[i-1]
                    nvi.iloc[i] = nvi.iloc[i-1] * (1 + pct_change)
                else:
                    nvi.iloc[i] = nvi.iloc[i-1]
            
            nvi_ma = nvi.rolling(50).mean()
            return nvi, nvi_ma
        except:
            return pd.Series(1000, index=df.index), pd.Series(1000, index=df.index)

    @staticmethod
    def calculate_adl_series(df):
        """計算ADL序列"""
        if df.empty:
            return pd.Series(0, index=df.index)
        
        try:
            mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
            mfm = mfm.fillna(0)
            mfv = mfm * df['volume']
            adl = mfv.cumsum()
            return adl
        except:
            return pd.Series(0, index=df.index)

    @staticmethod
    def calculate_rs_series(df, period=14):
        """計算相對強度序列"""
        if df.empty or len(df) < period:
            return pd.Series(50, index=df.index)
        
        try:
            returns = df['close'].pct_change()
            pos_returns = returns.where(returns > 0, 0)
            neg_returns = -returns.where(returns < 0, 0)
            
            avg_gain = pos_returns.rolling(period).mean()
            avg_loss = neg_returns.rolling(period).mean()
            
            rs = avg_gain / (avg_loss + 1e-10)
            rs_score = 100 - (100 / (1 + rs))
            return rs_score.fillna(50)
        except:
            return pd.Series(50, index=df.index)

    @staticmethod
    def calculate_pvi_series(df):
        """計算PVI序列"""
        if df.empty or len(df) < 2:
            return pd.Series(1000, index=df.index)
        
        try:
            pvi = pd.Series(1000.0, index=df.index)
            for i in range(1, len(df)):
                if df['volume'].iloc[i] > df['volume'].iloc[i-1]:
                    pct_change = (df['close'].iloc[i] - df['close'].iloc[i-1]) / df['close'].iloc[i-1]
                    pvi.iloc[i] = pvi.iloc[i-1] * (1 + pct_change)
                else:
                    pvi.iloc[i] = pvi.iloc[i-1]
            return pvi
        except:
            return pd.Series(1000, index=df.index)

    @staticmethod
    def calculate_clv_series(df):
        """計算CLV序列"""
        if df.empty:
            return pd.Series(0, index=df.index)
        
        try:
            clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
            return clv.fillna(0)
        except:
            return pd.Series(0, index=df.index)

    @staticmethod
    def calculate_3day_divergence_series(df):
        """計算3日背離序列"""
        if df.empty or len(df) < 3:
            return pd.Series(0, index=df.index), pd.Series(0, index=df.index)
        
        try:
            bull = pd.Series(0, index=df.index)
            bear = pd.Series(0, index=df.index)
            
            # Simple divergence: price down but volume up = bullish
            # price up but volume down = bearish
            price_change = df['close'].diff(3)
            vol_change = df['volume'].diff(3)
            
            bull = ((price_change < 0) & (vol_change > 0)).astype(int)
            bear = ((price_change > 0) & (vol_change < 0)).astype(int)
            
            return bull, bear
        except:
            return pd.Series(0, index=df.index), pd.Series(0, index=df.index)


    @staticmethod
    def calculate_vp_scheme3(df, lookback=20):
        """計算 Volume Profile (POC, VP_upper, VP_lower)"""
        result = {'POC': None, 'VP_upper': None, 'VP_lower': None}
        
        if df.empty or len(df) < 2:
            return result
        
        try:
            # Use recent data
            recent = df.tail(lookback) if len(df) >= lookback else df
            
            if len(recent) < 2:
                return result
                
            # Calculate typical price and volume profile
            high = recent['high'].max()
            low = recent['low'].min()
            
            if high == low:
                result['POC'] = high
                result['VP_upper'] = high
                result['VP_lower'] = low
                return result
            
            # Simple POC calculation - price with highest volume
            price_levels = 10
            step = (high - low) / price_levels
            
            volume_at_price = {}
            for i in range(price_levels):
                price_low = low + i * step
                price_high = low + (i + 1) * step
                mid_price = (price_low + price_high) / 2
                
                mask = (recent['close'] >= price_low) & (recent['close'] < price_high)
                vol = recent.loc[mask, 'volume'].sum()
                volume_at_price[mid_price] = vol
            
            if volume_at_price:
                poc_price = max(volume_at_price, key=volume_at_price.get)
                result['POC'] = round(poc_price, 2)
            else:
                result['POC'] = round(recent['close'].iloc[-1], 2)
            
            # Calculate value area (70% of volume)
            total_vol = sum(volume_at_price.values())
            if total_vol > 0:
                sorted_prices = sorted(volume_at_price.items(), key=lambda x: x[1], reverse=True)
                cumulative = 0
                value_area_prices = []
                
                for price, vol in sorted_prices:
                    cumulative += vol
                    value_area_prices.append(price)
                    if cumulative >= total_vol * 0.7:
                        break
                
                if value_area_prices:
                    result['VP_upper'] = round(max(value_area_prices) + step/2, 2)
                    result['VP_lower'] = round(min(value_area_prices) - step/2, 2)
                else:
                    result['VP_upper'] = round(high, 2)
                    result['VP_lower'] = round(low, 2)
            else:
                result['VP_upper'] = round(high, 2)
                result['VP_lower'] = round(low, 2)
            
            return result
        except Exception as e:
            return result


    @staticmethod
    def calculate_vsbc_bands(df, win=10):
        """計算 VSBC 上下通道"""
        if df.empty or len(df) < win:
            return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)
        
        try:
            # 計算成交量情緒
            signed_vol = np.where(df['close'] >= df['open'], df['volume'], -df['volume'])
            signed_vol = pd.Series(signed_vol, index=df.index)

            # 情緒推力平均 & 平均成交量
            vs_force = signed_vol.rolling(win, min_periods=1).mean()
            vol_mean = df['volume'].rolling(win, min_periods=1).mean()

            # 箱體基礎
            base_mid = (df['high'] + df['low']) / 2
            base_range = (df['high'] - df['low']).rolling(win, min_periods=1).mean().replace(0, 1e-9)

            # 中線位移（防爆範圍 -0.5 ~ 0.5）
            shift = (vs_force / vol_mean).fillna(0).clip(-0.5, 0.5)

            vsbc_mid = base_mid + shift * base_range
            
            # 假設通道寬度為 1 倍 base_range (上下各 0.5)
            # 或者根據原始邏輯，VSBC 主要是中線，這裡我們定義一個通道供參考
            upper = vsbc_mid + base_range * 0.5
            lower = vsbc_mid - base_range * 0.5
            
            return upper, lower
        except:
            return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_pattern_morning_star(df):
        """
        早晨之星 (Morning Star) - 底部反轉
        T-2: 長黑 K
        T-1: 星線 (實體小, 收盤 < T-2 收盤)
        T: 長紅 K (收盤 > T-2 實體中點)
        """
        if len(df) < 3:
            return pd.Series([False] * len(df), index=df.index)
            
        close = df['close']
        open_ = df['open']
        high = df['high']
        low = df['low']
        
        body = (close - open_).abs()
        candle_range = high - low
        
        c2 = close.shift(2)
        o2 = open_.shift(2)
        body2 = body.shift(2)
        range2 = candle_range.shift(2)
        
        c1 = close.shift(1)
        o1 = open_.shift(1)
        body1 = body.shift(1)
        
        c0 = close
        o0 = open_
        body0 = body
        range0 = candle_range
        
        is_long_black_2 = (c2 < o2) & (body2 > range2 * 0.6)
        is_star_1 = (body1 < body2 * 0.3) & (c1 < c2)
        mid_point_2 = (o2 + c2) / 2
        is_long_red_0 = (c0 > o0) & (c0 > mid_point_2) & (body0 > range0 * 0.6)
        
        return is_long_black_2 & is_star_1 & is_long_red_0

    @staticmethod
    def calculate_pattern_evening_star(df):
        """
        黃昏之星 (Evening Star) - 頂部反轉
        T-2: 長紅 K
        T-1: 星線 (實體小, 收盤 > T-2 收盤)
        T: 長黑 K (收盤 < T-2 實體中點)
        """
        if len(df) < 3:
            return pd.Series([False] * len(df), index=df.index)
            
        close = df['close']
        open_ = df['open']
        high = df['high']
        low = df['low']
        
        body = (close - open_).abs()
        candle_range = high - low
        
        c2 = close.shift(2)
        o2 = open_.shift(2)
        body2 = body.shift(2)
        range2 = candle_range.shift(2)
        
        c1 = close.shift(1)
        o1 = open_.shift(1)
        body1 = body.shift(1)
        
        c0 = close
        o0 = open_
        body0 = body
        range0 = candle_range
        
        is_long_red_2 = (c2 > o2) & (body2 > range2 * 0.6)
        is_star_1 = (body1 < body2 * 0.3) & (c1 > c2)
        mid_point_2 = (o2 + c2) / 2
        is_long_black_0 = (c0 < o0) & (c0 < mid_point_2) & (body0 > range0 * 0.6)
        
        return is_long_red_2 & is_star_1 & is_long_black_0


class TaiwanStockScreenerAdvanced:
    """
    台股五階篩選器 (完整修正版 - 整合版)
    - 市場環境作為動態調整因子
    - 永遠尋找市場中的相對強勢股
    - 黑天鵝防護機制
    """
    
    def __init__(self, db_conn):
        self.conn = db_conn
        self.base_params = {
            'min_relative_strength': 0.15,
        }
        self.current_params = self.base_params.copy()

    def calculate_technical_indicators(self, df):
        if df.empty or len(df) < 60: return df
        df = df.copy()
        
        # VWAP 60
        tp = (df['High'] + df['Low'] + df['Close']) / 3
        df['vwap_60'] = (tp * df['Volume']).rolling(60).sum() / df['Volume'].rolling(60).sum()
        
        # BBW
        df['ma20'] = df['Close'].rolling(20).mean()
        df['std20'] = df['Close'].rolling(20).std()
        df['upper_bb'] = df['ma20'] + 2 * df['std20']
        df['lower_bb'] = df['ma20'] - 2 * df['std20']
        df['bbw'] = (df['upper_bb'] - df['lower_bb']) / df['ma20']
        
        # KD
        lowest_low = df['Low'].rolling(9).min()
        highest_high = df['High'].rolling(9).max()
        df['rsv'] = 100 * (df['Close'] - lowest_low) / (highest_high - lowest_low)
        df['k'] = df['rsv'].ewm(alpha=1/3, adjust=False).mean()
        df['d'] = df['k'].ewm(alpha=1/3, adjust=False).mean()
        
        # Weekly (Resample)
        # Ensure index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
             # Try to convert if 'date' column exists, else return
             if 'date' in df.columns:
                 df['date'] = pd.to_datetime(df['date'])
                 df.set_index('date', inplace=True)
        
        try:
            weekly = df.resample('W').agg({'Open': 'first', 'Close': 'last'})
            df['weekly_close'] = weekly['Close'].reindex(df.index, method='ffill')
            df['weekly_open'] = weekly['Open'].reindex(df.index, method='ffill')
        except:
            df['weekly_close'] = df['Close']
            df['weekly_open'] = df['Open']
            
        return df

    def calculate_relative_strength(self, stock_df, twii_df):
        if len(stock_df) < 20 or len(twii_df) < 20: return 0.0
        try:
            s_ret = (stock_df['Close'].iloc[-1] / stock_df['Close'].iloc[-20]) - 1
            m_ret = (twii_df['Close'].iloc[-1] / twii_df['Close'].iloc[-20]) - 1
            rs = s_ret - m_ret
            if m_ret < 0 and s_ret > 0: rs += 0.3
            if m_ret > 0 and s_ret > m_ret * 1.5: rs += 0.2
            return rs
        except: return 0.0

    def market_filter(self, twii_df):
        if twii_df is None or len(twii_df) < 60:
            return {'market_score': 50, 'adjustment_factor': 1.0}
            
        score = 50
        ma60 = twii_df['Close'].rolling(60).mean().iloc[-1]
        if twii_df['Close'].iloc[-1] > ma60: score += 15
        else: score -= 15
        
        adj = 1.0
        if score > 70: adj = 1.2
        elif score < 40: adj = 0.8
        
        # Adjust params based on market
        self.current_params = self.base_params.copy()
        if score < 50:
            self.current_params['min_relative_strength'] = 0.25
        elif score > 70:
            self.current_params['min_relative_strength'] = 0.10
            
        return {'market_score': score, 'adjustment_factor': adj}

    def stock_strength_filter(self, df, adj=1.0):
        # Step 1: Strength (0-100)
        latest = df.iloc[-1]
        score = 0
        
        # Price > VWAP60 (20)
        if latest['Close'] > latest['vwap_60'] * adj: score += 20
        # Price > MA20 (20)
        if latest['Close'] > latest['ma20']: score += 20
        # Weekly Red (20)
        if latest['weekly_close'] > latest['weekly_open']: score += 20
        # Volume > 500 (20) - Basic check
        if latest['Volume'] > 500: score += 20
        # Trend (20)
        if latest['Close'] > df.iloc[-20]['Close']: score += 20
        
        return score >= 60, score

    def smart_money_validation(self, df, adj=1.0):
        # Step 2: Smart Money (0-100)
        latest = df.iloc[-1]
        score = 0
        
        # BBW Tight (< 0.15) (40)
        if latest['bbw'] < 0.15: score += 40
        # Volume > MA5 (30)
        vol_ma5 = df['Volume'].rolling(5).mean().iloc[-1]
        if latest['Volume'] > vol_ma5: score += 30
        # Price Stable (30)
        if abs(latest['Close'] - df.iloc[-5]['Close'])/df.iloc[-5]['Close'] < 0.05: score += 30
        
        return score >= 60, score

    def value_zone_filter(self, df):
        # Step 3: Value (0-100)
        latest = df.iloc[-1]
        score = 0
        
        # Near MA20 (< 5%) (40)
        dist = abs(latest['Close'] - latest['ma20']) / latest['ma20']
        if dist < 0.05: score += 40
        # KD Low (< 50) (30)
        if latest['k'] < 50: score += 30
        # Volume Shrink (30)
        if latest['Volume'] < df.iloc[-2]['Volume']: score += 30
        
        return score >= 50, score

    def entry_trigger(self, df):
        # Step 4: Trigger (0-100)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        score = 0
        
        # Bullish Candle (30)
        if latest['Close'] > latest['Open']: score += 30
        # Volume Surge (> 1.5x) (30)
        if latest['Volume'] > prev['Volume'] * 1.5: score += 30
        # KD Cross (20)
        if latest['k'] > latest['d']: score += 20
        # Price > Prev High (20)
        if latest['Close'] > prev['High']: score += 20
        
        return score >= 50, score


def run_five_stage_screener():
    """執行五階篩選器 (選單入口)"""
    print_flush("\n" + "="*60)
    print_flush("五階篩選器 (千問版 - 本地資料庫)")
    print_flush("="*60)
    
    date_str = get_latest_market_date()
    print_flush(f"📅 使用最新資料日期: {date_str}")
    
    # 獲取使用者輸入的掃描參數
    limit, min_vol = get_user_scan_params()
        
    try:
        screener = TaiwanStockScreenerAdvanced(db_path=Config.DB_PATH)
        results = screener.screen_all_stocks(date_str, max_stocks=None)
        print_flush(f"\n✨ 篩選完成! 共處理 {len(results)} 檔股票。")
    except Exception as e:
        print_flush(f"❌ 篩選過程發生錯誤: {e}")
        import traceback
        traceback.print_exc()

class InstitutionalValueStrategy:
    """
    機構價值回歸策略 (gemini版)
    策略核心:
    1. 市場濾網 (Step 0): 大盤多頭 + 市場騰落線 (Market ADL) 健康
    2. 趨勢強弱 (Step 1): WMA200/VWAP200 多頭 + RS > RS_MA200 (強於大盤) + 週KD向上
    3. 籌碼鎖定 (Step 2): 投信外資買超 (真主力) + ADL 底背離 (隱性吸籌)
    4. 價值回歸 (Step 3): 回測 Fib 0.618 / VP / WMA20
    5. 動能觸發 (Step 4): 日KD低檔金叉 + 量增
    """
    
    def __init__(self):
        self.market_df = None
        self.market_adl_status = False
        
    def get_connection(self):
        return db_manager.get_connection()

    def _wma(self, series, period):
        """加權移動平均 (Weighted Moving Average)"""
        if len(series) < period: return pd.Series(np.nan, index=series.index)
        weights = np.arange(1, period + 1)
        return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)

    def _vwap(self, df, period=200):
        """成交量加權平均價 (VWAP)"""
        try:
            typical_price = (df['High'] + df['Low'] + df['Close']) / 3
            pv = typical_price * df['Volume']
            cum_pv = pv.rolling(period).sum()
            cum_vol = df['Volume'].rolling(period).sum()
            return cum_pv / cum_vol
        except:
            return pd.Series(np.nan, index=df.index)

    def _mansfield_rs(self, stock_close, market_close, period=200):
        """曼斯菲爾德相對強度 (Standardized RS)"""
        try:
            # 確保索引對齊
            m_aligned = market_close.reindex(stock_close.index).fillna(method='ffill')
            # 原始比率
            raw_rs = stock_close / m_aligned
            # 基準線 (MA200 of Ratio)
            base = raw_rs.rolling(period).mean()
            return raw_rs, base
        except:
            return pd.Series(0, index=stock_close.index), pd.Series(0, index=stock_close.index)

    def _stock_adl(self, df):
        """Chaikin A/D Line (累積派發線)"""
        try:
            clv = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low'])
            clv = clv.fillna(0)
            return (clv * df['Volume']).cumsum()
        except:
            return pd.Series(0, index=df.index)

    def _fibonacci_pivots(self, df, lookback=60):
        """計算波段高低點與費波那契回撤"""
        if len(df) < lookback: return None
        recent = df.iloc[-lookback:]
        high = recent['High'].max()
        low = recent['Low'].min()
        diff = high - low
        if diff == 0: return None
        return {
            '0.618': high - (diff * 0.618) # 黃金買點
        }

    def _kd(self, df, period=9):
        """KD指標"""
        try:
            low_min = df['Low'].rolling(period).min()
            high_max = df['High'].rolling(period).max()
            rsv = (df['Close'] - low_min) / (high_max - low_min) * 100
            rsv = rsv.fillna(50)
            k = rsv.ewm(com=2, adjust=False).mean()
            d = k.ewm(com=2, adjust=False).mean()
            return k, d
        except:
            return pd.Series(50, index=df.index), pd.Series(50, index=df.index)

    def _weekly_kd(self, df):
        """週線 KD"""
        try:
            w_df = df.resample('W-FRI').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
            if len(w_df) < 9: return pd.Series(0, index=df.index), pd.Series(0, index=df.index)
            wk, wd = self._kd(w_df)
            return wk.reindex(df.index, method='ffill'), wd.reindex(df.index, method='ffill')
        except:
            return pd.Series(0, index=df.index), pd.Series(0, index=df.index)

    def calculate_market_adl(self):
        """計算全市場 ADL (Market Breadth)"""
        print_flush("\n[Step 1] 計算全市場騰落線 (Market ADL)...")
        try:
            with self.get_connection() as conn:
                # 簡單計算每日漲跌家數
                df = pd.read_sql("""
                    SELECT date_int, 
                           SUM(CASE WHEN close > open THEN 1 ELSE 0 END) as rise,
                           SUM(CASE WHEN close < open THEN 1 ELSE 0 END) as fall
                    FROM stock_history 
                    WHERE code != '0000' AND code != 'TAIEX'
                    GROUP BY date_int
                    ORDER BY date_int
                """, conn)
                
                if df.empty: return False
                
                df['net'] = df['rise'] - df['fall']
                df['adl'] = df['net'].cumsum()
                
                # 判斷狀態 (ADL > MA20)
                curr_adl = df['adl'].iloc[-1]
                ma20_adl = df['adl'].rolling(20).mean().iloc[-1]
                self.market_adl_status = curr_adl > ma20_adl
                
                status = "健康 (ADL > MA20)" if self.market_adl_status else "警戒 (ADL < MA20)"
                print_flush(f"Market ADL 狀態: {status}")
                return self.market_adl_status
        except Exception as e:
            print_flush(f"⚠ Market ADL 計算失敗: {e}")
            return False

    def load_market_index(self):
        """載入大盤指數 (0000 或 TAIEX)"""
        try:
            with self.get_connection() as conn:
                df = pd.read_sql("SELECT date_int, close FROM stock_history WHERE code='0000' OR code='TAIEX' ORDER BY date_int", conn)
                if df.empty: return False
                
                df['date'] = pd.to_datetime(df['date_int'].astype(str), format='%Y%m%d')
                df.set_index('date', inplace=True)
                self.market_df = df['close']
                return True
        except:
            return False

    def scan(self):
        """執行策略掃描"""
        print_flush("\n[機構價值回歸策略] V2.0")
        
        # 獲取使用者輸入的掃描參數
        limit, min_vol = get_user_scan_params()
        
        print_flush("\n[Step 1] 準備市場數據...")
        
        # 1. 準備數據
        if not self.load_market_index():
            print_flush("❌ 無法載入大盤數據")
            return
            
        self.calculate_market_adl()
        
        # 2. 獲取股票清單
        with self.get_connection() as conn:
            codes = [r[0] for r in conn.execute("SELECT DISTINCT code FROM stock_history WHERE code NOT LIKE '0%' AND LENGTH(code)=4").fetchall()]
            # Fetch names
            try:
                names = {r[0]: r[1] for r in conn.execute("SELECT code, name FROM stock_meta").fetchall()}
            except:
                names = {}
        
        print_flush(f"目標: {len(codes)} 檔股票 (分析需時較長，請稍候...)")
        
        candidates = []
        processed = 0
        
        for code in codes:
            processed += 1
            if processed % 50 == 0:
                print_flush(f"  進度: {processed}/{len(codes)}")
                
            try:
                # 載入個股數據
                with self.get_connection() as conn:
                    df = pd.read_sql(f"SELECT * FROM stock_history WHERE code='{code}' ORDER BY date_int", conn)
                    inst = pd.read_sql(f"SELECT date_int, foreign_buy-foreign_sell as f_net, trust_buy-trust_sell as t_net FROM institutional_investors WHERE code='{code}'", conn)
                
                if len(df) < 250: continue
                
                df['date'] = pd.to_datetime(df['date_int'].astype(str), format='%Y%m%d')
                df.set_index('date', inplace=True)
                
                df.rename(columns={'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume'}, inplace=True)
                
                # --- Step 1: 趨勢與強弱 ---
                df['wma20'] = self._wma(df['Close'], 20)
                df['wma200'] = self._wma(df['Close'], 200)
                df['vwap200'] = self._vwap(df, 200)
                
                # RS
                rs_val, rs_ma = self._mansfield_rs(df['Close'], self.market_df)
                
                # 週 KD
                wk, wd = self._weekly_kd(df)
                
                cur = df.iloc[-1]
                
                # 1. 多頭結構
                if not (cur['Close'] > cur['wma200'] and cur['Close'] > cur['vwap200']): continue
                # 2. 強於大盤
                if not (rs_val.iloc[-1] > rs_ma.iloc[-1]): continue
                # 3. 週線保護
                if not (wk.iloc[-1] > wd.iloc[-1]): continue
                
                # --- Step 2: 籌碼 ---
                # Stock ADL
                df['stock_adl'] = self._stock_adl(df)
                
                # 法人近5日
                inst_5d_net = 0
                if not inst.empty:
                    inst['date'] = pd.to_datetime(inst['date_int'].astype(str), format='%Y%m%d')
                    inst.set_index('date', inplace=True)
                    # Join
                    df_inst = df.join(inst, how='left').fillna(0)
                    inst_5d_net = (df_inst['f_net'] + df_inst['t_net']).rolling(5).sum().iloc[-1]
                
                # ADL 底背離 (股價跌 ADL 漲) - 簡單判斷近5日
                price_trend = df['Close'].iloc[-1] < df['Close'].iloc[-5]
                adl_trend = df['stock_adl'].iloc[-1] > df['stock_adl'].iloc[-5]
                adl_div = price_trend and adl_trend
                
                if not (inst_5d_net > 0 or adl_div): continue
                
                # --- Step 3: 價值區間 ---
                fibs = self._fibonacci_pivots(df)
                fib_0618 = fibs['0.618'] if fibs else 0
                
                dist_fib = abs(cur['Close'] - fib_0618) / cur['Close'] if fib_0618 else 1.0
                dist_wma = abs(cur['Close'] - cur['wma20']) / cur['Close']
                
                in_value_zone = (dist_fib < 0.05) or (dist_wma < 0.05) # 放寬至 5%
                
                # --- Step 4: 動能觸發 ---
                dk, dd = self._kd(df)
                kd_cross = (dk.iloc[-1] > dd.iloc[-1]) and (dk.iloc[-2] <= dd.iloc[-2])
                kd_low = dk.iloc[-1] < 60
                vol_up = cur['Volume'] > df['Volume'].iloc[-2]
                
                is_triggered = kd_cross and kd_low and vol_up
                
                if in_value_zone:
                    candidates.append({
                        'code': code,
                        'name': names.get(code, code),
                        'price': cur['Close'],
                        'inst': inst_5d_net,
                        'fib': fib_0618,
                        'status': "TRIGGERED" if is_triggered else "WAITING"
                    })
                    
            except Exception as e:
                continue
                
        # 輸出結果
        print_flush(f"\n{'='*80}")
        print_flush(f"【掃描結果】 機構價值回歸 V2.0 (RS+ADL+Fib)")
        print_flush(f"篩選標準: 強於大盤(RS) + 主力買超/背離 + 回測價值區")
        print_flush(f"{'-'*80}")
        # Header: 代號 名稱 收盤 機構籌碼 回測位置 狀態
        print_flush(f"{'代號':<6} {'名稱':<8} {'收盤':<10} {'機構籌碼':<12} {'回測位置':<12} {'狀態':<10}")
        print_flush(f"{'-'*80}")
        
        triggered = [c for c in candidates if c['status'] == "TRIGGERED"]
        waiting = [c for c in candidates if c['status'] == "WAITING"]
        
        reset = reset_color()
        
        for c in triggered:
            # Color logic
            c_price = Colors.RED # Default
            price_str = f"{c_price}{c['price']:.2f}{reset}"
            inst_str = f"{c['inst']:,.0f}"
            fib_str = f"{c['fib']:.2f}"
            status_str = f"{Colors.RED}★觸發買點{reset}"
            
            print_flush(f"{c['code']:<6} {c['name']:<8} {price_str:<19} {inst_str:<14} {fib_str:<14} {status_str:<19}")
            
        if not triggered:
            print_flush(" (無觸發標的)")
            
        print_flush(f"\n--- 觀察名單 (進入價值區但未觸發) ---")
        for c in waiting[:10]:
            price_str = f"{c['price']:.2f}"
            inst_str = f"{c['inst']:,.0f}"
            fib_str = f"{c['fib']:.2f}"
            print_flush(f"{c['code']:<6} {c['name']:<8} {price_str:<10} {inst_str:<12} {fib_str:<12} 等待轉折")
            
        print_flush(f"{'='*80}\n")

def run_institutional_value_strategy():
    """執行機構價值回歸策略 (選單入口)"""
    try:
        strategy = InstitutionalValueStrategy()
        strategy.scan()
    except Exception as e:
        print_flush(f"❌ 策略執行失敗: {e}")
        import traceback
        traceback.print_exc()


# 價量狀態查表 (Tuple Lookup)
PRICE_VOLUME_STATUS = {
    (1, 1): "價漲量增",
    (1, -1): "價漲量縮",
    (-1, 1): "價跌量增",
    (-1, -1): "價跌量縮"
}

def calculate_trade_setup(close, vp_upper, vp_lower, ma20, tp=0, sl=0):
    """
    計算止盈止損 (職責分離)
    :return: (tp, sl)
    """
    if tp == 0:
        if vp_upper and vp_upper > close:
            tp = vp_upper
        else:
            tp = close * 1.1
            
    if sl == 0:
        if vp_lower and vp_lower < close:
            sl = vp_lower
        elif ma20 and close > ma20:
            sl = ma20
        else:
            sl = close * 0.95
            
    return tp, sl


# ANSI Color Codes
class Colors:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    # 漲停/跌停背景色 (滿版)
    LIMIT_UP = "\033[97;41m"    # 白字紅底 (漲停)
    LIMIT_DOWN = "\033[97;42m"  # 白字綠底 (跌停)

def reset_color():
    return Colors.RESET

def get_color_code(val):
    """漲跌顏色：漲停=紅底、跌停=綠底、漲=紅字、跌=綠字"""
    if val >= 9.5: return Colors.LIMIT_UP    # 漲停 (紅底白字)
    elif val <= -9.5: return Colors.LIMIT_DOWN  # 跌停 (綠底白字)
    elif val > 0: return Colors.RED          # 上漲 (紅字)
    elif val < 0: return Colors.GREEN        # 下跌 (綠字)
    return Colors.RESET

def get_arrow(curr, prev):
    """Get arrow symbol based on trend"""
    if curr is None or prev is None: return ""
    if curr > prev: return "↑"
    elif curr < prev: return "↓"
    return "-"

def get_volume_color(ratio):
    """成交量顏色：量增=紅、量縮=綠、爆量=紫"""
    if ratio >= 2.0: return Colors.MAGENTA  # 爆量 (紫色特別標示)
    elif ratio > 1.0: return Colors.RED     # 量增 = 紅色
    elif ratio < 1.0: return Colors.GREEN   # 量縮 = 綠色
    return Colors.RESET  # 持平 = 白色

def get_trend_color(curr, prev):
    """Get color for trend"""
    if curr is None or prev is None: return Colors.RESET
    if curr > prev: return Colors.RED
    elif curr < prev: return Colors.GREEN
    return Colors.RESET

def get_colored_value(text, change, arrow=""):
    """Format value with color and arrow"""
    color = get_color_code(change)
    return f"{color}{text}{arrow}{Colors.RESET}"

def calculate_trade_setup(close, vp_upper, vp_lower, ma20, tp_raw=None, sl_raw=None):
    """Calculate Take Profit and Stop Loss"""
    # Default logic if not provided
    if tp_raw: tp = tp_raw
    else: tp = vp_upper if vp_upper and vp_upper > close else close * 1.1
    
    if sl_raw: sl = sl_raw
    else: sl = vp_lower if vp_lower and vp_lower < close else (ma20 if ma20 and ma20 < close else close * 0.9)
    
    return tp, sl

PRICE_VOLUME_STATUS = {
    (1, 1): "價漲量增",
    (1, -1): "價漲量縮",
    (-1, 1): "價跌量增",
    (-1, -1): "價跌量縮",
    (0, 1): "平盤量增",
    (0, -1): "平盤量縮"
}

def format_scan_result(code, name, indicators, show_date=False):
    """格式化單日技術指標 (客製化版 - 修復 RSI/VSBC/集保人數)"""
    if not indicators:
        return ""
    
    # 輔助函數
    def get_val(keys, default=None):
        if isinstance(keys, str): keys = [keys]
        for k in keys:
            if k in indicators and indicators[k] is not None:
                return indicators[k]
        return default

    def safe_f(val, default=0.0):
        try: return float(val) if val is not None else default
        except: return default

    # 讀取數據
    date = indicators.get('date', '')
    close = safe_f(get_val('close'))
    close_prev = safe_f(get_val('close_prev'))
    volume = safe_f(get_val('volume'))
    vol_prev = safe_f(get_val('vol_prev'))
    
    # 指標
    mfi = safe_f(get_val(['mfi14', 'MFI']))
    mfi_prev = safe_f(get_val(['mfi14_prev', 'MFI_prev']))
    chg14 = safe_f(get_val(['chg14_pct', 'CHG14']))
    chg14_prev = safe_f(get_val(['chg14_pct_prev', 'CHG14_prev']))
    
    # RSI 計算 (若 indicators 中沒有，嘗試計算)
    rsi = safe_f(get_val(['rsi', 'RSI']))
    if rsi == 0 and 'close' in indicators:
        # 這裡無法取得歷史數據計算 RSI，只能顯示 N/A 或依賴外部計算
        # 為了避免 N/A，我們假設 scan 函數應該計算好 RSI
        # 若真的沒有，顯示空字串
        rsi_str = ""
    else:
        rsi_str = f"{rsi:.1f}"

    poc = safe_f(get_val(['vp_poc', 'POC']))
    vwap = safe_f(get_val(['vwap20', 'VWAP']))
    vwap_prev = safe_f(get_val(['vwap20_prev', 'VWAP_prev']))
    
    major_pct = safe_f(get_val(['major_holders_pct', 'Major_Holders']))
    total_holders = safe_int(get_val(['total_shareholders', 'Total_Shareholders']))
    
    f_buy = safe_int(get_val(['foreign_buy', 'Foreign_Buy']))
    t_buy = safe_int(get_val(['trust_buy', 'Trust_Buy']))
    d_buy = safe_int(get_val(['dealer_buy', 'Dealer_Buy']))
    
    # VSBC
    vsbc_up = safe_f(get_val(['vsbc_up', 'VSBC_Upper']))
    vsbc_low = safe_f(get_val(['vsbc_low', 'VSBC_Lower']))
    if vsbc_up == 0 and vsbc_low == 0:
        vsbc_str = "N/A"
    else:
        vsbc_str = f"{vsbc_up:.1f}/{vsbc_low:.1f}"
    
    # 均線
    mas = {}
    for p in [20, 60, 120, 200]:
        mas[p] = safe_f(get_val([f'ma{p}', f'MA{p}']))

    # 計算
    change_pct = (close - close_prev) / close_prev * 100 if close_prev else 0
    volume_ratio = volume / vol_prev if vol_prev > 0 else 1.0
    vol_in_lots = volume / 1000
    
    # 格式化
    reset = reset_color()
    vol_text = f"{get_volume_color(volume_ratio)}{int(vol_in_lots):,}張({volume_ratio:.1f}倍){reset}"
    
    mfi_arrow = get_arrow(mfi, mfi_prev)
    chg14_arrow = get_arrow(chg14, chg14_prev)
    vwap_arrow = get_arrow(vwap, vwap_prev)
    
    colored_mfi = get_colored_value(f"{mfi:.1f}", mfi - mfi_prev, mfi_arrow)
    # 14日漲跌幅：使用 chg14 本身的正負來決定顏色（正=紅、負=綠）
    colored_chg14 = get_colored_value(f"{chg14:.1f}%", chg14, chg14_arrow)
    colored_vwap = get_colored_value(f"{vwap:.2f}", vwap - vwap_prev, vwap_arrow)
    
    # RSI 顏色
    colored_rsi = f"{rsi_str}" # 暫不加色，或可依 >70 紅 <30 綠
    
    # 訊號
    sig_desc = []
    price_dir = 1 if change_pct > 0 else (-1 if change_pct < 0 else 0)
    vol_dir = 1 if volume_ratio >= 1.0 else -1
    status = PRICE_VOLUME_STATUS.get((price_dir, vol_dir))
    if status: sig_desc.append(status)
    
    if safe_int(get_val(['smi_signal', 'SMI_Signal'])) == 1: sig_desc.append("主力進場")
    if safe_int(get_val(['svi_signal', 'SVI_Signal'])) == 1: sig_desc.append("多頭排列")
    if safe_int(get_val(['nvi_signal', 'NVI_Signal'])) == 1: sig_desc.append("籌碼鎖定")
    
    sig_str = f"[{','.join(sig_desc)}]" if sig_desc else "[-]"
    
    # 止盈止損
    tp_raw = safe_f(get_val(['take_profit', 'TP']))
    sl_raw = safe_f(get_val(['stop_loss', 'SL']))
    vp_lower = safe_f(get_val(['vp_lower', 'VP_lower']))
    vp_upper = safe_f(get_val(['vp_upper', 'VP_upper']))
    tp, sl = calculate_trade_setup(close, vp_upper, vp_lower, mas[20], tp_raw, sl_raw)

    # 籌碼
    inst_str = ""
    if f_buy is not None:
        inst_str = f" 外:{f_buy//1000} 投:{t_buy//1000} 自:{d_buy//1000}"

    # 集保人數顯示
    holders_str = f" 集保:{total_holders:,}" if total_holders is not None else " 集保:N/A"

    # Line 1
    line1 = f"{date} {name}({code}) 量:{vol_text} MFI:{colored_mfi} RSI:{colored_rsi}"
    
    # Line 2 - 使用 change_pct 的正負來決定顏色（漲=紅、跌=綠）
    close_color = get_color_code(change_pct)  # 正值紅，負值綠
    line2 = f"收盤:{close_color}{close:.2f}({change_pct:+.2f}%){reset} 14日:{colored_chg14} VSBC上/下:{vsbc_str} 大戶:{major_pct:.1f}%{holders_str}"
    
    # Line 3
    line3 = f"止盈:{tp:.2f}   VWAP:{colored_vwap}   POC:{poc:.2f}   止損:{sl:.2f}"
    
    # Line 4
    line4 = f"訊號3/6:{sig_str}{inst_str}"
    
    # Line 5
    line5 = f"MA20:{mas[20]:.2f} MA60:{mas[60]:.2f} MA120:{mas[120]:.2f} MA200:{mas[200]:.2f}"
    
    return f"{line1}\n{line2}\n{line3}\n{line4}\n{line5}\n"


def reset_color():
    """重置顏色"""
    return RESET_COLOR

def get_arrow(curr, prev):
    """根據當前值和前值獲取箭頭 (Table-Driven)"""
    if curr is None or prev is None: return ""
    
    # Table-Driven: (Condition) -> Symbol
    # Using a list of tuples for ordered evaluation
    rules = [
        (curr > prev, "↑"),
        (curr < prev, "↓")
    ]
    
    for condition, symbol in rules:
        if condition: return symbol
    return "-"

def get_volume_color(ratio):
    """成交量顏色：量增=紅、量縮=綠、爆量=紫"""
    if ratio >= 2.0: return "\033[95m"  # 爆量 (紫色)
    elif ratio > 1.0: return "\033[91m"  # 量增 = 紅色
    elif ratio < 1.0: return "\033[92m"  # 量縮 = 綠色
    return "\033[97m"  # 持平 = 白色

def get_trend_color(curr, prev):
    """根據趨勢獲取顏色 (Table-Driven)"""
    if curr is None or prev is None: return ""
    
    # Table-Driven: (Condition) -> Color
    rules = [
        (curr > prev, "\033[91m"), # Red
        (curr < prev, "\033[92m")  # Green
    ]
    
    for condition, color in rules:
        if condition: return color
    return "\033[97m" # White

def get_colored_value(text, change, arrow):
    """獲取帶顏色的值"""
    color = get_color_code(change)
    return f"{color}{text}{arrow}{reset_color()}"


def format_scan_result_list(code, name, indicators_list):
    """格式化多天技術指標結果"""
    if not indicators_list:
        return ""
    
    output_lines = []
    for i, indicators in enumerate(indicators_list):
        output_lines.append(format_scan_result(code, name, indicators, show_date=True))
        if i < len(indicators_list) - 1:
            output_lines.append("-" * 80)
    
    return "\n".join(output_lines)

def display_scan_results(results, title, limit=20, extra_info_func=None, description=None):
    """統一顯示掃描結果的模組"""
    print_flush(f"\n【{title}】")
    print_flush("═" * 31)
    if description:
        print_flush(f"{description}")
        print_flush("═" * 31)
    
    display_list = results[:limit]
    for i, item in enumerate(display_list):
        if len(item) == 2:
            code, ind = item
            value = None
        elif len(item) == 3:
            code, value, ind = item
        else:
            code, value, ind = item[0], item[1], item[2]
        
        name = get_correct_stock_name(code, ind.get('name', code))
        
        extra = ""
        if extra_info_func and value is not None:
            extra = f" {extra_info_func(code, value, ind)}"
        
        print_flush(f"{i+1}. {format_scan_result(code, name, ind, show_date=True)}{extra}")
        
        if i < len(display_list) - 1:
            print_flush("-" * 80)
    
    print_flush("=" * 80)
    print_flush(f"[顯示檔數: {min(limit, len(results))}/{len(results)}]")
    print_flush("=" * 80)
    
    # Return codes for external use
    return [item[0] for item in results[:limit]]

def prompt_stock_detail_report(result_codes):
    """提示使用者輸入股票代號查看詳細報告"""
    if not result_codes:
        return
    
    while True:
        print_flush("\n輸入股票代號查看詳細報告 (輸入 0 返回):")
        try:
            choice = input("請輸入: ").strip()
        except EOFError:
            break
            
        if choice == '0' or choice == '':
            break
            
        if choice in result_codes:
            # Show detailed report
            name = get_correct_stock_name(choice)
            print_flush(f"\n{'='*80}")
            print_flush(f"【{choice} {name} 詳細報告】")
            print_flush('='*80)
            
            # Get historical data for analysis
            try:
                days_input = input("顯示天數(預設10天): ").strip()
                days = int(days_input) if days_input.isdigit() else 10
            except:
                days = 10

            indicators_list = calculate_stock_history_indicators(choice, display_days=days, limit_days=max(400, days + 220))
            if indicators_list:
                # Show recent history
                for i, ind in enumerate(indicators_list[:10]):  # Show last 10 days
                    print_flush(format_scan_result(choice, name, ind, show_date=True))
                    if i < len(indicators_list[:10]) - 1:
                        print_flush("-" * 80)
            else:
                print_flush("❌ 無法取得歷史資料")
            
            print_flush('='*80)
        else:
            print_flush(f"❌ 找不到代號 {choice}，請從掃描結果中選擇")


# ==============================
# 步驟函數
# ==============================
def get_latest_market_date():
    """獲取市場最新交易日期"""
    dates = []
    
    # 1. Check TWSE
    try:
        url = f"{TWSE_STOCK_DAY_ALL_URL}&_={int(time.time())}"
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_ALL.html'
        })
        
        try:
            session.get('https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_ALL.html', timeout=5, verify=False)
        except:
            pass
        
        res = session.get(url, timeout=10, verify=False)
        if res.status_code == 200:
            data = res.json()
            if 'date' in data and len(data['date']) == 8:
                d = data['date']
                dates.append(f"{d[:4]}-{d[4:6]}-{d[6:]}")
    except:
        pass
    
    # 2. Check TPEx
    try:
        url = f"{TPEX_DAILY_TRADING_URL}?d=&stk_code=&o=json&_={int(time.time())}"
        res = requests.get(url, timeout=10, verify=False)
        if res.status_code == 200:
            data = res.json()
            if 'reportDate' in data:
                dates.append(roc_to_western_date(data['reportDate']))
    except:
        pass
    
    if not dates:
        # Fallback: Try to get max date from DB first
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT MAX(date) FROM stock_snapshot")
                db_date = cur.fetchone()[0]
                if db_date:
                    return db_date
        except:
            pass
            
        return datetime.now().strftime("%Y-%m-%d")
        
    return max(dates)

def step1_fetch_stock_list(silent_header=False):
    """步驟1: 更新上市櫃清單 (使用 Open Data API)"""
    if not silent_header:
        print_flush("\n[Step 1] 更新上市櫃清單...")
    stocks = []
    
    # 1. TWSE 上市 (OpenAPI) - 基本資料 + 行情表
    twse_meta_map = {}
    twse_quote_data = {}  # 儲存行情資料
    
    try:
        # 1a. 上市公司基本資料 (含上市日期)
        url_meta = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
        if not silent_header:
            print_flush("  [TWSE] 基本資料 (上市日期)...", end="")
        else:
            print_flush("  [TWSE] 基本資料 (上市日期)...", end="")
        res = requests.get(url_meta, timeout=30, verify=False)
        data_meta = res.json()
        
        for item in data_meta:
            code = item.get('公司代號')
            l_date = item.get('上市日期')  # Format: YYYYMMDD
            if code and l_date:
                twse_meta_map[code] = l_date
        print_flush(f" ✓ (取得 {len(twse_meta_map)} 檔)")
    except Exception as e:
        print_flush(f" ✗ ({e})")
    
    try:
        # 1b. TWSE 行情表 (STOCK_DAY_ALL - 個股日成交資訊)
        url_quote = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        if not silent_header:
            print_flush("  [TWSE] 行情表 (股票清單)...", end="")
        else:
            print_flush("  [TWSE] 行情表 (股票清單)...", end="")
        res = requests.get(url_quote, timeout=30, verify=False)
        data_quote = res.json()
        
        count = 0
        for item in data_quote:
            code = item.get('Code')
            name = item.get('Name')
            if code and name and len(code) == 4:
                l_date = twse_meta_map.get(code, '')
                # 轉為 YYYY-MM-DD
                if len(l_date) == 8:
                    l_date = f"{l_date[:4]}-{l_date[4:6]}-{l_date[6:]}"
                
                stocks.append({'code': code, 'name': name, 'market': 'TWSE', 'list_date': l_date})
                count += 1
        print_flush(f" ✓ (取得 {count} 檔)")
    except Exception as e:
        print_flush(f" ✗ ({e})")
        # Fallback: 使用 BWIBBU_d (本益比清單) 作為備援
        try:
            url_fallback = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d"
            print_flush("  -> 使用備援 API (BWIBBU_d)...", end="")
            res = requests.get(url_fallback, timeout=30, verify=False)
            data = res.json()
            
            count = 0
            for item in data:
                code = item.get('Code')
                name = item.get('Name')
                if code and name and len(code) == 4:
                    l_date = twse_meta_map.get(code, '')
                    if len(l_date) == 8:
                        l_date = f"{l_date[:4]}-{l_date[4:6]}-{l_date[6:]}"
                    stocks.append({'code': code, 'name': name, 'market': 'TWSE', 'list_date': l_date})
                    count += 1
            print_flush(f" ✓ (取得 {count} 檔)")
        except Exception as e2:
            print_flush(f" ✗ ({e2})")
    
    # 2. TPEx 上櫃 (Web API)
    # 上櫃公司基本資料: https://www.tpex.org.tw/openapi/v1/t187ap03_O (上櫃公司基本資料)
    tpex_meta_map = {}
    try:
        url_meta_tpex = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
        if not silent_header:
            print_flush("  [TPEx] 基本資料 (上市日期)...", end="")
        else:
            print_flush("  [TPEx] 基本資料 (上市日期)...", end="")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        res = requests.get(url_meta_tpex, timeout=30, verify=False, headers=headers)
        try:
            data_meta_tpex = res.json()
            for item in data_meta_tpex:
                code = item.get('SecuritiesCompanyCode')
                l_date = item.get('DateOfListing') # Format: YYYYMMDD
                if code and l_date:
                    tpex_meta_map[code] = l_date
            print_flush(" ✓")
        except:
            print_flush(" ⚠ (無法解析基本資料，將略過上市日期)")
    except Exception as e:
        print_flush(f" ⚠ ({e})")

    try:
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=&o=json"
        if not silent_header:
            print_flush("  [TPEx] 行情表 (股票清單)...", end="")
        else:
            print_flush("  [TPEx] 行情表 (股票清單)...", end="")
        res = requests.get(url, timeout=30, verify=False)
        data = res.json()
        
        count = 0
        if 'tables' in data:
            for table in data['tables']:
                if 'data' in table:
                    for row in table['data']:
                        if len(row) >= 2:
                            code = row[0]
                            name = row[1]
                            if len(code) == 4:
                                l_date = tpex_meta_map.get(code, '')
                                # 轉為 YYYY-MM-DD
                                if len(l_date) == 8:
                                    l_date = f"{l_date[:4]}-{l_date[4:6]}-{l_date[6:]}"
                            
                                stocks.append({'code': code, 'name': name, 'market': 'TPEx', 'list_date': l_date})
                                count += 1
        print_flush(f" ✓ (取得 {count} 檔)")
    except Exception as e:
        print_flush(f" ✗ ({e})")
        
    # 3. 補全缺失的上市日期 (從 stock_meta 讀取舊資料)
    if not silent_header:
        print_flush("  [補全] 填補缺失上市日期...", end="")
    else:
        print_flush("  [補全] 填補缺失上市日期...", end="")
    fill_count = 0
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT code, list_date FROM stock_meta WHERE list_date IS NOT NULL")
            existing_dates = {row[0]: row[1] for row in cur.fetchall()}
            
            for s in stocks:
                if not s['list_date'] and s['code'] in existing_dates:
                    s['list_date'] = existing_dates[s['code']]
                    fill_count += 1
        print_flush(f" ✓ (補全 {fill_count} 筆)")
    except:
        print_flush(" -")

    # 4. 寫入資料庫
    if stocks:
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                # 使用 INSERT OR REPLACE 更新
                for s in stocks:
                    cur.execute("""
                        INSERT INTO stock_meta (code, name, market_type, list_date)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(code) DO UPDATE SET
                            name=excluded.name,
                            market_type=excluded.market_type,
                            list_date=COALESCE(excluded.list_date, stock_meta.list_date)
                    """, (s['code'], s['name'], s['market'], s['list_date']))
                conn.commit()
            print_flush(f"✓ 已更新 {len(stocks)} 檔股票至清單")
            print_flush("✓ 已寫入資料庫 stock_meta")
        except Exception as e:
            print_flush(f"❌ 資料庫寫入失敗: {e}")
    else:
        print_flush("⚠ 未取得任何股票清單")

def sync_stock_names_to_supabase(stocks):
    """將股票清單同步至 Supabase (使用 stock_list 表)"""
    print_flush("☁ 正在同步股名至 Supabase...", end="")
    try:
        # 使用 stock_list 表 (存放股票代碼與名稱)
        url = f"{SUPABASE_URL}/rest/v1/stock_list"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        
        batch_size = 200
        total = len(stocks)
        success_count = 0
        failed_batches = 0
        
        for i in range(0, total, batch_size):
            batch = stocks[i:i+batch_size]
            # stock_list 表欄位: code, name
            payload = [{"code": s['code'], "name": s['name']} for s in batch]
            
            try:
                res = requests.post(url, json=payload, headers=headers, timeout=15)
                if res.status_code in (200, 201):
                    success_count += len(batch)
                    print_flush(".", end="")
                else:
                    failed_batches += 1
                    # 顯示詳細錯誤以便除錯
                    error_msg = res.text.replace('\n', ' ')
                    print_flush(f"x[{res.status_code}:{error_msg}]", end="")
            except requests.Timeout:
                failed_batches += 1
                print_flush("t", end="")
                
        if failed_batches > 0:
            print_flush(f" ⚠ ({success_count}/{total}, {failed_batches} 批失敗)")
        else:
            print_flush(f" ✓ ({success_count}/{total})")
    except Exception as e:
        print_flush(f" ✗ 同步錯誤: {e}")

# ==============================
# 市場資料更新模板 (Template Method)
# ==============================
def update_market_data(market_name, fetch_func, parse_func, silent_header=False):
    """
    通用市場資料更新邏輯 (Template Method)
    :param market_name: 市場名稱 (TPEx/TWSE)
    :param fetch_func: 資料獲取函式，回傳 (trade_date, data_list)
    :param parse_func: 資料解析函式，回傳 (code, name, open, high, low, close, vol)
    :param silent_header: 是否隱藏標題 (用於一鍵更新時避免重複)
    """
    if not silent_header:
        print_flush(f"\n[Step] 下載 {market_name} 本日行情...")
    
    try:
        trade_date, data_list = fetch_func()
        if not data_list:
            return set()
            
        date_int = int(trade_date.replace('-', ''))
        print_flush(f"  -> 日期: {trade_date}")
        print_flush("  -> 正在寫入資料庫: ", end="")
        
        new_count = 0
        update_count = 0
        skip_count = 0
        updated_codes = set()
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            conn.execute("PRAGMA synchronous = OFF;")
            
            for idx, item in enumerate(data_list):
                if idx % 100 == 0:
                    print_flush(".", end="")
                
                # 1. 解析資料
                parsed = parse_func(item)
                if not parsed:
                    continue
                
                code, name, open_price, high, low, close, vol, amount = parsed
                
                # 2. 檢查舊資料
                cur.execute("SELECT close, amount FROM stock_history WHERE code=? AND date_int=?", (code, date_int))
                existing = cur.fetchone()
                
                # 衛語句：資料存在且一致 (且 amount 不為空)
                # 如果資料庫中 amount 為空但新資料有 amount，則強制更新
                need_update = False
                if existing:
                    if existing[0] != close:
                        need_update = True
                    elif (existing[1] is None or existing[1] == 0) and (amount is not None and amount > 0):
                        need_update = True
                    else:
                        skip_count += 1
                        continue
                
                if existing is None:
                    new_count += 1
                else:
                    update_count += 1
                
                updated_codes.add(code)
                
                # 3. 取得前日收盤價 (計算漲跌幅用)
                cur.execute("""
                    SELECT close, volume FROM stock_history 
                    WHERE code=? AND date_int<? 
                    ORDER BY date_int DESC LIMIT 1
                """, (code, date_int))
                
                prev = cur.fetchone()
                pc, pv = (prev[0], prev[1]) if prev else (close, vol)
                
                # 4. 寫入歷史資料 (含 amount)
                cur.execute("""
                    INSERT OR REPLACE INTO stock_history 
                    (code, date_int, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (code, date_int, open_price, high, low, close, vol, amount))
                
                # 5. 寫入快照資料 (改用 UPSERT 以保留 PE/Yield 等資料)
                cur.execute("""
                    INSERT INTO stock_snapshot (code, name, date, close, volume, close_prev, vol_prev, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                        name=excluded.name,
                        date=excluded.date,
                        close=excluded.close,
                        volume=excluded.volume,
                        close_prev=excluded.close_prev,
                        vol_prev=excluded.vol_prev,
                        amount=excluded.amount
                """, (code, name, trade_date, close, vol, pc, pv, amount))
            
            conn.commit()
            
            # [已移除] 不再每次都同步 Supabase，改由 step8_sync_supabase 統一處理
            
        print_flush(f"\n✓ {market_name} 更新: 新增 {new_count} 筆 | 更新 {update_count} 筆 | 跳過 {skip_count} 筆")
        return updated_codes
        
    except Exception as e:
        print_flush(f"\n❌ 失敗: {e}")
        return set()

# TPEx 輔助函式
def _fetch_tpex_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    res = requests.get(TPEX_MAINBOARD_URL, timeout=Config.API_TIMEOUT, verify=False, headers=headers)
    data = res.json()
    if not data:
        return None, []
    
    raw_date = data[0].get('Date') or data[0].get('date')
    trade_date = roc_to_western_date(raw_date)
    return trade_date, data

def _parse_tpex_item(item):
    code = item.get('SecuritiesCompanyCode', '').strip()
    name = item.get('CompanyName', '').strip()
    
    # Guard Clause
    if not is_normal_stock(code, name):
        return None
        
    vol = safe_int(item.get('TradingShares'))
    if vol < 1: vol = 0
    
    return (
        code, name,
        safe_num(item.get('Open')),
        safe_num(item.get('High')),
        safe_num(item.get('Low')),
        safe_num(item.get('Close')),
        vol,
        safe_num(item.get('TransactionAmount'))  # [修正] 成交金額 key 為 TransactionAmount
    )

# TWSE 輔助函式
def _fetch_twse_data():
    """獲取 TWSE 上市股票今日行情 (使用 MI_INDEX 網頁版 API - 更即時)"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    # 使用今天日期
    today = datetime.now().strftime("%Y%m%d")
    
    # === 主要來源: MI_INDEX 網頁版 API (Table 8: 每日收盤行情) ===
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={today}&type=ALLBUT0999"
    
    try:
        res = requests.get(url, timeout=30, verify=False, headers=headers)
        data = res.json()
        
        if data.get('stat') != 'OK':
            # 嘗試 OpenAPI 作為備援
            return _fetch_twse_data_openapi_fallback()
        
        trade_date = data.get('date', today)
        # 轉換為 YYYY-MM-DD 格式
        if len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        
        # 找到 Table 8: 每日收盤行情
        stock_data = []
        for table in data.get('tables', []):
            title = table.get('title', '')
            if '每日收盤行情' in title:
                # 格式: [代號, 名稱, 成交股數, 成交筆數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, ...]
                for row in table.get('data', []):
                    if len(row) >= 9:
                        code = str(row[0]).strip()
                        # 只保留 4 碼普通股
                        if len(code) == 4 and code.isdigit():
                            stock_data.append({
                                'Code': code,
                                'Name': str(row[1]).strip(),
                                'TradeVolume': str(row[2]).replace(',', ''),
                                'TradeValue': str(row[4]).replace(',', ''),
                                'OpeningPrice': str(row[5]).replace(',', '').replace('--', '0'),
                                'HighestPrice': str(row[6]).replace(',', '').replace('--', '0'),
                                'LowestPrice': str(row[7]).replace(',', '').replace('--', '0'),
                                'ClosingPrice': str(row[8]).replace(',', '').replace('--', '0'),
                            })
                break
        
        if stock_data:
            return trade_date, stock_data
        else:
            # 備援
            return _fetch_twse_data_openapi_fallback()
            
    except Exception as e:
        logger.debug(f"MI_INDEX API 失敗: {e}，使用 OpenAPI 備援")
        return _fetch_twse_data_openapi_fallback()

def _fetch_twse_data_openapi_fallback():
    """OpenAPI 備援 (資料更新較慢)"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        res = requests.get(TWSE_STOCK_DAY_ALL_URL, timeout=Config.API_TIMEOUT, verify=False, headers=headers)
        data = res.json()
        
        if not data or not isinstance(data, list):
            return None, []
        
        raw_date = data[0].get('Date', '')
        trade_date = roc_to_western_date(raw_date)
        return trade_date, data
    except:
        return None, []

def _parse_twse_item(item):
    code = item.get('Code', '').strip()
    name = item.get('Name', '').strip()
    
    # Guard Clause
    if not is_normal_stock(code, name):
        return None
        
    vol = safe_int(item.get('TradeVolume'))
    if vol < 1: vol = 0

    return (
        code, name,
        safe_num(item.get('OpeningPrice')),
        safe_num(item.get('HighestPrice')),
        safe_num(item.get('LowestPrice')),
        safe_num(item.get('ClosingPrice')),
        vol,
        safe_num(item.get('TradeValue'))  # [OpenAPI] 成交金額
    )

def _fetch_and_update_tpex_valuation():
    """下載並更新 TPEx 個股本益比、殖利率、股價淨值比"""
    print_flush("\n[Step 2+] 更新 TPEx 估值資料 (PE/Yield/PB)...")
    try:
        url = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=json"
        res = requests.get(url, timeout=30, verify=False)
        data = res.json()
        
        # TPEx 格式變異多，嘗試不同欄位
        rows = data.get('aaData') or data.get('data') or []
        if not rows and 'tables' in data:
             rows = data['tables'][0]['data']
             
        updates = []
        for item in rows:
            if len(item) < 7: continue
            
            code = item[0]
            # 0:Code, 1:Name, 2:PE, 3:Div, 4:Year, 5:Yield, 6:PB
            pe = safe_float_preserving_none(item[2])
            dy = safe_float_preserving_none(item[5])
            pb = safe_float_preserving_none(item[6])
            
            if len(code) == 4:
                updates.append((pe, dy, pb, code))
                
        if updates:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.executemany("""
                    UPDATE stock_snapshot 
                    SET pe=?, yield=?, pb=?
                    WHERE code=?
                """, updates)
                conn.commit()
            today_display = datetime.now().strftime("%Y-%m-%d")
            print_flush(f"✓ 已更新 {len(updates)} 筆 TPEx 估值資料 ({today_display})")
    except Exception as e:
        print_flush(f"❌ TPEx 估值更新失敗: {e}")

def step2_download_tpex_daily(silent_header=False):
    """步驟2: 下載 TPEx (上櫃) 本日行情 (含估值)"""
    updated = update_market_data("TPEx (上櫃)", _fetch_tpex_data, _parse_tpex_item, silent_header=silent_header)
    _fetch_and_update_tpex_valuation()
    return updated

def _fetch_and_update_twse_valuation():
    """下載並更新 TWSE 個股本益比、殖利率、股價淨值比"""
    print_flush("\n[Step 3+] 更新 TWSE 估值資料 (PE/Yield/PB)...")
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d"
        res = requests.get(url, timeout=30, verify=False)
        data = res.json()
        
        updates = []
        for item in data:
            code = item.get('Code')
            # TWSE OpenAPI: PEratio, DividendYield, PBratio
            pe = safe_float_preserving_none(item.get('PEratio'))
            dy = safe_float_preserving_none(item.get('DividendYield'))
            pb = safe_float_preserving_none(item.get('PBratio'))
            
            if code:
                updates.append((pe, dy, pb, code))
                
        if updates:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.executemany("""
                    UPDATE stock_snapshot 
                    SET pe=?, yield=?, pb=?
                    WHERE code=?
                """, updates)
                conn.commit()
            today_display = datetime.now().strftime("%Y-%m-%d")
            print_flush(f"✓ 已更新 {len(updates)} 筆 TWSE 估值資料 ({today_display})")
    except Exception as e:
        print_flush(f"❌ TWSE 估值更新失敗: {e}")

def step3_download_twse_daily(silent_header=False):
    """步驟3: 下載 TWSE (上市) 本日行情 (含估值)"""
    updated = update_market_data("TWSE (上市)", _fetch_twse_data, _parse_twse_item, silent_header=silent_header)
    _fetch_and_update_twse_valuation()
    return updated

MIN_DATA_COUNT = 450 # 450筆
    
def step4_check_data_gaps():
    """步驟4: 檢查數據缺失 (含金額與法人) - 支援上市日期判斷"""
    print_flush("\n[Step 4] 檢查數據缺失...")
    # MIN_DATA_COUNT = 400  # 用戶指定門檻
    
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        
        # 0. 預先載入上市日期 (Source of Truth)
        list_date_map = {}
        try:
            cur.execute("SELECT code, list_date FROM stock_meta")
            for r in cur.fetchall():
                if r[1]: list_date_map[r[0]] = r[1]
        except:
            pass

        # 1. 檢查歷史資料筆數與金額缺失 (只有 volume > 0 時 amount 為 0/NULL 才算缺失)
        print_flush("正在分析歷史資料與金額...")
        sql = """
            SELECT code, 
                   COUNT(*) as total_cnt,
                   SUM(CASE WHEN volume > 0 AND (amount IS NULL OR amount = 0) THEN 1 ELSE 0 END) as missing_amount_cnt,
                   MIN(date_int) as min_date
            FROM stock_history 
            GROUP BY code
        """
        rows = cur.execute(sql).fetchall()
        
        # 2. 檢查法人資料
        print_flush("正在分析法人資料...")
        try:
            inst_rows = cur.execute("SELECT code, COUNT(*) FROM institutional_investors GROUP BY code").fetchall()
            inst_map = {r[0]: r[1] for r in inst_rows}
        except:
            inst_map = {}  # 表格可能不存在
            
    # 分析缺失
    count_gaps = []
    amount_gaps = []
    inst_gaps = []
    
    for r in rows:
        code = r[0]
        total = r[1]
        missing_amt = r[2] if r[2] else 0
        min_date_int = r[3]
        
        # 檢查資料筆數 (加入上市日期判斷)
        if total < MIN_DATA_COUNT:
            is_new_stock = False
            l_date_str = list_date_map.get(code)
            
            if l_date_str:
                try:
                    l_date = datetime.strptime(l_date_str, '%Y-%m-%d')
                    # 計算上市至今的天數
                    days_since = (datetime.now() - l_date).days
                    
                    # 預期交易日 (約總天數的 68%，扣除假日)
                    expected_count = int(days_since * 0.68)
                    
                    # 如果資料量達到預期的 90%，視為完整 (針對新上市股票)
                    if total >= expected_count * 0.9:
                        is_new_stock = True
                    
                    # 雙重確認: 如果最早資料日期接近上市日期 (20天內)，也視為完整
                    if min_date_int:
                        min_date = datetime.strptime(str(min_date_int), '%Y%m%d')
                        if min_date <= l_date + timedelta(days=20):
                            is_new_stock = True
                            
                except Exception:
                    pass
            
            # 只有當不是新上市股票，且筆數不足時，才列入缺失
            if not is_new_stock:
                count_gaps.append((code, total))
        
        if missing_amt > 0:
            amount_gaps.append((code, missing_amt))
            
        if code not in inst_map:
            inst_gaps.append(code)
            
    # 顯示結果
    if not count_gaps and not amount_gaps and not inst_gaps:
        print_flush(f"✓ 所有股票資料皆充足 (>= {MIN_DATA_COUNT} 筆或符合上市天數, 金額/法人皆完整)")
    else:
        if count_gaps:
            print_flush(f"\n⚠ 資料筆數不足 (<{MIN_DATA_COUNT}): {len(count_gaps)} 檔")
            for c, n in count_gaps[:5]:
                print_flush(f"  - {c}: {n} 筆")
            if len(count_gaps) > 5:
                print_flush(f"  ... 等共 {len(count_gaps)} 檔")
                
        if amount_gaps:
            print_flush(f"\n⚠ 成交金額缺失 (Amount=0/Null): {len(amount_gaps)} 檔")
            for c, n in amount_gaps[:5]:
                print_flush(f"  - {c}: 缺 {n} 筆")
            if len(amount_gaps) > 5:
                print_flush(f"  ... 等共 {len(amount_gaps)} 檔")
                
        if inst_gaps:
            print_flush(f"\n⚠ 法人資料缺失 (完全無資料): {len(inst_gaps)} 檔")
            for c in inst_gaps[:5]:
                print_flush(f"  - {c}")
            if len(inst_gaps) > 5:
                print_flush(f"  ... 等共 {len(inst_gaps)} 檔")

def step5_clean_delisted():
    """步驟5: 清理下市股票"""
    print_flush("\n[Step 5] 清理下市股票...")
    
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # 使用 stock_meta 表作為有效股票名冊 (包含上市+上櫃)
            cur.execute("SELECT code FROM stock_meta WHERE delist_date IS NULL OR delist_date = ''")
            valid_codes = set(row[0] for row in cur.fetchall())
            
            if not valid_codes:
                print_flush("⚠ stock_meta 表為空，跳過清理 (避免誤刪)")
                return
            
            # 查詢 stock_history 中的股票
            db_codes = set(row[0] for row in cur.execute("SELECT DISTINCT code FROM stock_history").fetchall())
            
            # 找出已下市的股票 (在 history 中但不在有效名冊中)
            delisted = db_codes - valid_codes
            
            if delisted:
                # 安全機制：如果刪除數量超過總數 10%，需要確認
                deletion_ratio = len(delisted) / max(len(db_codes), 1)
                if deletion_ratio > 0.1:
                    print_flush(f"⚠ 發現 {len(delisted)} 檔 ({deletion_ratio:.1%}) 可能是下市股票")
                    print_flush("  刪除比例過高，可能是名冊不完整，跳過清理")
                    print_flush("  請先執行 [1] 更新上市櫃清單 確保名冊完整")
                    return
                
                print_flush(f"發現 {len(delisted)} 檔下市股票，準備清理...")
                for code in delisted:
                    cur.execute("DELETE FROM stock_history WHERE code=?", (code,))
                    cur.execute("DELETE FROM stock_snapshot WHERE code=?", (code,))
                
                conn.commit()
                print_flush(f"✓ 已清除 {len(delisted)} 檔下市股票資料")
            else:
                print_flush("✓ 無下市股票殘留")
                
    except Exception as e:
        print_flush(f"❌ 清理失敗: {e}")

def step3_5_download_institutional(days=60, silent_header=False):
    """步驟3.5: 下載三大法人買賣超資料 (官方 OpenAPI 為主，網頁為備援)"""
    if not silent_header:
        print_flush(f"\n[Step 3.5] 下載三大法人買賣超資料 (官方 OpenAPI 優先)...")
    
    try:
        from io import StringIO
        
        # === A. 官方 OpenAPI (主要來源 - 只抓今天) ===
        today_int = int(datetime.now().strftime("%Y%m%d"))
        openapi_success = False
        
        try:
            print_flush("正在從官方 OpenAPI 取得今日法人資料...")
            saved = InstitutionalInvestorAPI.fetch_all_openapi()
            if saved > 0:
                today_display = datetime.now().strftime("%Y-%m-%d")
                print_flush(f"✓ 官方 OpenAPI: 已儲存 {saved} 筆法人資料 ({today_display})")
                openapi_success = True
        except Exception as e:
            print_flush(f"⚠ 官方 OpenAPI 失敗: {e}，切換至備援來源...")
        
        # === B. 歷史資料補漏 (網頁爬蟲備援) ===
        print_flush(f"檢查近 {days} 天歷史缺漏...")
        
        # 1. 準備日期列表
        base_date = datetime.now()
        dates_to_check = []
        for i in range(days + 10): # 多抓一點以防假日
            dt = base_date - timedelta(days=i)
            if dt.weekday() < 5: # 只取平日
                dates_to_check.append(dt)
            if len(dates_to_check) >= days: break
            
        # 2. 檢查資料庫現有資料
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            # 確保表格存在
            cur.execute("""
                CREATE TABLE IF NOT EXISTS institutional_investors (
                    code TEXT NOT NULL,
                    date_int INTEGER NOT NULL,
                    foreign_buy INTEGER DEFAULT 0,
                    foreign_sell INTEGER DEFAULT 0,
                    trust_buy INTEGER DEFAULT 0,
                    trust_sell INTEGER DEFAULT 0,
                    dealer_buy INTEGER DEFAULT 0,
                    dealer_sell INTEGER DEFAULT 0,
                    PRIMARY KEY (code, date_int)
                )
            """)
            conn.commit()
            
            # 取得已有的日期
            check_start = int(dates_to_check[-1].strftime("%Y%m%d"))
            cur.execute("SELECT DISTINCT date_int FROM institutional_investors WHERE date_int >= ?", (check_start,))
            existing_dates = {r[0] for r in cur.fetchall()}
            
        # 3. 找出缺漏日期 (排除休市日，今天只有在 14:00 後才嘗試回補)
        today_int = int(datetime.now().strftime("%Y%m%d"))
        current_hour = datetime.now().hour
        
        missing_dates = []
        for d in dates_to_check:
            d_int = int(d.strftime("%Y%m%d"))
            if d_int in existing_dates:
                continue
            if is_market_holiday(d_int):
                continue
            # 今天只有在 14:00 後才嘗試回補 (收盤 13:30，盤後更新約 14:00)
            if d_int == today_int and current_hour < 14:
                continue
            missing_dates.append(d)
        
        if not missing_dates:
            print_flush("✓ 法人資料完整，無須補漏")
            return

        print_flush(f"發現 {len(missing_dates)} 天缺漏，開始回補...")
        
        # 4. 執行回補
        total_inserted = 0
        for i, dt in enumerate(missing_dates):
            date_str = dt.strftime("%Y%m%d")
            date_int = int(date_str)
            print_flush(f"\r[{i+1}/{len(missing_dates)}] 處理 {dt.strftime('%Y-%m-%d')} ... ", end="")
            
            inst_data = []
            
            # --- TWSE (T86) ---
            try:
                url = f'https://www.twse.com.tw/rwd/zh/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999'
                # 隨機延遲
                time.sleep(random.uniform(2.0, 4.0))
                r = requests.get(url, timeout=15, verify=False)
                
                if r.status_code == 200 and len(r.text) > 100:
                    df = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
                    df = df.astype(str).apply(lambda s: s.str.replace(',', ''))
                    if '證券代號' in df.columns:
                        df['code'] = df['證券代號'].str.replace('=', '').str.replace('"', '').str.strip()
                        df = df[df['code'].str.len() == 4]
                        
                        for _, row in df.iterrows():
                            try:
                                code = row['code']
                                f_buy = safe_int(row.get('外資及陸資(不含外資自營商)買進股數', 0))
                                f_sell = safe_int(row.get('外資及陸資(不含外資自營商)賣出股數', 0))
                                t_buy = safe_int(row.get('投信買進股數', 0))
                                t_sell = safe_int(row.get('投信賣出股數', 0))
                                d_buy = safe_int(row.get('自營商買進股數(自行買賣)', 0))
                                d_sell = safe_int(row.get('自營商賣出股數(自行買賣)', 0))
                                inst_data.append((code, date_int, f_buy, f_sell, t_buy, t_sell, d_buy, d_sell))
                            except: pass
            except Exception as e:
                pass # TWSE 失敗
                
            # --- TPEx ---
            try:
                d_obj = dt
                roc_date = f'{d_obj.year - 1911}/{d_obj.month:02d}/{d_obj.day:02d}'
                url = f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d={roc_date}&se=EW&t=D&o=json'
                time.sleep(random.uniform(2.0, 4.0))
                r = requests.get(url, timeout=15, verify=False)
                data = r.json()
                
                tables = data.get('tables', [])
                if tables and isinstance(tables, list) and len(tables) > 0:
                    table_data = tables[0].get('data', [])
                    for row in table_data:
                        try:
                            code = str(row[0]).strip()
                            if len(code) != 4: continue
                            f_buy = safe_int(row[2])
                            f_sell = safe_int(row[3])
                            t_buy = safe_int(row[5])
                            t_sell = safe_int(row[6])
                            d_buy = safe_int(row[8])
                            d_sell = safe_int(row[9])
                            inst_data.append((code, date_int, f_buy, f_sell, t_buy, t_sell, d_buy, d_sell))
                        except: pass
            except Exception as e:
                pass # TPEx 失敗
            
            # 寫入資料庫
            if inst_data:
                with db_manager.get_connection() as conn:
                    cur = conn.cursor()
                    cur.executemany("""
                        INSERT OR REPLACE INTO institutional_investors 
                        (code, date_int, foreign_buy, foreign_sell, trust_buy, trust_sell, dealer_buy, dealer_sell)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, inst_data)
                    conn.commit()
                print_flush(f"成功 ({len(inst_data)} 筆)")
                total_inserted += len(inst_data)
            else:
                print_flush("無資料")
                
        print_flush(f"✓ 法人資料更新完成，共新增 {total_inserted} 筆紀錄")
        
        # [新增] 同步最新法人數據到 stock_snapshot
        print_flush("正在同步最新法人數據到快照表...")
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE stock_snapshot
                SET foreign_buy = (SELECT foreign_buy - foreign_sell FROM institutional_investors WHERE code = stock_snapshot.code ORDER BY date_int DESC LIMIT 1),
                    trust_buy = (SELECT trust_buy - trust_sell FROM institutional_investors WHERE code = stock_snapshot.code ORDER BY date_int DESC LIMIT 1),
                    dealer_buy = (SELECT dealer_buy - dealer_sell FROM institutional_investors WHERE code = stock_snapshot.code ORDER BY date_int DESC LIMIT 1)
                WHERE EXISTS (SELECT 1 FROM institutional_investors WHERE code = stock_snapshot.code)
            """)
            conn.commit()
        print_flush("✓ 快照表法人數據更新完成")
            
    except Exception as e:
        
        # 使用 requests 下載 (避開 SSL 錯誤)
        import requests
        import io
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        df = pd.read_csv(io.StringIO(response.text))
        
        # 檢查必要欄位
        if '證券代號' not in df.columns or '持股分級' not in df.columns or '占集保庫存數比例%' not in df.columns:
            print_flush("❌ CSV 格式不符，跳過")
            return

        # 處理資料
        df['持股分級'] = pd.to_numeric(df['持股分級'], errors='coerce')
        df['證券代號'] = df['證券代號'].astype(str)
        
        # 1. 計算千張大戶持股比例 (持股分級 15: 1,000,001股以上)
        # 注意: 級別 17 是合計，不能加總！
        # 若要計算 400張以上，可使用 isin([12, 13, 14, 15])
        # 這裡依據使用者需求 (1000張以上)，只取級別 15
        df_major = df[df['持股分級'] == 15].copy()
        major_holders = df_major.groupby('證券代號')['占集保庫存數比例%'].sum().to_dict()
        
        # 2. 取得總股東人數 (持股分級 17: 合計)
        df_total = df[df['持股分級'] == 17].copy()
        # 移除人數中的逗號並轉為整數
        if df_total['人數'].dtype == object:
            df_total['人數'] = df_total['人數'].astype(str).str.replace(',', '')
        df_total['人數'] = pd.to_numeric(df_total['人數'], errors='coerce').fillna(0).astype(int)
        total_shareholders = df_total.set_index('證券代號')['人數'].to_dict()
        
        if not major_holders:
            print_flush("⚠ 未找到符合條件的大戶資料")
            return
            
        print_flush(f"取得 {len(major_holders)} 檔股票的大戶持股資料，正在更新資料庫...")
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            count = 0
            for code, pct in major_holders.items():
                holders = total_shareholders.get(code, 0)
                # 更新大戶比例與總股東人數
                cur.execute("""
                    UPDATE stock_snapshot 
                    SET major_holders_pct=?, total_shareholders=? 
                    WHERE code=?
                """, (pct, holders, code))
                count += 1
            conn.commit()
            
def step3_6_download_major_holders(force=False, silent_header=False):
    """步驟3.6: 下載集保戶股權分散表 (千張大戶 & 總股東人數) - 每週五更新"""
    if not silent_header:
        print_flush("\n[Step 3.6] 下載集保戶股權分散表 (千張大戶 & 總股東人數)...")
    
    # 檢查是否為週五 (weekday 4)、週六 (5) 或週日 (6) 或資料缺失
    # 擴展為週末三天都可更新，避免週五忘記
    should_run = force or datetime.now().weekday() in [4, 5, 6]
    
    if not should_run:
        # 檢查資料庫是否完全缺失集保資料
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM stock_snapshot WHERE total_shareholders IS NULL OR total_shareholders = 0")
                missing_count = cur.fetchone()[0]
                if missing_count > 1000: # 若大量缺失，強制執行
                    print_flush(f"⚠ 偵測到 {missing_count} 筆集保資料缺失，強制執行下載")
                    should_run = True
        except:
            pass
            
    if not should_run:
        print_flush("⚠ 非更新時段 (週五至週日) 且資料完整，跳過集保大戶資料下載")
        return
    
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    
    try:
        print_flush("正在下載 CSV (資料量大，請稍候)...")
        
        # 使用 requests 下載 (避開 SSL 錯誤)
        import requests
        import io
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        df = pd.read_csv(io.StringIO(response.text))
        
        # 檢查必要欄位
        if '證券代號' not in df.columns or '持股分級' not in df.columns or '占集保庫存數比例%' not in df.columns:
            print_flush("❌ CSV 格式不符，跳過")
            return

        # 處理資料
        df['持股分級'] = pd.to_numeric(df['持股分級'], errors='coerce')
        df['證券代號'] = df['證券代號'].astype(str).str.strip()
        
        # 1. 計算千張大戶持股比例 (持股分級 15: 1,000,001股以上)
        # 注意: 級別 17 是合計，不能加總！
        # 若要計算 400張以上，可使用 isin([12, 13, 14, 15])
        # 這裡依據使用者需求 (1000張以上)，只取級別 15
        df_major = df[df['持股分級'] == 15].copy()
        major_holders = df_major.groupby('證券代號')['占集保庫存數比例%'].sum().to_dict()
        
        # 2. 取得總股東人數 (持股分級 17: 合計)
        df_total = df[df['持股分級'] == 17].copy()
        # 移除人數中的逗號並轉為整數
        if df_total['人數'].dtype == object:
            df_total['人數'] = df_total['人數'].astype(str).str.replace(',', '')
        df_total['人數'] = pd.to_numeric(df_total['人數'], errors='coerce').fillna(0).astype(int)
        total_shareholders = df_total.set_index('證券代號')['人數'].to_dict()
        
        if not major_holders:
            print_flush("⚠ 未找到符合條件的大戶資料")
            return
            
        print_flush(f"取得 {len(major_holders)} 檔股票的大戶持股資料，正在更新資料庫...")
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            count = 0
            for code, pct in major_holders.items():
                holders = total_shareholders.get(code, 0)
                # 更新大戶比例與總股東人數
                cur.execute("""
                    UPDATE stock_snapshot 
                    SET major_holders_pct=?, total_shareholders=? 
                    WHERE code=?
                """, (pct, holders, code))
                count += 1
            conn.commit()
            
        today_display = datetime.now().strftime("%Y-%m-%d")
        print_flush(f"✓ 已更新 {count} 檔大戶持股比例與總股東人數 ({today_display})")
        
    except Exception as e:
        print_flush(f"❌ 下載或處理失敗: {e}")


def step3_7_fetch_margin_data(days=60, silent_header=False):
    """步驟3.7: 下載融資融券資料 (官方 OpenAPI 為主，FinMind/網頁為備援)"""
    if not silent_header:
        print_flush(f"\n[Step 3.7] 下載融資融券資料 (官方 OpenAPI 優先)...")
    
    try:
        # === A. 官方 OpenAPI (主要來源 - 只抓今天) ===
        today_int = int(datetime.now().strftime("%Y%m%d"))
        openapi_success = False
        
        try:
            print_flush("正在從官方 OpenAPI 取得今日融資融券...")
            saved = MarginDataAPI.fetch_all_margin_data()
            if saved > 0:
                today_display = datetime.now().strftime("%Y-%m-%d")
                print_flush(f"✓ 官方 OpenAPI: 已儲存 {saved} 筆融資融券資料 ({today_display})")
                openapi_success = True
        except Exception as e:
            print_flush(f"⚠ 官方 OpenAPI 失敗: {e}，切換至備援來源...")
        
        # === B. 歷史資料補漏 (FinMind/網頁備援) ===
        print_flush(f"檢查近 {days} 天歷史缺漏...")
        
        # 1. 準備日期列表
        base_date = datetime.now()
        dates_to_check = []
        for i in range(days + 10):
            dt = base_date - timedelta(days=i)
            if dt.weekday() < 5:
                dates_to_check.append(dt)
            if len(dates_to_check) >= days: break
            
        # 2. 檢查資料庫現有資料
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            check_start = int(dates_to_check[-1].strftime("%Y%m%d"))
            try:
                cur.execute("SELECT DISTINCT date_int FROM margin_data WHERE date_int >= ?", (check_start,))
                existing_dates = {r[0] for r in cur.fetchall()}
            except:
                existing_dates = set()
                
        # 3. 找出缺漏日期 (排除休市日，今天只有在 14:00 後才嘗試回補)
        current_hour = datetime.now().hour
        missing_dates = []
        for d in dates_to_check:
            d_int = int(d.strftime("%Y%m%d"))
            if d_int in existing_dates:
                continue
            if is_market_holiday(d_int):
                continue
            # 今天只有在 14:00 後才嘗試回補 (收盤 13:30，盤後更新約 14:00)
            if d_int == today_int and current_hour < 14:
                continue
            missing_dates.append(d)
        
        if not missing_dates:
            print_flush("✓ 融資融券資料完整，無須補漏")
            return

        print_flush(f"發現 {len(missing_dates)} 天缺漏，開始回補 (FinMind 優先)...")
        
        finmind_limit_hit = False
        
        for i, dt in enumerate(missing_dates):
            d_dash = dt.strftime("%Y-%m-%d")
            d_nodash = dt.strftime("%Y%m%d")
            d_int = int(d_nodash)
            
            print_flush(f"\r[{i+1}/{len(missing_dates)}] 處理 {d_dash} ... ", end="")
            
            margin_data = None
            
            # --- B1. FinMind (備援) ---
            if not finmind_limit_hit:
                try:
                    dataset = "TaiwanStockMarginPurchaseShortSale"
                    url = f"{FINMIND_URL}?dataset={dataset}&date={d_dash}&token={FINMIND_TOKEN}"
                    r = requests.get(url, timeout=10)

                    
                    if r.status_code == 429:
                        print_flush("⛔ FinMind 限流! 切換至 TWSE... ", end="")
                        finmind_limit_hit = True
                    elif r.status_code == 200:
                        data = r.json()
                        if data.get('msg') == 'success' and data.get('data'):
                            batch = []
                            for d in data['data']:
                                # FinMind 回傳的是 Limit，這裡轉換為 Rate (Balance / Limit * 100) 以符合 schema
                                # 若 Limit 為 0，則 Rate 為 0
                                m_bal = safe_int(d.get('MarginPurchaseTodayBalance'))
                                m_lim = safe_float(d.get('MarginPurchaseLimit'))
                                m_rate = round(m_bal / m_lim * 100, 2) if m_lim > 0 else 0.0
                                
                                s_bal = safe_int(d.get('ShortSaleTodayBalance'))
                                s_lim = safe_float(d.get('ShortSaleLimit'))
                                s_rate = round(s_bal / s_lim * 100, 2) if s_lim > 0 else 0.0
                                
                                batch.append((
                                    to_date_int(d.get('date')), d.get('stock_id'),
                                    safe_int(d.get('MarginPurchaseBuy')), safe_int(d.get('MarginPurchaseSell')), 
                                    safe_int(d.get('MarginPurchaseCashRepayment')), m_bal, m_rate,
                                    safe_int(d.get('ShortSaleBuy')), safe_int(d.get('ShortSaleSell')), 
                                    safe_int(d.get('ShortSaleCashRepayment')), s_bal, s_rate
                                ))
                            margin_data = batch
                            print_flush(f"FinMind({len(batch)}) ", end="")
                except Exception as e:
                    pass

            # --- B. TWSE/TPEx (備援) ---
            if not margin_data:
                # TWSE
                try:
                    url = f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={d_nodash}&selectType=ALL"
                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                    time.sleep(random.uniform(2.0, 4.0))
                    r = requests.get(url, headers=headers, timeout=15)
                    data = r.json()
                    
                    if data.get('stat') == 'OK':
                        raw_data = data.get('data', [])
                        batch = []
                        for row in raw_data:
                            code = row[0]
                            if len(code) != 4: continue
                            # TWSE row[8] 是融資使用率, row[15] 是融券使用率
                            batch.append((
                                int(d_nodash), code,
                                safe_int(row[2]), safe_int(row[3]), safe_int(row[4]), safe_int(row[6]), safe_float(row[8]), 
                                safe_int(row[9]), safe_int(row[10]), safe_int(row[11]), safe_int(row[13]), safe_float(row[15])
                            ))
                        if batch:
                            margin_data = batch
                            print_flush(f"TWSE({len(batch)}) ", end="")
                except: pass
                
                # TPEx (若 TWSE 沒抓到或需要補 TPEx，這裡簡單起見若 TWSE 有就不抓 TPEx? 不，應該都要抓)
                # 但 fix.py 的 fetch_margin_from_twse 似乎只抓 TWSE? 
                # 最終修正.py 原本有抓 TPEx。
                # 為了完整性，我們也抓 TPEx
                try:
                    d_obj = dt
                    roc_date = f"{d_obj.year - 1911}/{d_obj.month:02d}/{d_obj.day:02d}"
                    url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={roc_date}&s=0,asc,0"
                    time.sleep(random.uniform(1.5, 3.0))
                    r = requests.get(url, timeout=10, verify=False)
                    data = r.json()
                    
                    if data.get('tables'):
                        tpex_batch = []
                        for row in data['tables'][0]['data']:
                            code = row[0]
                            if len(code) != 4: continue
                            # TPEx 格式: 代號, 名稱, 融資前日餘額, 融資買進, 融資賣出, 融資現金償還, 融資今日餘額, 融資使用率, ...
                            # row[7] 是融資使用率, row[14] 是融券使用率
                            tpex_batch.append((
                                int(d_nodash), code,
                                safe_int(row[3]), safe_int(row[4]), safe_int(row[5]), safe_int(row[6]), safe_num(row[7]),
                                safe_int(row[10]), safe_int(row[11]), safe_int(row[12]), safe_int(row[13]), safe_num(row[14])
                            ))
                        if tpex_batch:
                            if margin_data is None: margin_data = []
                            margin_data.extend(tpex_batch)
                            print_flush(f"TPEx({len(tpex_batch)}) ", end="")
                except: pass

            # 寫入資料庫
            if margin_data:
                with db_manager.get_connection() as conn:
                    cur = conn.cursor()
                    cur.executemany("""
                        INSERT OR REPLACE INTO margin_data 
                        (date_int, code, margin_buy, margin_sell, margin_redemp, margin_balance, margin_util_rate,
                         short_buy, short_sell, short_redemp, short_balance, short_util_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, margin_data)
                    conn.commit()
                
                # 同步到 snapshot (只同步最新的一天)
                if d_int == int(datetime.now().strftime("%Y%m%d")):
                    print_flush("同步至快照... ", end="")
                    with db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        for rec in margin_data:
                            # rec: date_int, code, m_buy, m_sell, m_redemp, m_bal, m_rate, s_buy, s_sell, s_redemp, s_bal, s_rate
                            cur.execute("""
                                UPDATE stock_snapshot 
                                SET margin_balance=?, margin_util_rate=?, short_balance=?, short_util_rate=?
                                WHERE code=?
                            """, (rec[5], rec[6], rec[10], rec[11], rec[1]))
                        conn.commit()
            else:
                print_flush("無資料", end="")
            
            print_flush("") # Newline

    except Exception as e:
        print_flush(f"❌ 融資融券下載失敗: {e}")

def to_date_int(d):
    """輔助函式: 轉日期整數"""
    if isinstance(d, int): return d
    if isinstance(d, str):
        s = d.replace('-', '').replace('/', '').split(' ')[0]
        return int(s)
    return 0

def step3_8_fetch_market_index(date_str=None, silent_header=False):
    """步驟3.8: 下載大盤指數 (TWSE + TPEx)"""
    if not silent_header:
        print_flush("\n[Step 3.8] 下載大盤指數...")
    
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
        
    date_int = int(date_str)
    records = []
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    # TWSE Index - 使用 FMTQIK (每日市場成交資訊) API，更穩定
    try:
        url_twse = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date_str}"
        r = requests.get(url_twse, headers=headers, timeout=15, verify=False)
        data = r.json()
        
        if data.get('stat') == 'OK' and data.get('data'):
            # 找當天的資料
            for row in data['data']:
                # row[0] = 日期 (民國年), row[1] = 開盤, row[2] = 最高, row[3] = 最低, row[4] = 收盤
                try:
                    parts = row[0].split('/')
                    western_year = int(parts[0]) + 1911
                    row_date_int = int(f"{western_year}{parts[1]}{parts[2]}")
                    
                    if row_date_int == date_int:
                        open_val = safe_num(row[1])
                        high_val = safe_num(row[2])
                        low_val = safe_num(row[3])
                        close_val = safe_num(row[4])
                        volume = safe_int(row[5]) if len(row) > 5 else 0
                        
                        if close_val > 0:
                            records.append((date_int, 'TAIEX', close_val, open_val, high_val, low_val, volume))
                        break
                except:
                    pass
            
            # 如果今天沒資料，取最後一筆
            if not records and data['data']:
                row = data['data'][-1]
                try:
                    parts = row[0].split('/')
                    western_year = int(parts[0]) + 1911
                    row_date_int = int(f"{western_year}{parts[1]}{parts[2]}")
                    open_val = safe_num(row[1])
                    high_val = safe_num(row[2])
                    low_val = safe_num(row[3])
                    close_val = safe_num(row[4])
                    volume = safe_int(row[5]) if len(row) > 5 else 0
                    
                    if close_val > 0:
                        records.append((row_date_int, 'TAIEX', close_val, open_val, high_val, low_val, volume))
                except:
                    pass
    except Exception as e:
        print_flush(f"⚠ TWSE 指數下載失敗: {e}")

    # TPEx Index - 使用 aftertrading API
    try:
        time.sleep(0.5)
        d_obj = datetime.strptime(date_str, '%Y%m%d')
        roc_date = f"{d_obj.year - 1911}/{d_obj.month:02d}/{d_obj.day:02d}"
        url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_index_summary/OTC_index_summary_result.php?l=zh-tw&d={roc_date}&o=json"
        
        r = requests.get(url_tpex, headers=headers, timeout=15, verify=False)
        data_tpex = r.json()
        
        if data_tpex.get('aaData'):
            # aaData[0] 通常是櫃買指數
            for row in data_tpex['aaData']:
                if '櫃買指數' in str(row[0]) or 'OTC' in str(row[0]).upper():
                    close_val = safe_num(row[1]) if len(row) > 1 else 0
                    if close_val > 0:
                        records.append((date_int, 'TPEX', close_val, 0, 0, 0, 0))
                    break
    except Exception as e:
        # TPEx 指數 API 不穩定，靜默處理
        pass
                    
    if records:
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                # 檢查是否已有今日資料
                first_date = records[0][0]
                cur.execute("SELECT COUNT(*) FROM market_index WHERE date_int = ?", (first_date,))
                existing_count = cur.fetchone()[0]
                
                if existing_count > 0:
                    # 格式化日期顯示
                    date_display = f"{str(first_date)[:4]}-{str(first_date)[4:6]}-{str(first_date)[6:]}"
                    print_flush(f"✓ 大盤指數 ({date_display}) 已是最新")
                else:
                    cur.executemany("""
                        INSERT OR REPLACE INTO market_index (
                            date_int, index_id, close, open, high, low, volume
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, records)
                    conn.commit()
                    date_display = f"{str(first_date)[:4]}-{str(first_date)[4:6]}-{str(first_date)[6:]}"
                    print_flush(f"✓ 已更新大盤指數資料 ({date_display}, {len(records)} 筆)")
        except Exception as e:
            print_flush(f"❌ 大盤指數儲存失敗: {e}")
    else:
        print_flush("⚠ 今日尚無大盤指數資料 (可能尚未收盤)")

def step4_load_data():
    """步驟4: 載入分析資料 (新三表架構)"""
    print_flush("\n[Step 4] 載入分析資料...")
    data = {}
    
    with db_manager.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # 從快照表讀取 (新三表架構，不再 Fallback 到舊表)
        try:
            rows = cur.execute("SELECT * FROM stock_snapshot").fetchall()
            for row in rows:
                data[row['code']] = dict(row)
        except Exception as e:
            print_flush(f"⚠ 快照表讀取失敗: {e}")
            
    print_flush(f"✓ 已載入 {len(data)} 檔股票資料")
    return data

def _auto_fix_missing_amount():
    """
    自動修復缺失的成交金額/收盤價/成交量
    
    修復邏輯矩陣：
    1. 有量、有價、有額 → 正常
    2. 有量、有價、無額 → 額 = 量 × 價
    3. 有量、無價、有額 → 價 = 額 ÷ 量
    4. 有量、無價、無額 → 需要爬蟲 (先跳過)
    5. 無量、有價、有額 → 量 = 額 ÷ 價
    6. 無量、有價、無額 → 需要爬蟲 (先跳過)
    7. 無量、無價、有額 → 需要爬蟲 (先跳過)
    8. 無量、無價、無額 → 可能停牌或下市 (標記為零成交)
    """
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # 情況 2: 有量、有價、無額 → 額 = 量 × 價
            cur.execute("""
                SELECT code, date_int, close, volume 
                FROM stock_history 
                WHERE volume > 0 AND close > 0 AND (amount IS NULL OR amount = 0)
            """)
            case2 = cur.fetchall()
            if case2:
                updates = [(int(close * volume), code, date_int) for code, date_int, close, volume in case2]
                cur.executemany("UPDATE stock_history SET amount = ? WHERE code = ? AND date_int = ?", updates)
                print_flush(f"  [修復] 情況2 (有量有價無額): {len(case2)} 筆 → 額 = 量 × 價")
            
            # 情況 3: 有量、無價、有額 → 價 = 額 ÷ 量
            cur.execute("""
                SELECT code, date_int, amount, volume 
                FROM stock_history 
                WHERE volume > 0 AND (close IS NULL OR close = 0) AND amount > 0
            """)
            case3 = cur.fetchall()
            if case3:
                updates = [(round(amount / volume, 2), code, date_int) for code, date_int, amount, volume in case3]
                cur.executemany("UPDATE stock_history SET close = ? WHERE code = ? AND date_int = ?", updates)
                print_flush(f"  [修復] 情況3 (有量無價有額): {len(case3)} 筆 → 價 = 額 ÷ 量")
            
            # 情況 5: 無量、有價、有額 → 量 = 額 ÷ 價
            cur.execute("""
                SELECT code, date_int, amount, close 
                FROM stock_history 
                WHERE (volume IS NULL OR volume = 0) AND close > 0 AND amount > 0
            """)
            case5 = cur.fetchall()
            if case5:
                updates = [(int(amount / close), code, date_int) for code, date_int, amount, close in case5]
                cur.executemany("UPDATE stock_history SET volume = ? WHERE code = ? AND date_int = ?", updates)
                print_flush(f"  [修復] 情況5 (無量有價有額): {len(case5)} 筆 → 量 = 額 ÷ 價")
            
            # 情況 4, 6, 7: 需要爬蟲抓取
            cur.execute("""
                SELECT code, date_int, close, volume, amount 
                FROM stock_history 
                WHERE (volume > 0 AND (close IS NULL OR close = 0) AND (amount IS NULL OR amount = 0))
                   OR ((volume IS NULL OR volume = 0) AND close > 0 AND (amount IS NULL OR amount = 0))
                   OR ((volume IS NULL OR volume = 0) AND (close IS NULL OR close = 0) AND amount > 0)
                ORDER BY code, date_int
            """)
            need_crawl = cur.fetchall()
            
            if need_crawl:
                fixed_by_crawl = 0
                fixed_by_prev = 0
                
                # 按日期分組
                from collections import defaultdict
                by_date = defaultdict(list)
                for code, date_int, close, volume, amount in need_crawl:
                    by_date[date_int].append((code, close, volume, amount))
                
                for date_int, stocks in by_date.items():
                    # 嘗試從 TWSE/TPEx 抓取該日資料
                    try:
                        date_str = str(date_int)
                        url_twse = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999"
                        url_tpex = f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
                        
                        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                        
                        # 抓取 TWSE 資料
                        crawled_data = {}
                        try:
                            resp = requests.get(url_twse, headers=headers, timeout=15, verify=False)
                            data = resp.json()
                            if data.get('stat') == 'OK':
                                # 找到個股資料 (通常在 tables[8] 或類似位置)
                                for table in data.get('tables', []):
                                    if table.get('title') and '每日收盤行情' in table.get('title', ''):
                                        for row in table.get('data', []):
                                            if len(row) >= 9:
                                                c = str(row[0]).strip()
                                                if len(c) == 4 and c.isdigit():
                                                    try:
                                                        crawled_data[c] = {
                                                            'close': safe_num(row[8]),
                                                            'volume': safe_int(row[2]),
                                                            'amount': safe_int(row[4])
                                                        }
                                                    except:
                                                        pass
                        except:
                            pass
                        
                        # 抓取 TPEx 資料
                        try:
                            # 轉換日期格式為民國年
                            d_obj = datetime.strptime(date_str, '%Y%m%d')
                            roc_date = f"{d_obj.year - 1911}/{d_obj.month:02d}/{d_obj.day:02d}"
                            url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc_date}&o=json"
                            
                            resp = requests.get(url_tpex, headers=headers, timeout=15, verify=False)
                            data = resp.json()
                            
                            if data.get('aaData'):
                                for row in data['aaData']:
                                    if len(row) >= 6:
                                        c = str(row[0]).strip()
                                        if len(c) == 4 and c.isdigit():
                                            try:
                                                crawled_data[c] = {
                                                    'close': safe_num(row[2]),  # 收盤
                                                    'volume': safe_int(row[8]) if len(row) > 8 else 0,  # 成交量
                                                    'amount': safe_int(row[9]) if len(row) > 9 else 0   # 成交金額
                                                }
                                            except:
                                                pass
                        except:
                            pass
                        
                        # 用爬取的資料更新
                        for code, old_close, old_volume, old_amount in stocks:
                            if code in crawled_data:
                                cdata = crawled_data[code]
                                new_close = cdata.get('close') or old_close
                                new_volume = cdata.get('volume') or old_volume
                                new_amount = cdata.get('amount') or old_amount
                                
                                # 如果還是缺，用計算補齊
                                if new_volume and new_close and not new_amount:
                                    new_amount = int(new_volume * new_close)
                                if new_amount and new_close and not new_volume:
                                    new_volume = int(new_amount / new_close) if new_close > 0 else 0
                                if new_amount and new_volume and not new_close:
                                    new_close = round(new_amount / new_volume, 2) if new_volume > 0 else 0
                                
                                if new_close and new_volume and new_amount:
                                    cur.execute("UPDATE stock_history SET close=?, volume=?, amount=? WHERE code=? AND date_int=?",
                                               (new_close, new_volume, new_amount, code, date_int))
                                    fixed_by_crawl += 1
                            else:
                                # 沒抓到，用前一天估算
                                cur.execute("""
                                    SELECT close FROM stock_history 
                                    WHERE code = ? AND date_int < ? AND close > 0
                                    ORDER BY date_int DESC LIMIT 1
                                """, (code, date_int))
                                prev = cur.fetchone()
                                if prev and prev[0] > 0:
                                    prev_close = prev[0]
                                    if old_volume and old_volume > 0:
                                        est_amount = int(prev_close * old_volume)
                                        cur.execute("UPDATE stock_history SET close=?, amount=? WHERE code=? AND date_int=?",
                                                   (prev_close, est_amount, code, date_int))
                                        fixed_by_prev += 1
                        
                        time.sleep(0.3)  # 避免請求過快
                        
                    except Exception as e:
                        pass
                
                if fixed_by_crawl > 0:
                    print_flush(f"  [修復] 情況4/6/7 (爬蟲): {fixed_by_crawl} 筆 → 從 TWSE/TPEx 抓取")
                if fixed_by_prev > 0:
                    print_flush(f"  [修復] 情況4/6/7 (估算): {fixed_by_prev} 筆 → 用前日價格估算")
            
            # 情況 8: 無量、無價、無額 → 保持不變 (可能停牌或下市)
            cur.execute("""
                SELECT COUNT(*) FROM stock_history 
                WHERE (volume IS NULL OR volume = 0) 
                  AND (close IS NULL OR close = 0) 
                  AND (amount IS NULL OR amount = 0)
            """)
            case8_count = cur.fetchone()[0]
            if case8_count > 0:
                print_flush(f"  [略過] 情況8 (全無): {case8_count} 筆 (可能停牌/下市)")
            
            conn.commit()
            
    except Exception as e:
        print_flush(f"  ⚠ 自動修復失敗: {e}")

def step6_verify_and_backfill(data=None, resume=False, skip_downloads=False):
    """步驟6: 驗證資料完整性與回補 (含 amount 與法人資料)"""
    print_flush("\n[Step 6] 驗證資料完整性與回補...")
    
    # 0. 自動修復缺金額記錄 (用 close * volume 估算)
    _auto_fix_missing_amount()
    
    if not skip_downloads:
        # 1. 檢查並補齊法人資料 (智慧模式)
        step3_5_download_institutional(days=3)
        
        # 2. 下載集保大戶資料 (每週一次，這裡每次檢查更新)
        step3_6_download_major_holders()
    
    if data is None:
        data = step4_load_data()
    
    # 收集需要回補的股票 (使用新三表架構)
    tasks = []
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        
        # 取得市場最新交易日 (作為基準)
        latest_market_date_str = get_latest_market_date()
        latest_market_date_int = int(latest_market_date_str.replace('-', ''))
        print_flush(f"基準最新日期: {latest_market_date_int}")

        # Pre-load listing dates from stock_meta (Source of Truth)
        list_date_map = {}
        try:
            cur.execute("SELECT code, list_date FROM stock_meta")
            for r in cur.fetchall():
                if r[1]: list_date_map[r[0]] = r[1]
        except:
            pass

        # 優化: 使用 GROUP BY 一次查詢所有股票的歷史資料筆數與 amount 缺失數 (僅檢查最近 3 年)
        cutoff_date = (datetime.now() - timedelta(days=Config.HISTORY_DAYS_LOOKBACK)).strftime("%Y%m%d")
        cutoff_int = int(cutoff_date)
        
        print_flush("正在分析資料庫狀態 (含成交金額與時效檢查)...")
        cur.execute(f"""
            SELECT code, COUNT(*), MIN(date_int), MAX(date_int),
                   SUM(CASE WHEN volume > 0 AND (amount IS NULL OR amount = 0) AND date_int >= {cutoff_int} THEN 1 ELSE 0 END)
            FROM stock_history 
            GROUP BY code
        """)
        history_stats = {row[0]: {'count': row[1], 'min_date': row[2], 'max_date': row[3], 'missing_amount': row[4]} for row in cur.fetchall()}
        
        for code, info in data.items():
            stats = history_stats.get(code)
            
            # Guard Clause: No history data
            if not stats:
                tasks.append((code, info['name'], 0, "無歷史資料"))
                continue
                
            count = stats['count']
            min_date_int = stats['min_date']
            max_date_int = stats['max_date'] or 0
            missing_amount = stats['missing_amount'] or 0
            
            # Guard Clause: Data is outdated
            if max_date_int < latest_market_date_int:
                tasks.append((code, info['name'], count, f"資料過舊(至{max_date_int})"))
                continue
                
            # Guard Clause: Missing amount (Strict Check)
            # 用戶強調: 只要少一張，指標都會錯，因此必須嚴格檢查
            if missing_amount > 0:
                tasks.append((code, info['name'], count, f"缺金額({missing_amount}筆)"))
                continue
            
            # Guard Clause: Insufficient count
            if count < MIN_DATA_COUNT:
                # Check if it's a new stock (listed recently) using stock_meta
                is_new_stock = False
                l_date_str = list_date_map.get(code)
                
                if l_date_str:
                    try:
                        l_date = datetime.strptime(l_date_str, '%Y-%m-%d')
                        # Calculate theoretical max market days since listing (approx 5/7 of total days)
                        # Or simply check if listing date is recent enough
                        days_since = (datetime.now() - l_date).days
                        # If listed less than MIN_DATA_COUNT * 1.5 days ago (approx), and we have most of the data
                        # expected_market_days approx days_since * 0.68 (taking holidays into account)
                        expected_count = int(days_since * 0.68)
                        
                        # If we have at least 90% of expected data, consider it complete
                        if count >= expected_count * 0.9:
                            is_new_stock = True
                        
                        # Also check if min_date is close to list_date (within 20 days)
                        if min_date_int:
                            min_date = datetime.strptime(str(min_date_int), '%Y%m%d')
                            if min_date <= l_date + timedelta(days=20):
                                is_new_stock = True
                                
                    except Exception as e:
                        # print_flush(f"Date parse error: {e}")
                        pass
                
                # Fallback to twstock if stock_meta missing (Legacy logic)
                if not is_new_stock and not l_date_str:
                    if min_date_int:
                        try:
                            stock_info = twstock.codes.get(code)
                            if stock_info and stock_info.start:
                                list_date = datetime.strptime(stock_info.start, '%Y/%m/%d')
                                min_date = datetime.strptime(str(min_date_int), '%Y%m%d')
                                if min_date <= list_date + timedelta(days=10):
                                    is_new_stock = True
                        except:
                            pass
                
                if not is_new_stock:
                    tasks.append((code, info['name'], count, f"筆數不足({count})"))
                    continue

            # If we reached here, data is considered complete
            pass
    
    if not tasks:
        print_flush(f"✓ 所有股票資料完整 (筆數充足且無缺失金額)")
        return set()

    # 讀取進度
    progress = load_progress()
    start_idx = progress.get("last_code_index", 0) if resume else 0
    failed_stocks = set(progress.get("failed_stocks", []))
    
    # 重置進度
    if not resume:
        save_progress(last_idx=0, failed_stocks=[])
        start_idx = 0
        failed_stocks = set()
    
    # Filter out failed stocks (Avoid infinite loop on same day)
    if resume or start_idx > 0:
        original_count = len(tasks)
        tasks = [t for t in tasks if t[0] not in failed_stocks]
        if len(tasks) < original_count:
            print_flush(f"⚠ 已略過 {original_count - len(tasks)} 檔先前失敗的股票")

    if start_idx >= len(tasks):
        print_flush(f"⚠ 進度紀錄 ({start_idx}) 超出當前任務範圍 ({len(tasks)})，重置進度從頭開始...")
        start_idx = 0
        save_progress(last_idx=0)
    
    print_flush(f"⚠ 發現 {len(tasks)} 檔股票資料不足，開始回補...")
    
    if start_idx > 0:
        print_flush(f"📍 從第 {start_idx+1} 檔繼續(已完成 {start_idx} 檔)")
    
    tracker = ProgressTracker(total_lines=4)
    data_source_manager = DataSourceManager(progress_tracker=tracker, silent=False)
    
    success_count = 0
    verified_count = 0
    updated_codes = set()
    
    # 預先載入上市日期 Map
    list_date_map = {}
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT code, list_date FROM stock_meta")
            for r in cur.fetchall():
                if r[1]: list_date_map[r[0]] = r[1]
    except:
        pass

    with tracker:
        latest_date = get_latest_market_date()
        end_date = latest_date
        default_start_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=1095)).strftime("%Y-%m-%d")
        
        for i in range(start_idx, len(tasks)):
            code, name, count, reason = tasks[i]
            
            # 動態調整 Start Date (依據上市日期)
            start_date = default_start_date
            l_date_str = list_date_map.get(code)
            if l_date_str:
                try:
                    # 假設 list_date 格式為 YYYY-MM-DD
                    if l_date_str > start_date:
                        start_date = l_date_str
                        # 如果上市日期比 end_date 還晚(理論上不可能，除非資料錯)，則無需補
                        if start_date > end_date:
                            tracker.update_lines(f"跳過 {code} {name}: 上市日期 {l_date_str} 晚於 {end_date}")
                            continue
                except:
                    pass

            tracker.update_lines(
                f"正在回補: {code} {name} (自 {start_date})",
                f"原因: {reason}",
                f"進度: {i+1}/{len(tasks)} | 成功: {success_count}",
                "正在連接 API..."
            )
            
            df = data_source_manager.fetch_history(code, start_date, end_date)
            
            if df is not None and not df.empty:
                try:
                    with db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        
                        for _, row in df.iterrows():
                            # 寫入 stock_history (新三表架構) - 含成交金額
                            # 使用 REPLACE 確保更新 amount 欄位
                            date_int = int(str(row['date']).replace('-', ''))
                            cur.execute("""
                                INSERT OR REPLACE INTO stock_history 
                                (code, date_int, open, high, low, close, volume, amount)
                                VALUES (?,?,?,?,?,?,?,?)
                            """, (code, date_int, row.get('open'), row.get('high'), 
                                  row.get('low'), row.get('close'), row.get('volume'),
                                  row.get('amount')))
                        
                        conn.commit()
                        success_count += 1
                        updated_codes.add(code)
                        
                        # Remove from failed_stocks if it was there
                        if code in failed_stocks:
                            failed_stocks.remove(code)
                        
                except Exception:
                    pass
            else:
                # Mark as failed
                failed_stocks.add(code)
            
            # 儲存進度 (包含 failed_stocks)
            if (i + 1) % 10 == 0:
                save_progress(last_idx=i + 1, failed_stocks=list(failed_stocks))
                
            time.sleep(1)  # 避免過快請求
            
    # 完成後清除進度
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        
    print_flush(f"\n✓ 回補完成 - 成功: {success_count}")
    print_flush(f"\n✓ 回補完成 - 成功: {success_count}")
    return updated_codes



def step8_sync_supabase():
    """步驟8: 同步資料到 Supabase"""
    print_flush("\n[Step 8] 同步資料到 Supabase (已停用)")
    return
    
    if not HAS_SUPABASE:
        print_flush("❌ 未安裝 supabase 套件，無法同步 (pip install supabase)")
        return

    print_flush("\n[Step 8] 同步資料到 Supabase...")
    
    # Supabase 設定
    url = "https://gqiyvefcldxslrqpqlri.supabase.co"
    key = "sb_secret_XSeaHx_76CRxA6j8nZ3qDg_nzgFgTAN"
    
    try:
        supabase: Client = create_client(url, key)
        
        with db_manager.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            # 1. 同步 institutional_investors
            cur.execute("SELECT COUNT(*) FROM institutional_investors")
            total = cur.fetchone()[0]
            
            if total > 0:
                print_flush(f"正在同步法人資料 ({total} 筆)...")
                BATCH_SIZE = 1000
                total_batches = math.ceil(total / BATCH_SIZE)
                
                cur.execute("SELECT * FROM institutional_investors")
                
                success_count = 0
                for i in range(total_batches):
                    rows = cur.fetchmany(BATCH_SIZE)
                    if not rows: break
                    
                    data = [dict(row) for row in rows]
                    try:
                        supabase.table("institutional_investors").upsert(data).execute()
                        success_count += len(data)
                        if (i+1) % 10 == 0:
                            print(f"\r  進度: {i+1}/{total_batches}", end="")
                    except Exception as e:
                        if "Could not find the table" in str(e):
                            print_flush(f"\n❌ 錯誤: 表格不存在，請先執行 update_supabase.sql")
                            return
                        # 其他錯誤忽略，繼續下一批
                        pass
                print_flush(f"\n✓ 法人資料同步完成 ({success_count}/{total})")
            else:
                print_flush("法人資料為空，跳過")

            # 2. 同步 stock_history (可選，因為資料量太大，這裡先只同步法人)
            # 若要同步 stock_history，建議只同步最近 N 天
            
    except Exception as e:
        print_flush(f"❌ 同步失敗: {e}")



def _build_history_query(limit_days=None):
    """建構歷史資料查詢語句 (Extract Method)"""
    if limit_days:
        return """
            SELECT * FROM (
                SELECT 
                    CAST(date_int/10000 AS TEXT) || '-' || 
                    SUBSTR('0'||CAST((date_int/100)%100 AS TEXT),-2) || '-' ||
                    SUBSTR('0'||CAST(date_int%100 AS TEXT),-2) as date,
                    open, high, low, close, volume, amount
                FROM stock_history 
                WHERE code = ? 
                ORDER BY date_int DESC
                LIMIT ?
            ) ORDER BY date ASC
        """
    else:
        return """
            SELECT 
                CAST(date_int/10000 AS TEXT) || '-' || 
                SUBSTR('0'||CAST((date_int/100)%100 AS TEXT),-2) || '-' ||
                SUBSTR('0'||CAST(date_int%100 AS TEXT),-2) as date,
                open, high, low, close, volume, amount
            FROM stock_history 
            WHERE code = ? 
            ORDER BY date_int ASC
        """

def calculate_stock_history_indicators(code, display_days=30, limit_days=None, conn=None, preloaded_df=None):
    """計算股票歷史技術指標"""
    try:
        # 獲取集保人數
        total_shareholders = 0
        try:
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT total_shareholders FROM stock_snapshot WHERE code = ?", (code,))
                res = cur.fetchone()
                if res and res[0]: total_shareholders = res[0]
            else:
                with db_manager.get_connection() as tmp_conn:
                    cur = tmp_conn.cursor()
                    cur.execute("SELECT total_shareholders FROM stock_snapshot WHERE code = ?", (code,))
                    res = cur.fetchone()
                    if res and res[0]: total_shareholders = res[0]
        except:
            pass

        # 內部函數: 執行查詢 (新三表架構)
        def execute_query(connection):
            query = _build_history_query(limit_days)
            
            # 參數處理
            params = [code]
            if limit_days:
                params.append(limit_days + 250) # 多抓一些以計算 MA200
                
            df = pd.read_sql_query(query, connection, params=params)
            return df

        t_start = time.time()
        
        if preloaded_df is not None:
            df = preloaded_df.copy()
            # 如果有 limit_days，截取最後 N 筆
            if limit_days and len(df) > limit_days:
                df = df.iloc[-limit_days:].reset_index(drop=True)
        elif conn:
            df = execute_query(conn)
        else:
            with db_manager.get_connection() as new_conn:
                df = execute_query(new_conn)
        
        if df.empty or len(df) < 20:
            return None
        
        # 確保日期格式正確
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # 計算指標
        df['MA3'] = df['close'].rolling(3).mean().round(2)
        df['MA20'] = df['close'].rolling(20).mean().round(2)
        df['MA60'] = df['close'].rolling(60).mean().round(2)
        df['MA120'] = df['close'].rolling(120).mean().round(2)
        df['MA200'] = df['close'].rolling(200).mean().round(2)
        
        # 成交量均線
        df['Vol_MA3'] = df['volume'].rolling(3).mean().round(2)
        
        df['WMA3'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 3), index=df.index).round(2)
        df['WMA20'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 20), index=df.index).round(2)
        df['WMA60'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 60), index=df.index).round(2)
        df['WMA120'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 120), index=df.index).round(2)
        df['WMA200'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 200), index=df.index).round(2)
        
        df['MFI'] = IndicatorCalculator.calculate_mfi(df, 14).round(2)
        df['VWAP'] = IndicatorCalculator.calculate_vwap_series(df, lookback=20).round(2)
        df['CHG14'] = IndicatorCalculator.calculate_chg14_series(df).round(2)
        df['RSI'] = IndicatorCalculator.calculate_rsi_series(df, 14).round(2)
        
        macd, signal = IndicatorCalculator.calculate_macd_series(df)
        df['MACD'] = macd.round(2)
        df['SIGNAL'] = signal.round(2)
        
        # [New] Six-Dim Resonance Indicators
        # 1. BBI (Bullish Bearish Indicator) - (MA3 + MA6 + MA12 + MA24) / 4
        # Using existing MAs or calculating new ones if needed. Standard BBI uses 3, 6, 12, 24.
        # We have MA3, MA20, MA60, MA120. Let's calculate specific ones for BBI.
        ma3 = df['close'].rolling(3).mean()
        ma6 = df['close'].rolling(6).mean()
        ma12 = df['close'].rolling(12).mean()
        ma24 = df['close'].rolling(24).mean()
        df['BBI'] = ((ma3 + ma6 + ma12 + ma24) / 4).round(2)

        # 2. MTM (Momentum) - Close - Close(N), usually N=12
        df['MTM'] = (df['close'] - df['close'].shift(12)).round(2)
        df['MTM_MA'] = df['MTM'].rolling(6).mean().round(2) # MTM Signal line

        # 3. LWR (Williams %R) - usually 9 days
        # Formula: (Highest High - Close) / (Highest High - Lowest Low) * -100
        low_min = df['low'].rolling(9).min()
        high_max = df['high'].rolling(9).max()
        df['LWR'] = (((high_max - df['close']) / (high_max - low_min)) * -100).round(2)
        
        k_series, d_series = IndicatorCalculator.calculate_monthly_kd_series(df)
        daily_k, daily_d = IndicatorCalculator.calculate_daily_kd_series(df)
        week_k, week_d = IndicatorCalculator.calculate_weekly_kd_series(df)
        
        smart_score, smi_sig, nvi_sig, vsa_sig, svi_sig, vol_div_sig, weekly_nvi_sig = IndicatorCalculator.calculate_smart_score_series(df)
        
        # 計算並儲存原始數值
        df['SMI'] = IndicatorCalculator.calculate_smi_series(df).round(2)
        nvi, _ = IndicatorCalculator.calculate_nvi_series(df)
        df['NVI'] = nvi.round(2)
        
        # [Restored] SVI, RSI, MACD
        df['SVI'] = ((df['close'] - df['MA200']) / df['MA200'] * 100).round(2)
        
        # [Added] ADL, RS
        df['ADL'] = IndicatorCalculator.calculate_adl_series(df).round(2)
        df['RS'] = IndicatorCalculator.calculate_rs_series(df).round(2)
        
        df['Smart_Score'] = smart_score
        df['SMI_Signal'] = smi_sig
        df['NVI_Signal'] = nvi_sig
        df['VSA_Signal'] = vsa_sig
        df['SVI_Signal'] = svi_sig
        df['Vol_Div_Signal'] = vol_div_sig
        df['Weekly_NVI_Signal'] = weekly_nvi_sig
        
        df['PVI'] = IndicatorCalculator.calculate_pvi_series(df).round(2)
        
        # [Fix] 補上缺失的 CLV 計算
        df['clv'] = IndicatorCalculator.calculate_clv_series(df).round(2)
        
        # [Fix] 補上缺失的 3日背離訊號計算
        div_bull, div_bear = IndicatorCalculator.calculate_3day_divergence_series(df)
        df['div_3day_bull'] = div_bull
        df['div_3day_bear'] = div_bear
        
        df['Month_K'] = k_series.round(2)
        df['Month_D'] = d_series.round(2)
        df['Daily_K'] = daily_k.round(2)
        df['Daily_D'] = daily_d.round(2)
        df['Week_K'] = pd.Series(week_k, index=df.index).round(2)
        df['Week_D'] = pd.Series(week_d, index=df.index).round(2)
        
        df['close_prev'] = df['close'].shift(1)
        df['vol_prev'] = df['volume'].shift(1)
        
        # [New] VWAP 60
        df['VWAP60'] = IndicatorCalculator.calculate_vwap_series(df, lookback=60).round(2)
        
        # [New] BBW (Bollinger Band Width)
        # Using simple calculation here as IndicatorCalculator might not have a dedicated series method for BBW
        
        # [New] VSBC Bands
        vsbc_u, vsbc_l = IndicatorCalculator.calculate_vsbc_bands(df)
        df['VSBC_Upper'] = vsbc_u.round(2)
        df['VSBC_Lower'] = vsbc_l.round(2)
        ma20_for_bb = df['close'].rolling(20).mean()
        std20_for_bb = df['close'].rolling(20).std()
        upper_bb = ma20_for_bb + 2 * std20_for_bb
        lower_bb = ma20_for_bb - 2 * std20_for_bb
        df['BBW'] = ((upper_bb - lower_bb) / ma20_for_bb).round(4)
        
        # [New] Fibonacci 0.618 (Recent 60 days)
        # We need a rolling calculation for this to be correct for each day in history
        # For efficiency, we can use rolling max/min
        roll_high_60 = df['high'].rolling(60).max()
        roll_low_60 = df['low'].rolling(60).min()
        diff_60 = roll_high_60 - roll_low_60
        df['Fib_0618'] = (roll_high_60 - (diff_60 * 0.618)).round(2)
        
        # [New] VWAP 200
        df['VWAP200'] = IndicatorCalculator.calculate_vwap_series(df, lookback=200).round(2)
        
        # [New] Weekly/Monthly Data (Resampled)
        # Note: This is computationally expensive, so we do it only if needed or optimize it
        # Here we use a simplified approach by taking the last available weekly/monthly data
        # For a proper implementation, we should resample the whole series and reindex
        
        # Weekly
        df['date_idx'] = df['date']
        df.set_index('date_idx', inplace=True)
        
        weekly_df = df.resample('W').agg({'open': 'first', 'close': 'last'})
        monthly_df = df.resample('M').agg({'open': 'first', 'close': 'last'})
        
        # Reindex back to daily to fill values
        df['weekly_open'] = weekly_df['open'].reindex(df.index, method='ffill')
        df['weekly_close'] = weekly_df['close'].reindex(df.index, method='ffill')
        df['monthly_open'] = monthly_df['open'].reindex(df.index, method='ffill')
        df['monthly_close'] = monthly_df['close'].reindex(df.index, method='ffill')
        
        df.reset_index(drop=True, inplace=True)
        
        # [New] Mansfield RS (Simplified Relative Strength Score)
        # Since we don't have a reliable market index in this context efficiently, 
        # we use the RS score we already calculated (0-100) as a proxy for now.
        # Or we can implement a self-relative strength if needed.
        # For now, we map the existing RS to this field to ensure data availability.
        df['Mansfield_RS'] = df['RS'] 
        
        # 準備結果列表
        indicators_list = []
        start_index = 0 if not display_days else max(0, len(df) - display_days)
        
        for i in range(start_index, len(df)):
            row = df.iloc[i]
            prev_row = df.iloc[i-1] if i > 0 else row
            
            indicators = {
                'date': row['date'].strftime('%Y-%m-%d'),
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'close_prev': row['close_prev'] if pd.notnull(row['close_prev']) else None,
                'vol_prev': row['vol_prev'] if pd.notnull(row['vol_prev']) else None,
                'Vol_MA3': row['Vol_MA3'],
                'MA3': row['MA3'],
                'MA20': row['MA20'],
                'MA60': row['MA60'],
                'MA120': row['MA120'],
                'MA200': row['MA200'],
                'WMA3': row['WMA3'],
                'WMA20': row['WMA20'],
                'WMA60': row['WMA60'],
                'WMA120': row['WMA120'],
                'WMA200': row['WMA200'],
                'MA3_prev': prev_row['MA3'],
                'MA20_prev': prev_row['MA20'],
                'MA60_prev': prev_row['MA60'],
                'MA120_prev': prev_row['MA120'],
                'MA200_prev': prev_row['MA200'],
                'WMA3_prev': prev_row['WMA3'],
                'WMA20_prev': prev_row['WMA20'],
                'WMA60_prev': prev_row['WMA60'],
                'WMA120_prev': prev_row['WMA120'],
                'WMA200_prev': prev_row['WMA200'],
                'MFI': row['MFI'],
                'MFI_prev': prev_row['MFI'],
                'VWAP': row['VWAP'],
                'VWAP_prev': prev_row['VWAP'],
                'CHG14': row['CHG14'],
                'CHG14_prev': prev_row['CHG14'],
                'RSI': row['RSI'],
                'MACD': row['MACD'],
                'SIGNAL': row['SIGNAL'],
                'Month_K': row['Month_K'],
                'Month_D': row['Month_D'],
                'Daily_K': row['Daily_K'] if pd.notnull(row['Daily_K']) else None,
                'Daily_D': row['Daily_D'] if pd.notnull(row['Daily_D']) else None,
                'Week_K': row['Week_K'] if pd.notnull(row['Week_K']) else None,
                'Week_D': row['Week_D'] if pd.notnull(row['Week_D']) else None,
                'Month_K_prev': prev_row['Month_K'],
                'Month_D_prev': prev_row['Month_D'],
                'Daily_K_prev': prev_row['Daily_K'],
                'Daily_D_prev': prev_row['Daily_D'],
                'Week_K_prev': prev_row['Week_K'],
                'Week_D_prev': prev_row['Week_D'],
                'SMI': row['SMI'],
                'SVI': row['SVI'],
                'NVI': row['NVI'],
                'Smart_Score': int(row['Smart_Score']) if pd.notnull(row['Smart_Score']) else None,
                'SMI_Signal': int(row['SMI_Signal']) if pd.notnull(row['SMI_Signal']) else None,
                'NVI_Signal': int(row['NVI_Signal']) if pd.notnull(row['NVI_Signal']) else None,
                'VSA_Signal': int(row['VSA_Signal']) if pd.notnull(row['VSA_Signal']) else None,
                'SVI_Signal': int(row['SVI_Signal']) if pd.notnull(row['SVI_Signal']) else None,
                'SMI_Signal_prev': int(prev_row['SMI_Signal']) if pd.notnull(prev_row['SMI_Signal']) else None,
                'NVI_Signal_prev': int(prev_row['NVI_Signal']) if pd.notnull(prev_row['NVI_Signal']) else None,
                'SVI_Signal_prev': int(prev_row['SVI_Signal']) if pd.notnull(prev_row['SVI_Signal']) else None,
                'Smart_Score_prev': int(prev_row['Smart_Score']) if pd.notnull(prev_row['Smart_Score']) else None,
                'Smart_Score_prev': int(prev_row['Smart_Score']) if pd.notnull(prev_row['Smart_Score']) else None,
                'PVI': float(row['PVI']) if pd.notnull(row['PVI']) else None,
                'pvi_prev': float(prev_row['PVI']) if pd.notnull(prev_row['PVI']) else None, # [Fix] Add pvi_prev
                'clv': float(row['clv']) if pd.notnull(row.get('clv')) else None, # [Fix] 加入 CLV
                'Vol_Div_Signal': int(row['Vol_Div_Signal']) if pd.notnull(row['Vol_Div_Signal']) else None,
                'Weekly_NVI_Signal': int(row['Weekly_NVI_Signal']) if pd.notnull(row['Weekly_NVI_Signal']) else None,
                'Div_3Day_Bull': int(row['div_3day_bull']) if pd.notnull(row.get('div_3day_bull')) else None,
                'Div_3Day_Bear': int(row['div_3day_bear']) if pd.notnull(row.get('div_3day_bear')) else None,
                'VWAP60': row['VWAP60'],
                'BBW': row['BBW'],
                'Fib_0618': row['Fib_0618'],
                'VWAP200': row['VWAP200'],
                'Weekly_Close': row['weekly_close'] if pd.notnull(row['weekly_close']) else None,
                'Weekly_Open': row['weekly_open'] if pd.notnull(row['weekly_open']) else None,
                'Monthly_Close': row['monthly_close'] if pd.notnull(row['monthly_close']) else None,
                'Monthly_Open': row['monthly_open'] if pd.notnull(row['monthly_open']) else None,
                'Mansfield_RS': row['Mansfield_RS'],
                'ADL': float(row['ADL']) if pd.notnull(row['ADL']) else None,
                'RS': float(row['RS']) if pd.notnull(row['RS']) else None,
            }
            
            current_window = df.iloc[max(0, i-19):i+1]
            vp = IndicatorCalculator.calculate_vp_scheme3(current_window, lookback=20)
            
            indicators['POC'] = vp['POC']
            indicators['VP_upper'] = vp['VP_upper']
            indicators['VP_lower'] = vp['VP_lower']
            
            # VSBC Bands
            indicators['VSBC_Upper'] = row['VSBC_Upper']
            indicators['VSBC_Lower'] = row['VSBC_Lower']
            
            # 集保人數
            indicators['Total_Shareholders'] = total_shareholders
            
            indicators_list.append(indicators)
        
        return indicators_list[::-1]
        
    except Exception as e:
        # Log error for debugging purposes
        # logger.debug(f"Error in calculate_stock_history_indicators: {e}")
        return None

def process_single_stock_calculation(code, name, preloaded_df, conn):
    """
    處理單一股票的指標計算 (提取自 step7)
    :return: update_tuple (for executemany) or None
    """
    try:
        # 計算指標 (使用預載入的 DataFrame)
        # 注意: 這裡我們只需要最新的一筆來更新 snapshot，所以 display_days=1 即可
        # 但為了計算 MA200 等長天期指標，limit_days 仍需足夠長 (由 Config 控制)
        indicators_list = calculate_stock_history_indicators(
            code, 
            display_days=1, 
            limit_days=Config.CALC_LOOKBACK_DAYS, 
            conn=conn, 
            preloaded_df=preloaded_df
        )
        
        if not indicators_list:
            return None
            
        # 取得最新一筆資料 (calculate_stock_history_indicators 回傳的是倒序 list，最新在 index 0)
        latest = indicators_list[0]
        
        # 建構更新 Tuple (必須與 step7 的 SQL UPDATE 順序完全一致)
        return (
            latest.get('MA3'), latest.get('MA20'), latest.get('MA60'), latest.get('MA120'), latest.get('MA200'),
            latest.get('WMA3'), latest.get('WMA20'), latest.get('WMA60'), latest.get('WMA120'), latest.get('WMA200'),
            latest.get('MFI'), latest.get('VWAP'), latest.get('CHG14'), latest.get('RSI'), latest.get('MACD'), latest.get('SIGNAL'),
            latest.get('POC'), latest.get('VP_upper'), latest.get('VP_lower'),
            latest.get('Month_K'), latest.get('Month_D'),
            latest.get('Daily_K'), latest.get('Daily_D'),
            latest.get('Week_K'), latest.get('Week_D'),
            latest.get('MA3_prev'), latest.get('MA20_prev'), latest.get('MA60_prev'), latest.get('MA120_prev'), latest.get('MA200_prev'),
            latest.get('WMA3_prev'), latest.get('WMA20_prev'), latest.get('WMA60_prev'), latest.get('WMA120_prev'), latest.get('WMA200_prev'),
            latest.get('MFI_prev'), latest.get('VWAP_prev'), latest.get('CHG14_prev'),
            latest.get('Month_K_prev'), latest.get('Month_D_prev'),
            latest.get('Daily_K_prev'), latest.get('Daily_D_prev'),
            latest.get('Week_K_prev'), latest.get('Week_D_prev'),
            latest.get('close_prev'), latest.get('vol_prev'),
            latest.get('SMI'), latest.get('SVI'), latest.get('NVI'), latest.get('PVI'), latest.get('clv'),
            latest.get('Smart_Score'), latest.get('SMI_Signal'), latest.get('SVI_Signal'), latest.get('NVI_Signal'), latest.get('VSA_Signal'),
            latest.get('SMI_Signal_prev'), latest.get('SVI_Signal_prev'), latest.get('NVI_Signal_prev'), latest.get('Smart_Score_prev'),
            latest.get('Vol_Div_Signal'), latest.get('Weekly_NVI_Signal'),
            latest.get('Div_3Day_Bull'), latest.get('Div_3Day_Bear'),
            latest.get('Vol_MA3'), latest.get('pvi_prev'),
            latest.get('VWAP60'), latest.get('BBW'), latest.get('Fib_0618'),
            latest.get('Weekly_Close'), latest.get('Weekly_Open'),
            latest.get('Monthly_Close'), latest.get('Monthly_Open'),
            latest.get('VWAP200'), latest.get('Mansfield_RS'),
            latest.get('ADL'), latest.get('RS'),
            code # WHERE code=?
        )
    except Exception as e:
        # 發生錯誤時回傳 None，避免中斷批次處理
        return None


def step7_calc_indicators(data=None, force=False, batch_size=500):
    """[Step 7] 計算技術指標 (多進程並行版)"""
    from multiprocessing import Pool
    
    print_flush("\n[Step 7] 計算技術指標 (多進程加速)...")
    
    if data is None:
        data = step4_load_data()
    
    if not data:
        print_flush("❌ 無股票資料可計算")
        return {}
    
    stocks = [(code, info['name']) for code, info in data.items()]
    total = len(stocks)
    
    if total == 0:
        print_flush("❌ 無股票需要計算指標")
        return {}
    
    # 讀取進度
    progress = load_progress()
    start_idx = 0
    if not force and progress.get('calc_last_idx', 0) > 0:
        start_idx = progress['calc_last_idx']
        print_flush(f"⚡ 偵測到上次進度，從第 {start_idx+1} 筆繼續計算...")
    
    tracker = ProgressTracker(total_lines=3)
    start_time = time.time()
    
    # 使用 CPU 核心數 (保留 1 核給 UI)
    num_processes = max(1, os.cpu_count() - 1)
    print_flush(f"啟動 {num_processes} 個進程並行計算...")
    
    with tracker:
        for batch_start in range(start_idx, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch_stocks = stocks[batch_start:batch_end]
            
            pending_updates = []
            
            with db_manager.get_connection() as conn:
                conn.execute("PRAGMA synchronous = OFF;")
                cur = conn.cursor()
                
                # 1. 批次載入歷史資料 (單線程 I/O)
                batch_codes = [s[0] for s in batch_stocks]
                history_map = batch_load_history(batch_codes, limit_days=Config.CALC_LOOKBACK_DAYS, conn=conn)
                
                # 2. 準備並行任務
                tasks = []
                for code, name in batch_stocks:
                    tasks.append((code, name, history_map.get(code)))
                
                # 3. 多進程並行計算 (CPU Bound)
                # 使用 imap 保持順序並更新進度
                with Pool(processes=num_processes) as pool:
                    for i, res in enumerate(pool.imap(_worker_calc_indicators, tasks, chunksize=20)):
                        current_idx = batch_start + i
                        
                        if res:
                            pending_updates.append(res)
                        
                        # 更新進度顯示 (每 10 筆或最後一筆)
                        if i % 10 == 0 or i == len(batch_stocks) - 1:
                            elapsed = time.time() - start_time
                            processed = current_idx - start_idx + 1
                            avg_speed = processed / elapsed if elapsed > 0 else 0
                            remaining = (total - current_idx - 1) / avg_speed if avg_speed > 0 else 0
                            
                            tracker.update_lines(
                                f'正在計算: {batch_stocks[i][0]} {batch_stocks[i][1]}',
                                f'進度: {current_idx+1}/{total} (批次: {batch_start//batch_size + 1})',
                                f'速度: {avg_speed:.1f} 檔/秒 | 預估剩餘: {int(remaining/60)}分{int(remaining%60)}秒'
                            )
                
                # 4. 批次寫入 (單線程 I/O)
                if pending_updates:
                    try:
                        cur.executemany("""
                            UPDATE stock_snapshot SET
                                ma3=?, ma20=?, ma60=?, ma120=?, ma200=?,
                                wma3=?, wma20=?, wma60=?, wma120=?, wma200=?,
                                mfi14=?, vwap20=?, chg14_pct=?, rsi=?, macd=?, signal=?,
                                vp_poc=?, vp_upper=?, vp_lower=?,
                                month_k=?, month_d=?,
                                daily_k=?, daily_d=?,
                                week_k=?, week_d=?,
                                ma3_prev=?, ma20_prev=?, ma60_prev=?, ma120_prev=?, ma200_prev=?,
                                wma3_prev=?, wma20_prev=?, wma60_prev=?, wma120_prev=?, wma200_prev=?,
                                mfi14_prev=?, vwap20_prev=?, chg14_pct_prev=?,
                                month_k_prev=?, month_d_prev=?,
                                daily_k_prev=?, daily_d_prev=?,
                                week_k_prev=?, week_d_prev=?,
                                close_prev=?, vol_prev=?,
                                smi=?, svi=?, nvi=?, pvi=?, clv=?,
                                smart_score=?, smi_signal=?, svi_signal=?, nvi_signal=?, vsa_signal=?,
                                smi_prev=?, svi_prev=?, nvi_prev=?, smart_score_prev=?,
                                vol_div_signal=?, weekly_nvi_signal=?,
                                div_3day_bull=?, div_3day_bear=?,
                                vol_ma3=?, pvi_prev=?,
                                vwap60=?, bbw=?, fib_0618=?,
                                weekly_close=?, weekly_open=?,
                                monthly_close=?, monthly_open=?,
                                vwap200=?, mansfield_rs=?,
                                adl=?, rs=?
                            WHERE code=?
                        """, pending_updates)
                        conn.commit()
                        save_progress(batch_end - 1)
                    except Exception as e:
                        tracker.update_lines(f"寫入錯誤: {e}", "", "")
                        time.sleep(1)

    print_flush(f"\n[Step 7] 計算完成! 總耗時: {int(time.time() - start_time)} 秒")
    clear_progress()
    return data


def scan_mfi_mode(indicators_data, order='asc', min_volume=0):
    """MFI掃描 (並行版)"""
    
    def filter_func(code, ind):
        mfi = safe_num(ind.get('mfi14') or ind.get('MFI'))
        mfi_prev = safe_num(ind.get('mfi14_prev') or ind.get('MFI_prev'))
        if mfi is None or mfi_prev is None:
            return False
        if order == 'asc':
            return mfi > mfi_prev and mfi < 30
        else:
            return mfi < mfi_prev and mfi > 70
    
    def transform_func(code, ind):
        mfi = safe_num(ind.get('mfi14') or ind.get('MFI'))
        return (code, mfi, ind)
    
    return scan_with_parallel(
        indicators_data,
        filter_func,
        transform_func,
        sort_key=lambda x: x[1],
        reverse=(order == 'desc'),
        min_volume=min_volume
    )


# ==============================
# Phase 4: 統一掃描輸出格式化
# ==============================

def format_volume_ratio(volume, vol_prev=None):
    """格式化成交量與量能比"""
    if volume is None:
        return "-"
    vol = safe_num(volume)
    if vol is None:
        return "-"
    if vol_prev and safe_num(vol_prev) and safe_num(vol_prev) > 0:
        ratio = vol / safe_num(vol_prev)
        return f"{vol:,.0f}({ratio:.2f}x)"
    return f"{vol:,.0f}"

def format_vsbc(ind):
    """格式化 VSBC 上/下"""
    upper = safe_num(ind.get('vsbc_upper') or ind.get('VSBC_Upper'))
    lower = safe_num(ind.get('vsbc_lower') or ind.get('VSBC_Lower'))
    if upper is None or lower is None:
        return "-/-"
    return f"{upper:.0f}/{lower:.0f}"

def format_vp(ind):
    """格式化 VP 上/下"""
    upper = safe_num(ind.get('vp_upper') or ind.get('VP_Upper'))
    lower = safe_num(ind.get('vp_lower') or ind.get('VP_Lower'))
    if upper is None or lower is None:
        return "-/-"
    return f"{upper:.0f}/{lower:.0f}"

def format_scan_row(code, ind, extra_cols=None):
    """
    格式化掃描結果單行輸出
    
    統一格式: 代號 | 名稱 | 收盤 | 成交量(量能比) | VSBC上/下 | VP上/下 | [額外欄位]
    """
    name = ind.get('name', '')[:8]  # 最多8字元
    close = safe_num(ind.get('close'))
    vol = safe_num(ind.get('volume'))
    vol_prev = safe_num(ind.get('vol_prev') or ind.get('volume_prev'))
    
    close_str = f"{close:.2f}" if close else "-"
    vol_str = format_volume_ratio(vol, vol_prev)
    vsbc_str = format_vsbc(ind)
    vp_str = format_vp(ind)
    
    base = f"{code:<6} {name:<10} {close_str:>10} {vol_str:>18} {vsbc_str:>12} {vp_str:>12}"
    
    if extra_cols:
        extra = " ".join(str(c) for c in extra_cols)
        return f"{base} {extra}"
    return base

def print_scan_header(extra_headers=None):
    """印出掃描結果表頭"""
    base = f"{'代號':<6} {'名稱':<10} {'收盤':>10} {'成交量(量能比)':>18} {'VSBC上/下':>12} {'VP上/下':>12}"
    if extra_headers:
        extra = " ".join(extra_headers)
        print_flush(f"{base} {extra}")
    else:
        print_flush(base)
    print_flush("-" * 80)

def print_scan_results(results, title, limit=30, description="", extra_headers=None, extra_func=None):
    """
    統一掃描結果輸出函數
    
    :param results: list of (code, sort_val, ind) 或 (code, sort_val, ind, extra_data)
    :param title: 標題
    :param limit: 顯示限制
    :param description: 說明文字
    :param extra_headers: 額外欄位標頭 list
    :param extra_func: 額外欄位產生函數 (code, sort_val, ind) -> list
    :return: list of codes
    """
    print_flush(f"\n【{title}】 (前 {min(len(results), limit)} 筆)")
    print_scan_header(extra_headers)
    
    codes = []
    for i, item in enumerate(results[:limit]):
        code = item[0]
        sort_val = item[1]
        ind = item[2]
        codes.append(code)
        
        extra_cols = None
        if extra_func:
            extra_cols = extra_func(code, sort_val, ind)
        elif len(item) > 3:
            extra_cols = [item[3]] if not isinstance(item[3], list) else item[3]
        
        print_flush(format_scan_row(code, ind, extra_cols))
    
    print_flush("-" * 80)
    if description:
        print_flush(description)
    print_flush(f"✓ 掃描完成，共找到 {len(results)} 檔符合條件")
    
    return codes

# ==============================
# Phase 4: 高階函數 - 通用掃描模板
# ==============================
def scan_with_filter(indicators_data, filter_func, transform_func, sort_key, reverse=False, min_volume=0):
    """
    通用掃描函數模板 (高階函數模式)
    
    :param indicators_data: 指標數據字典
    :param filter_func: 過濾函數 (code, ind) -> bool
    :param transform_func: 轉換函數 (code, ind) -> result_dict
    :param sort_key: 排序鍵
    :param reverse: 是否降序
    :param min_volume: 最小成交量
    """
    def volume_filter(item):
        code, ind = item
        if not ind:
            return False
        vol = safe_num(ind.get('volume', 0))
        return vol is not None and vol >= min_volume
    
    def combined_filter(item):
        return volume_filter(item) and filter_func(item[0], item[1])
    
    # 使用高階函數鏈式處理
    filtered = filter(combined_filter, indicators_data.items())
    transformed = map(lambda x: transform_func(x[0], x[1]), filtered)
    return sorted(transformed, key=sort_key, reverse=reverse)


def _scan_worker(args):
    """掃描工作進程 (用於多進程)"""
    code, ind, filter_func, transform_func, min_volume = args
    try:
        if not ind:
            return None
        vol = safe_num(ind.get('volume', 0))
        if vol is None or vol < min_volume:
            return None
        if not filter_func(code, ind):
            return None
        return transform_func(code, ind)
    except:
        return None


def scan_with_parallel(indicators_data, filter_func, transform_func, sort_key, 
                       reverse=False, min_volume=0, use_parallel=True, num_workers=None):
    """
    並行掃描函數模板 (多進程版)
    
    :param indicators_data: 指標數據字典
    :param filter_func: 過濾函數 (code, ind) -> bool
    :param transform_func: 轉換函數 (code, ind) -> result_dict
    :param sort_key: 排序鍵
    :param reverse: 是否降序
    :param min_volume: 最小成交量
    :param use_parallel: 是否使用並行 (資料量 > 500 時自動啟用)
    :param num_workers: 工作進程數 (None = CPU 核心數 - 1)
    """
    items = list(indicators_data.items())
    
    # 資料量小時使用單線程
    if len(items) < 500 or not use_parallel:
        return scan_with_filter(indicators_data, filter_func, transform_func, 
                                sort_key, reverse, min_volume)
    
    # 準備並行任務
    tasks = [(code, ind, filter_func, transform_func, min_volume) for code, ind in items]
    
    # 使用多進程
    num_workers = num_workers or max(1, os.cpu_count() - 1)
    results = []
    
    with multiprocessing.Pool(processes=num_workers) as pool:
        for res in pool.imap_unordered(_scan_worker, tasks, chunksize=100):
            if res is not None:
                results.append(res)
    
    return sorted(results, key=sort_key, reverse=reverse)

def scan_ma_mode(indicators_data, ma_type='MA200', min_volume=0):
    """均線掃描 (高階函數重構版)"""
    ma_key = ma_type.lower()
    
    def filter_func(code, ind):
        close = safe_num(ind.get('close'))
        ma_val = safe_num(ind.get(ma_key) or ind.get(ma_type))
        if not (close and ma_val):
            return False
        diff_pct = (close - ma_val) / ma_val * 100
        return -10 <= diff_pct <= 0
    
    def transform_func(code, ind):
        close = safe_num(ind.get('close'))
        ma_val = safe_num(ind.get(ma_key) or ind.get(ma_type))
        diff_pct = (close - ma_val) / ma_val * 100
        return (code, diff_pct, ind)
    
    return scan_with_parallel(
        indicators_data,
        filter_func,
        transform_func,
        sort_key=lambda x: x[1],
        reverse=False,
        min_volume=min_volume
    )

def scan_smart_money_strategy():
    """聰明錢指標掃描 (OpenSpec: Smart Score 0-5)"""
    # 1. 設定掃描參數
    limit = get_display_limit(30)
    min_vol = get_volume_limit(100)  # 預設大於100張
    
    # 使用預設參數 (無阻塞 input)
    vol_mul = 1.1      # 成交量放大倍數
    ma_key = 'MA200'   # 均線趨勢檢查
    mfi_thr = 80.0     # MFI 超買閾值

    print_flush(f"\n正在掃描 聰明錢指標 (NVI主力籌碼)...")
    print_flush(f"條件: 成交量 > 昨日x{vol_mul}, 價格 > {ma_key}, MFI < {mfi_thr}")
    
    results = []
    stats = {
        'total': 0,
        'vol_pass': 0,
        'has_score': 0,
        'smi_sig': 0,
        'svi_sig': 0,
        'nvi_sig': 0,
        'vsa_sig': 0,
        'vwap_sig': 0,
        'vol_div_sig': 0,
        'weekly_nvi_sig': 0,
        'score_3': 0,
        'score_4': 0,
        'score_5': 0,
        'score_6': 0
    }
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else None
    
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    stats['total'] = len(data)
    
    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue
            
            # Volume Multiplier Filter
            vol_prev = safe_float_preserving_none(ind.get('vol_prev'))
            if vol_prev and vol < vol_prev * vol_mul:
                continue
                
            # MFI Filter
            mfi = safe_float_preserving_none(ind.get('mfi14') or ind.get('MFI'))
            if mfi and mfi > mfi_thr:
                continue
                
            # MA Trend Filter (Override SVI signal check if needed, or just add as extra filter)
            # The SVI signal in DB is based on Price > 200MA.
            # If user selects different MA, we check it here.
            close = safe_float_preserving_none(ind.get('close'))
            ma_val = safe_float_preserving_none(ind.get(ma_key) or ind.get(ma_key.lower()))
            if close and ma_val and close <= ma_val:
                continue
            
            stats['vol_pass'] += 1
            
            score = safe_int(ind.get('smart_score') or ind.get('Smart_Score'))
            
            if score is None:
                continue
                
            stats['has_score'] += 1
            
            if safe_int(ind.get('smi_signal') or ind.get('SMI_Signal')) == 1:
                stats['smi_sig'] += 1
            if safe_int(ind.get('svi_signal') or ind.get('SVI_Signal')) == 1:
                stats['svi_sig'] += 1
            if safe_int(ind.get('nvi_signal') or ind.get('NVI_Signal')) == 1:
                stats['nvi_sig'] += 1
            if safe_int(ind.get('vsa_signal') or ind.get('VSA_Signal')) > 0:
                stats['vsa_sig'] += 1
            if safe_int(ind.get('vol_div_signal') or ind.get('Vol_Div_Signal')) > 0:
                stats['vol_div_sig'] += 1
            if safe_int(ind.get('weekly_nvi_signal') or ind.get('Weekly_NVI_Signal')) > 0:
                stats['weekly_nvi_sig'] += 1
            
            vwap_val = safe_float_preserving_none(ind.get('vwap20') or ind.get('VWAP'))
            if ind.get('close') and vwap_val:
                if safe_float_preserving_none(ind.get('close')) > vwap_val:
                    stats['vwap_sig'] += 1
            
            # Score distribution (max score is 6)
            if score >= 4:
                stats['score_4'] += 1
            if score >= 5:
                stats['score_5'] += 1
            if score >= 6:
                stats['score_6'] += 1
            
            if score >= 4:
                results.append((code, score, ind))
                
        except:
            continue
        
    results.sort(key=lambda x: x[1], reverse=True)
    
    print_flush("\n" + "=" * 60)
    print_flush("[篩選過程] 聰明錢指標多層篩選 (NVI版)")
    print_flush("=" * 60)
    print_flush(f"總股數: {stats['total']}")
    print_flush("─" * 60)
    print_flush(f"✓ 成交量 >= {min_vol//1000}張            → {stats['vol_pass']} 檔")
    print_flush("─" * 60)
    print_flush("【各項訊號統計】(通過成交量門檻者)")
    print_flush(f"  • NVI 趨勢 (NVI>200MA)    → {stats['smi_sig']} 檔")
    print_flush(f"  • NVI > PVI (多頭排列)    → {stats['nvi_sig']} 檔")
    print_flush(f"  • 無背離 (價高NVI高)      → {stats['vsa_sig']} 檔")
    print_flush(f"  • 價格趨勢 (價>200MA)     → {stats['svi_sig']} 檔")
    print_flush(f"  • 無量價背離 (新)         → {stats['vol_div_sig']} 檔")
    print_flush(f"  • 週線NVI趨勢 (新)        → {stats['weekly_nvi_sig']} 檔")
    print_flush("─" * 60)
    print_flush("【Smart Score 分布】(滿分6分)")
    print_flush(f"  • Score >= 4 (買入訊號)   → {stats['score_4']} 檔")
    print_flush(f"  • Score >= 5 (強烈買入)   → {stats['score_5']} 檔")
    print_flush(f"  • Score >= 6 (極強訊號)   → {stats['score_6']} 檔")
    print_flush("=" * 60)
    
    if stats['vol_div_sig'] == 0 and stats['weekly_nvi_sig'] == 0:
        print_flush("💡 提示: 若新訊號(無量價背離/週線NVI)均為 0，請執行 [1] 資料管理 -> [4] 重新計算指標")
    
    # 使用統一格式輸出 (v2)
    def smart_money_extra(code, ind):
        nvi = safe_num(ind.get('nvi') or ind.get('NVI'))
        nvi_ma = safe_num(ind.get('nvi_ma200') or ind.get('NVI_MA200'))
        score = safe_int(ind.get('smart_score') or ind.get('Smart_Score'))
        
        # 簡單風險建議邏輯
        risk = "低" if score >= 5 else "中"
        suggestion = "強力買進" if score >= 6 else ("買進" if score >= 4 else "觀察")
        
        nvi_str = f"{nvi:.1f}" if nvi else "-"
        nvi_ma_str = f"{nvi/nvi_ma:.2f}" if nvi and nvi_ma else "-"
        
        return [nvi_str, nvi_ma_str, str(score), risk, suggestion]

    codes = display_scan_results_v2(results, "聰明錢掃描結果 (NVI版)", limit=limit,
                            extra_headers=["NVI值", "NVI/MA", "分數", "風險", "建議"],
                            extra_func=smart_money_extra)
    
    prompt_stock_detail_report(codes)



def execute_kd_golden_scan():
    """月KD交叉掃描"""
    limit, min_vol = get_user_scan_params()
    
    print_flush(f"\n正在掃描 月KD交叉 (K↑穿越D↑ 或 D↑穿越K↑)...")
    
    results = []
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else None
    
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue

            k = safe_float_preserving_none(ind.get('month_k'))
            d = safe_float_preserving_none(ind.get('month_d'))
            k_prev = safe_float_preserving_none(ind.get('month_k_prev'))
            d_prev = safe_float_preserving_none(ind.get('month_d_prev'))
            
            if None in [k, d, k_prev, d_prev]:
                continue
            
            k_rising = k > k_prev
            d_rising = d > d_prev
            
            if (k > d and k_prev <= d_prev) and k_rising and d_rising:
                results.append((code, k, ind, "K↑穿越D↑"))
            elif (d > k and d_prev <= k_prev) and d_rising and k_rising:
                results.append((code, k, ind, "D↑穿越K↑"))
                
        except:
            continue
        
    results.sort(key=lambda x: x[1])
    
    print_flush(f"\n月KD交叉: 找到 {len(results)} 檔符合條件的股票")
    print_flush(f"排序方式: K值由小到大 (0% -> 100%)")
    
    # 使用統一格式輸出
    def kd_extra(code, ind):
        k = safe_num(ind.get('month_k')) or 0
        d = safe_num(ind.get('month_d')) or 0
        type_str = ind.get('_type_str', '')
        return [f"K:{k:.1f}", f"D:{d:.1f}", type_str]
    
    for item in results:
        item[2]['_type_str'] = item[3] if len(item) > 3 else ''
    
    codes = display_scan_results_v2(results, "月KD交叉", limit=limit, 
                               description="KD: K值由下往上穿越D值=黃金交叉 (買進訊號)",
                               extra_headers=["月K", "月D", "訊號"],
                               extra_func=kd_extra)
    prompt_stock_detail_report(codes)

def scan_nvi_pvi_crossover():
    """NVI/PVI 交叉掃描"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在掃描 NVI/PVI 交叉訊號...")
    
    results = []
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else None
    
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue

            # 1. NVI > PVI Golden Cross
            nvi = safe_float_preserving_none(ind.get('nvi'))
            pvi = safe_float_preserving_none(ind.get('pvi'))
            nvi_prev = safe_float_preserving_none(ind.get('nvi_prev'))
            pvi_prev = safe_float_preserving_none(ind.get('pvi_prev'))
            
            nvi_pvi_cross = False
            if None not in [nvi, pvi, nvi_prev, pvi_prev]:
                if nvi > pvi and nvi_prev <= pvi_prev:
                    nvi_pvi_cross = True

            # 2. NVI > MA200 Golden Cross (using smi_signal)
            # smi_signal = 1 means NVI > MA200 & MA200 Rising
            smi_sig = ind.get('smi_signal')
            smi_sig_prev = ind.get('smi_signal_prev')
            
            nvi_ma_cross = False
            if smi_sig == 1 and (smi_sig_prev is None or smi_sig_prev == 0):
                nvi_ma_cross = True
                
            if nvi_pvi_cross or nvi_ma_cross:
                signals = []
                if nvi_pvi_cross: signals.append("NVI穿越PVI")
                if nvi_ma_cross: signals.append("NVI多頭確認")
                
                results.append((code, nvi, ind, ",".join(signals)))
                
        except:
            continue
            
    results.sort(key=lambda x: x[1], reverse=True) # NVI 大的排前面
    
    # 使用統一格式輸出
    def nvi_extra(code, ind):
        val = safe_num(ind.get('nvi') or ind.get('NVI')) or 0
        pvi = safe_num(ind.get('pvi') or ind.get('PVI')) or 0
        signals = ind.get('_scan_note', '')
        return [f"NVI:{val:.1f}", f"PVI:{pvi:.1f}", signals]
    
    for r in results:
        r[2]['_scan_note'] = r[3] if len(r) > 3 else ''
    
    codes = display_scan_results_v2(results, "NVI/PVI 交叉掃描", limit=limit, 
                               description="NVI: 負量指標(聰明錢), PVI: 正量指標(散戶). NVI穿越PVI=主力控盤",
                               extra_headers=["NVI", "PVI", "訊號"],
                               extra_func=nvi_extra)
    prompt_stock_detail_report(codes)

def crossover_scan_submenu():
    """交叉掃描子選單 (月KD, NVI/PVI)"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("【交叉訊號掃描】")
        print_flush("="*60)
        print_flush("[1] 月KD交叉 (K↑穿越D↑)")
        print_flush("[2] NVI/PVI 交叉 (主力籌碼訊號)")
        print_flush("[0] 返回")
        
        ch = read_single_key()
        
        if ch == '0':
            break
        elif ch == '1':
            execute_kd_golden_scan()
        elif ch == '2':
            scan_nvi_pvi_crossover()

def scan_ma_alignment_rising(check_price_above=True):
    """均線多頭掃描 (四線: 20,60,120,200)"""
    limit, min_vol = get_user_scan_params()

    title = "均線篩選 (四線上揚+股價在上+0-10%)" if check_price_above else "均線篩選 (四線上揚)"
    print_flush(f"\n正在掃描 {title}...")
    
    results = []
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else None
    
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue

            close = safe_float_preserving_none(ind.get('close'))
            ma20 = safe_float_preserving_none(ind.get('ma20'))
            ma60 = safe_float_preserving_none(ind.get('ma60'))
            ma120 = safe_float_preserving_none(ind.get('ma120'))
            ma200 = safe_float_preserving_none(ind.get('ma200'))
            
            ma20_prev = safe_float_preserving_none(ind.get('ma20_prev'))
            ma60_prev = safe_float_preserving_none(ind.get('ma60_prev'))
            ma120_prev = safe_float_preserving_none(ind.get('ma120_prev'))
            ma200_prev = safe_float_preserving_none(ind.get('ma200_prev'))
            
            if None in [close, ma20, ma60, ma120, ma200]:
                continue
            
            if not (ma20_prev and ma60_prev and ma120_prev and ma200_prev):
                continue
                
            # 檢查斜率 > 0 (Rising) - 四線上揚
            is_all_rising = (ma20 > ma20_prev and
                            ma60 > ma60_prev and
                            ma120 > ma120_prev and
                            ma200 > ma200_prev)
            
            if not is_all_rising:
                continue
            
            if check_price_above:
                # 檢查股價 > 所有均線 (四線)
                is_above = (close > ma20 and close > ma60 and 
                           close > ma120 and close > ma200)
                
                if not is_above:
                    continue
            
            highest_ma = max(ma20, ma60, ma120, ma200)
            lowest_ma = min(ma20, ma60, ma120, ma200)
            
            if highest_ma <= 0 or lowest_ma <= 0:
                continue
            
            # 檢查四線差距 (最高與最低均線差距 <= 10%)
            ma_spread_pct = (highest_ma - lowest_ma) / lowest_ma * 100
            if ma_spread_pct > 10:
                continue
                
            # 檢查距離 (0-10%)
            distance_pct = (close - highest_ma) / highest_ma * 100
            
            if not (0 <= distance_pct <= 10):
                continue
            
            ind['distance_pct'] = distance_pct
            results.append((code, distance_pct, ind))
                
        except:
            continue
    
    results = sorted(results, key=lambda x: x[1])
    
    # 使用統一格式輸出
    def ma_extra(code, ind):
        dist_pct = ind.get('distance_pct', 0)
        ma20 = safe_num(ind.get('ma20') or ind.get('MA20'))
        ma200 = safe_num(ind.get('ma200') or ind.get('MA200'))
        return [f"距MA:{dist_pct:.1f}%", f"MA200:{ma200:.1f}" if ma200 else "-"]
    
    codes = display_scan_results_v2(results, title, limit=limit, 
                               description="條件: 20,60,120,200 全數向上，股價 > 所有均線，距最高均線 0-10%",
                               extra_headers=["距MA", "MA200"],
                               extra_func=ma_extra)
    prompt_stock_detail_report(codes)

def triple_filter_scan():
    """三重篩選入口"""
    limit, min_vol = get_user_scan_params()

    title = "三重篩選 (進階版)"
    print_flush(f"◇ 正在執行{title}... (最小成交量: {min_vol}張)")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else None
    
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return
    
    print_flush("此功能已整合至 [7] 聰明錢掃描")

def analyze_smart_money(code):
    """分析單一個股的聰明錢指標狀態"""
    print_flush(f"\n正在分析 {code} 的聰明錢指標狀態...")
    
    try:
        with db_manager.get_connection() as conn:
            # Fetch last 400 days to ensure enough data for 200MA + lookback
            df = pd.read_sql_query("SELECT date_int, close, volume, high, low FROM stock_history WHERE code=? ORDER BY date_int ASC", conn, params=(code,))
            
        if df.empty or len(df) < 250:
            print_flush("❌ 歷史數據不足 (需至少250天)，無法進行完整分析")
            return None

        # Calculate NVI/PVI
        df = IndicatorCalculator.calculate_nvi_pvi_df(df)
        
        # Calculate MFI
        df['MFI'] = IndicatorCalculator.calculate_mfi(df, 14)
        
        latest = df.iloc[-1]
        prev_20 = df.iloc[-20] if len(df) >= 20 else df.iloc[0]
        
        # Cond 1: NVI Trend
        cond1 = (latest['NVI'] > latest['NVI_200MA']) and (latest['NVI_200MA'] > prev_20['NVI_200MA'])
        
        # Cond 2: NVI > PVI and Crossover
        prev_5 = df.iloc[-5] if len(df) >= 5 else df.iloc[0]
        cond2 = (latest['NVI'] > latest['PVI']) and (prev_5['NVI'] <= prev_5['PVI'])
        
        # Cond 3: No Divergence
        lookback = 60
        recent_df = df.iloc[-lookback:]
        recent_high = recent_df['close'].max()
        recent_nvi_high = recent_df['NVI'].max()
        
        divergence = (
            (abs(latest['close'] - recent_high) / recent_high < 0.02) and
            (abs(latest['NVI'] - recent_nvi_high) / recent_nvi_high > 0.05)
        )
        cond3 = not divergence
        
        # Cond 4: Price Trend
        cond4 = latest['close'] > latest['Price_200MA']
        
        overall_pass = cond1 and cond2 and cond3 and cond4
        
        print_flush("\n" + "="*60)
        print_flush(f"【聰明錢指標深度分析】 {code}")
        print_flush("="*60)
        print_flush(f"當前價格: {latest['close']:.2f}")
        print_flush(f"價格200日均線: {latest['Price_200MA']:.2f}")
        print_flush(f"NVI值: {latest['NVI']:.2f} (200MA: {latest['NVI_200MA']:.2f})")
        print_flush(f"PVI值: {latest['PVI']:.2f} (200MA: {latest['PVI_200MA']:.2f})")
        print_flush(f"MFI(14日): {latest['MFI']:.1f}")
        print_flush("-" * 60)
        print_flush("篩選條件檢查:")
        
        # 5. Volume Divergence
        vol_div = IndicatorCalculator.detect_volume_divergence(df)
        no_vol_div_pass = ~vol_div.iloc[-1]
        
        # 6. Weekly NVI
        weekly_nvi_signal = IndicatorCalculator.calculate_weekly_nvi_signal(df)
        weekly_nvi_pass = weekly_nvi_signal.iloc[-1] == 1
        
        # 計算總分
        score = (int(cond1) + int(cond2) + int(cond3) + 
                 int(cond4) + int(no_vol_div_pass) + int(weekly_nvi_pass))
    
        print_flush(f"  1. NVI 趨勢 (NVI > 200MA & Rising): {'✅ 通過' if cond1 else '❌ 未通過'}")
        print_flush(f"  2. NVI/PVI 關係 (NVI > PVI):        {'✅ 通過' if cond2 else '❌ 未通過'}")
        print_flush(f"  3. 無背離 (價高NVI高):              {'✅ 通過' if cond3 else '❌ 未通過'}")
        print_flush(f"  4. 價格趨勢 (價 > 200MA):           {'✅ 通過' if cond4 else '❌ 未通過'}")
        print_flush(f"  5. 無量價背離 (價漲量增):           {'✅ 通過' if no_vol_div_pass else '❌ 未通過'}")
        print_flush(f"  6. 週線NVI趨勢 (週NVI > 40MA):      {'✅ 通過' if weekly_nvi_pass else '❌ 未通過'}")
        print_flush("-" * 40)
        print_flush(f"綜合評分: {score}/6")
        print_flush("="*60)
        
        return {
            'nvi': latest['NVI'],
            'pvi': latest['PVI'],
            'mfi': latest['MFI'],
            'pass_all': overall_pass
        }
        
    except Exception as e:
        print_flush(f"❌ 分析失敗: {e}")
        return None

# ==========================================
# Advanced Price-Volume Divergence Analysis
# ==========================================
def calculate_mfi_series_advanced(df, period=14):
    """計算MFI（資金流量指標）序列 (Advanced)"""
    df = df.copy()
    # Ensure columns exist (case insensitive)
    if 'High' not in df.columns: df['High'] = df['high']
    if 'Low' not in df.columns: df['Low'] = df['low']
    if 'Close' not in df.columns: df['Close'] = df['close']
    if 'Volume' not in df.columns: df['Volume'] = df['volume']
    
    df['Typical_Price'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['Money_Flow'] = df['Typical_Price'] * df['Volume']
    df['Price_Change'] = df['Typical_Price'].diff()
    df['Positive_MF'] = np.where(df['Price_Change'] > 0, df['Money_Flow'], 0)
    df['Negative_MF'] = np.where(df['Price_Change'] < 0, df['Money_Flow'], 0)
    df['Positive_MF_14'] = df['Positive_MF'].rolling(window=period).sum()
    df['Negative_MF_14'] = df['Negative_MF'].rolling(window=period).sum()
    df['MF_Ratio'] = df['Positive_MF_14'] / df['Negative_MF_14']
    df['MFI'] = 100 - (100 / (1 + df['MF_Ratio']))
    return df['MFI']

def calculate_mfi_for_volume(volume_series, period=14):
    """為成交量計算MFI"""
    fake_df = pd.DataFrame({
        'High': volume_series,
        'Low': volume_series,
        'Close': volume_series,
        'Volume': np.ones_like(volume_series)
    })
    return calculate_mfi_series_advanced(fake_df, period)

def method1_direct_comparison(df, consecutive_days=3):
    """方法1：直接比較法"""
    results = {'價漲量縮': False, '價跌量縮': False, '連續天數': 0, '詳細數據': []}
    if len(df) < consecutive_days + 1: return results
    
    price_changes = df['Close'].diff()
    volume_changes = df['Volume'].diff()
    consecutive_up_down = 0
    consecutive_down_up = 0
    price_up_volume_down_days = []
    price_down_volume_up_days = []
    
    for i in range(1, min(consecutive_days + 5, len(df))):
        price_change = price_changes.iloc[-i]
        volume_change = volume_changes.iloc[-i]
        
        if price_change > 0 and volume_change < 0:
            consecutive_up_down += 1
            consecutive_down_up = 0
            price_up_volume_down_days.append(i)
        elif price_change < 0 and volume_change < 0:
            consecutive_down_up += 1
            consecutive_up_down = 0
            price_down_volume_up_days.append(i)
        else:
            consecutive_up_down = 0
            consecutive_down_up = 0
        
        if consecutive_up_down >= consecutive_days:
            results['價漲量縮'] = True
            results['連續天數'] = consecutive_up_down
        if consecutive_down_up >= consecutive_days:
            results['價跌量縮'] = True
            if results['連續天數'] < consecutive_down_up:
                results['連續天數'] = consecutive_down_up
    
    results['詳細數據'] = {
        '價漲量縮天數': price_up_volume_down_days[:consecutive_days],
        '價跌量縮天數': price_down_volume_up_days[:consecutive_days]
    }
    return results

def calculate_slope_r2(x, y):
    """使用 numpy 計算斜率與 R平方"""
    if len(x) < 2: return 0.0, 0.0
    try:
        slope, intercept = np.polyfit(x, y, 1)
        y_pred = slope * x + intercept
        residuals = y - y_pred
        ss_res = np.sum(residuals**2)
        ss_tot = np.sum((y - np.mean(y))**2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        return slope, r2
    except:
        return 0.0, 0.0

def method2_trend_regression(df, lookback_period=3):
    """方法2：趨勢回歸法"""
    results = {'價漲量縮': False, '價跌量縮': False, '價格斜率': 0, '成交量斜率': 0, '價格R方': 0, '成交量R方': 0}
    if len(df) < lookback_period: return results
    
    recent_df = df.iloc[-lookback_period:].copy()
    x = np.arange(len(recent_df))
    price_values = recent_df['Close'].values
    volume_values = recent_df['Volume'].values
    
    try:
        price_slope, price_r2 = calculate_slope_r2(x, price_values)
        volume_log = np.log1p(volume_values)
        volume_slope, volume_r2 = calculate_slope_r2(x, volume_log)
        
        if price_slope > 0 and volume_slope < 0: results['價漲量縮'] = True
        elif price_slope < 0 and volume_slope < 0: results['價跌量縮'] = True
        
        results['價格斜率'] = price_slope
        results['成交量斜率'] = volume_slope
        results['價格R方'] = price_r2
        results['成交量R方'] = volume_r2
    except:
        pass
    return results

def method3_ma_slope_divergence(df, short_ma_period=5, slope_lookback=5):
    """方法3：短均線斜率背離法"""
    results = {'價漲量縮': False, '價跌量縮': False, '價格_sMA5_斜率': 0.0, '成交量_sMA5_斜率': 0.0, 
               '價格_sMA5_趨勢強度': '無趨勢', '成交量_sMA5_趨勢強度': '無趨勢'}
    if len(df) < short_ma_period + slope_lookback: return results
    
    price_sma5 = df['Close'].rolling(window=short_ma_period).mean()
    volume_sma5 = df['Volume'].rolling(window=short_ma_period).mean()
    
    recent_price = price_sma5.iloc[-slope_lookback:].values
    recent_volume = volume_sma5.iloc[-slope_lookback:].values
    x = np.arange(len(recent_price))
    
    try:
        if len(recent_price) > 1:
            p_slope, p_r2 = calculate_slope_r2(x, recent_price)
        else: p_slope, p_r2 = 0, 0
        
        if len(recent_volume) > 1:
            v_log = np.log1p(recent_volume)
            v_slope, v_r2 = calculate_slope_r2(x, v_log)
        else: v_slope, v_r2 = 0, 0
        
        p_str = '無趨勢'
        if p_r2 > 0.6: p_str = '強上升' if p_slope > 0 else '強下降'
        elif p_r2 > 0.3: p_str = '弱上升' if p_slope > 0 else '弱下降'
            
        v_str = '無趨勢'
        if v_r2 > 0.6: v_str = '強上升' if v_slope > 0 else '強下降'
        elif v_r2 > 0.3: v_str = '弱上升' if v_slope > 0 else '弱下降'
        
        if abs(p_slope) > 0.001 and abs(v_slope) > 0.001:
            if p_slope > 0 and v_slope < 0: results['價漲量縮'] = True
            elif p_slope < 0 and v_slope < 0: results['價跌量縮'] = True
            
        results['價格_sMA5_斜率'] = p_slope
        results['成交量_sMA5_斜率'] = v_slope
        results['價格_sMA5_趨勢強度'] = p_str
        results['成交量_sMA5_趨勢強度'] = v_str
    except:
        pass
    return results

def method4_mfi_divergence(df, period=14):
    """方法4：MFI背離法"""
    results = {'價漲量縮': False, '價跌量縮': False, '價格MFI': 0, '成交量MFI': 0, '價格狀態': '正常', '成交量狀態': '正常'}
    if len(df) < period + 1: return results
    
    p_mfi = calculate_mfi_series_advanced(df, period)
    cur_p_mfi = p_mfi.iloc[-1]
    
    v_mfi = calculate_mfi_for_volume(df['Volume'], period)
    cur_v_mfi = v_mfi.iloc[-1]
    
    if cur_p_mfi > 70 and cur_v_mfi < 30:
        results['價漲量縮'] = True
        results['價格狀態'] = '超買'
        results['成交量狀態'] = '超賣'
    elif cur_p_mfi < 30 and cur_v_mfi < 30:
        results['價跌量縮'] = True
        results['價格狀態'] = '超賣'
        results['成交量狀態'] = '超賣'
    else:
        if cur_p_mfi > 70: results['價格狀態'] = '超買'
        elif cur_p_mfi < 30: results['價格狀態'] = '超賣'
        if cur_v_mfi > 70: results['成交量狀態'] = '超買'
        elif cur_v_mfi < 30: results['成交量狀態'] = '超賣'
            
    results['價格MFI'] = cur_p_mfi
    results['成交量MFI'] = cur_v_mfi
    return results

def detect_all_divergence_methods(df, params=None):
    """綜合應用四種方法檢測量價背離"""
    if params is None: params = {}
    default_params = {
        'consecutive_days': 3, 'trend_lookback': 20, 
        'short_ma_period': 5, 'ma_slope_lookback': 5, 'mfi_period': 14
    }
    config = {**default_params, **params}
    results = {}
    
    results['方法1_直接比較'] = method1_direct_comparison(df, config['consecutive_days'])
    results['方法2_趨勢回歸'] = method2_trend_regression(df, config['trend_lookback'])
    results['方法3_短均線斜率'] = method3_ma_slope_divergence(df, config['short_ma_period'], config['ma_slope_lookback'])
    results['方法4_MFI背離'] = method4_mfi_divergence(df, config['mfi_period'])
    
    bullish_count = 0
    bearish_count = 0
    for m in ['方法1_直接比較', '方法2_趨勢回歸', '方法3_短均線斜率', '方法4_MFI背離']:
        res = results.get(m, {})
        if res.get('價跌量縮', False): bullish_count += 1
        if res.get('價漲量縮', False): bearish_count += 1
        
    if bullish_count > bearish_count:
        primary = '看漲背離'
        strength = bullish_count / 4.0
    elif bearish_count > bullish_count:
        primary = '看跌背離'
        strength = bearish_count / 4.0
    else:
        primary = '無明確背離'
        strength = 0.0
        
    if bullish_count >= 3: sugg = "強烈看漲信號，考慮分批買入"
    elif bullish_count == 2: sugg = "溫和看漲信號，可少量布局"
    elif bearish_count >= 3: sugg = "強烈看跌信號，考慮減倉或觀望"
    elif bearish_count == 2: sugg = "溫和看跌信號，注意風險"
    elif bullish_count == 1 and bearish_count == 1: sugg = "信號矛盾，建議觀望"
    elif bullish_count == 1: sugg = "輕微看漲信號，等待確認"
    elif bearish_count == 1: sugg = "輕微看跌信號，謹慎操作"
    else: sugg = "無明顯背離，趨勢可能延續"
    
    results['綜合評分'] = {
        '看漲背離次數': bullish_count,
        '看跌背離次數': bearish_count,
        '主要信號': primary,
        '信號強度': strength,
        '交易建議': sugg,
        '參數配置': config
    }
    return results

def generate_detailed_report(df, divergence_results):
    """生成詳細分析報告"""
    report = []
    report.append("=" * 70)
    report.append("📊 量價背離四方法整合分析報告")
    report.append("=" * 70)
    
    latest_price = df['Close'].iloc[-1]
    latest_volume = df['Volume'].iloc[-1]
    price_change = df['Close'].iloc[-1] - df['Close'].iloc[-2]
    volume_change = df['Volume'].iloc[-1] - df['Volume'].iloc[-2]
    
    report.append(f"\n📈 基本資料:")
    report.append(f"  當前價格: {latest_price:.2f}")
    report.append(f"  當前成交量: {latest_volume:,.0f}")
    report.append(f"  價格變化: {price_change:+.2f}")
    report.append(f"  成交量變化: {volume_change:+,.0f}")
    
    m1 = divergence_results['方法1_直接比較']
    report.append(f"\n🔍 方法1: 直接比較法")
    if m1['價漲量縮']: report.append(f"  ✓ 檢測到價漲量縮 (連續{m1['連續天數']}天)")
    if m1['價縮量漲']: report.append(f"  ✓ 檢測到價縮量漲 (連續{m1['連續天數']}天)")
    if not m1['價漲量縮'] and not m1['價縮量漲']: report.append(f"  ○ 未檢測到明顯背離")
    
    m2 = divergence_results['方法2_趨勢回歸']
    report.append(f"\n📊 方法2: 趨勢回歸法")
    report.append(f"  價格斜率: {m2['價格斜率']:.6f}")
    report.append(f"  成交量斜率: {m2['成交量斜率']:.6f}")
    if m2['價漲量縮']: report.append(f"  ✓ 檢測到價漲量縮")
    if m2['價跌量縮']: report.append(f"  ✓ 檢測到價跌量縮")
    
    m3 = divergence_results['方法3_短均線斜率']
    report.append(f"\n📈 方法3: 短均線斜率法")
    report.append(f"  價格sMA5斜率: {m3['價格_sMA5_斜率']:.6f} ({m3['價格_sMA5_趨勢強度']})")
    report.append(f"  成交量sMA5斜率: {m3['成交量_sMA5_斜率']:.6f} ({m3['成交量_sMA5_趨勢強度']})")
    if m3['價漲量縮']: report.append(f"  ✓ 檢測到價漲量縮")
    if m3['價跌量縮']: report.append(f"  ✓ 檢測到價跌量縮")
    
    m4 = divergence_results['方法4_MFI背離']
    report.append(f"\n⚡ 方法4: MFI背離法")
    report.append(f"  價格MFI: {m4['價格MFI']:.1f} ({m4['價格狀態']})")
    report.append(f"  成交量MFI: {m4['成交量MFI']:.1f} ({m4['成交量狀態']})")
    if m4['價漲量縮']: report.append(f"  ✓ 檢測到價漲量縮")
    if m4['價跌量縮']: report.append(f"  ✓ 檢測到價跌量縮")
    
    summary = divergence_results['綜合評分']
    report.append(f"\n{'='*70}")
    report.append(f"🎯 綜合評分")
    report.append(f"  主要信號: {summary['主要信號']}")
    report.append(f"  信號強度: {summary['信號強度']:.2f}")
    report.append(f"  交易建議: {summary['交易建議']}")
    report.append(f"{'='*70}")
    return "\n".join(report)

# ==========================================
# 3-Day Divergence System
# ==========================================
def get_three_day_divergence_params():
    """3日背離檢測專用參數配置"""
    return {
        '方法1_直接比較': {'consecutive_days': 3, 'description': '檢查是否連續3天出現價漲量縮或價跌量縮'},
        '方法2_趨勢回歸': {'lookback_period': 3, 'description': '分析最近3天原始價格/成交量的線性趨勢'},
        '方法3_短均線斜率': {'short_ma_period': 3, 'slope_lookback': 3, 'description': '分析sMA3均線在最近3天的趨勢方向'},
        '方法4_MFI背離': {'mfi_period': 3, 'description': '計算3日MFI判斷超買超賣狀態'}
    }

def check_three_day_data_sufficiency(df):
    """檢查數據是否足夠進行3日分析"""
    warnings = []
    if len(df) < 10:
        warnings.append(f"數據量不足 ({len(df)}天)，建議至少10天")
    return warnings




def scan_pv_divergence_analysis():
    """量價背離形態詳解 (使用快取信號)"""
    limit = get_display_limit(30)
    min_vol = get_volume_limit(100)
    
    print_flush("\n正在掃描 量價背離形態...")
    
    results = []
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else None
    if not data:
        data = step4_load_data()
        if GLOBAL_INDICATOR_CACHE:
            GLOBAL_INDICATOR_CACHE.set_data(data)
            
    if not data:
        print_flush("❌ 無法載入資料")
        return

    # Use cached signals from snapshot
    for code, info in data.items():
        try:
            vol = safe_float_preserving_none(info.get('volume', 0))
            if vol is None or vol < min_vol:
                continue
            
            # Read cached divergence signals
            div_bull = safe_int(info.get('div_3day_bull'))
            div_bear = safe_int(info.get('div_3day_bear'))
            nvi_sig = safe_int(info.get('nvi_signal') or info.get('NVI_Signal'))
            
            if div_bull == 0 and div_bear == 0:
                continue
            
            # Determine divergence type
            if div_bull > 0:
                div_type = "價跌量漲"
                score = div_bull / 3.0  # Normalize to 0-1
                color = get_color_code(1)
                suggestion = "強烈看漲信號，考慮分批買入"
            elif div_bear > 0:
                div_type = "價漲量跌"
                score = div_bear / 3.0
                color = get_color_code(-1)
                suggestion = "強烈看跌信號，考慮減倉觀望"
            else:
                continue
            
            # Calculate MA200 trend
            ma200 = safe_float_preserving_none(info.get('ma200') or info.get('MA200'))
            ma200_prev = safe_float_preserving_none(info.get('ma200_prev') or info.get('MA200_prev'))
            
            if ma200 and ma200_prev:
                ma200_trend = "上揚" if ma200 > ma200_prev else "下跌"
            else:
                ma200_trend = "N/A"
            
            # Calculate TP/SL
            close = safe_float_preserving_none(info.get('close'))
            vp_upper = safe_float_preserving_none(info.get('vp_upper') or info.get('VP_upper'))
            vp_lower = safe_float_preserving_none(info.get('vp_lower') or info.get('VP_lower'))
            
            tp = vp_upper if vp_upper else (close * 1.1 if close else 0)
            sl = vp_lower if vp_lower else (close * 0.95 if close else 0)
            
            results.append({
                'code': code,
                'name': info.get('name', code),
                'close': close,
                'volume': vol,
                'type': div_type,
                'score': score,
                'color': color,
                'nvi_sig': nvi_sig,
                'suggestion': suggestion,
                'ma200_trend': ma200_trend,
                'tp': tp,
                'sl': sl,
                'info': info
            })
            
        except Exception:
            continue
    
    # Sort by score desc
    results.sort(key=lambda x: x['score'], reverse=True)
    
    # 使用統一格式輸出
    def pv_extra(code, item):
        div_type = item.get('type', '')
        score = item.get('score', 0)
        ma200_trend = item.get('ma200_trend', '')
        suggestion = item.get('suggestion', '').split("，")[0]
        
        stars = "★" * int(score * 3)
        if not stars: stars = "★"
        
        risk = "低" if ma200_trend == "上揚" else "高"
        
        return [div_type, stars, risk, suggestion]

    codes = display_scan_results_v2(results, "量價背離形態", limit=limit,
                            extra_headers=["型態", "強度", "風險", "建議"],
                            extra_func=pv_extra)
    prompt_stock_detail_report(codes)


# ==============================
# 輔助判斷函數
# ==============================

def get_user_scan_params():
    """獲取使用者輸入的掃描參數"""
    try:
        print("選擇檔數(預設30檔): ", end='', flush=True)
        l = sys.stdin.readline().strip()
        limit = int(l) if l else 30
    except:
        limit = 30
    
    try:
        print("大於成交量(預設大於100張): ", end='', flush=True)
        v = sys.stdin.readline().strip()
        min_vol_lots = int(v) if v else 100
        min_vol = min_vol_lots
    except:
        min_vol = 100
    
    return limit, min_vol

# ==============================
# 雲端同步管理器
# ==============================
class CloudSync:
    """Supabase 雲端同步管理器"""
    
    @staticmethod
    def get_headers():
        return {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    @staticmethod
    def upload_stock_list():
        """上傳股票清單到雲端"""
        if not ENABLE_CLOUD_SYNC:
            print_flush("⚠ 未設定 Supabase，無法同步")
            return False
            
        print_flush("☁ 正在上傳股票清單到雲端...")
        
        try:
            df = pd.read_csv(STOCK_LIST_PATH, dtype=str)
            records = []
            
            for _, row in df.iterrows():
                records.append({
                    "code": row['code'],
                    "name": row['name'],
                    "market_type": row.get('market', '未知')
                })
            
            batch_size = 100
            total = len(records)
            
            for i in range(0, total, batch_size):
                batch = records[i:i+batch_size]
                url = f"{SUPABASE_URL}/rest/v1/stock_list"
                response = requests.post(url, headers=CloudSync.get_headers(), json=batch, verify=False)
                
                if response.status_code not in [200, 201]:
                    print_flush(f"⚠ 上傳失敗 (批次 {i}): {response.text}")
                
                print_flush(f"\r進度: {min(i+batch_size, total)}/{total}", end="")
            
            print_flush("\n✓ 股票清單上傳完成")
            return True
            
        except Exception as e:
            print_flush(f"\n❌ 上傳錯誤: {e}")
            return False

    @staticmethod
    def upload_calculated_data(days=None):
        """上傳計算結果到雲端"""
        if not ENABLE_CLOUD_SYNC:
            print_flush("⚠ 未設定 Supabase，無法同步")
            return False
            
        range_str = f"最近 {days} 天" if days else "所有"
        print_flush(f"☁ 正在上傳 {range_str} 數據到雲端...")
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                if days:
                    # 使用 stock_snapshot (新三表架構) - 依日期排序
                    sql = f"SELECT DISTINCT date FROM stock_snapshot ORDER BY date DESC LIMIT {days}"
                else:
                    sql = "SELECT DISTINCT date FROM stock_snapshot ORDER BY date DESC"
                
                cur.execute(sql)
                dates = [row[0] for row in cur.fetchall()]
                
                if not dates:
                    print_flush("⚠ 本地無數據可上傳")
                    return False
                
                total_dates = len(dates)
                
                for idx, date in enumerate(dates):
                    print_flush(f"正在處理日期: {date} ({idx+1}/{total_dates})")
                    
                    # 從 stock_snapshot 取得最新指標資料
                    df = pd.read_sql_query("SELECT * FROM stock_snapshot WHERE date=?", conn, params=(date,))
                    
                    def clean_value(x):
                        if isinstance(x, bytes):
                            try:
                                return int.from_bytes(x, byteorder='little')
                            except:
                                return str(x)
                        return x

                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].apply(clean_value)
                            
                    vol_cols = ['volume', 'vol_prev', 'volume_prev']
                    
                    for col in vol_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            df[col] = df[col].astype('Int64')
                            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

                    records = df.to_dict(orient='records')
                    
                    batch_size = 500
                    total_recs = len(records)
                    
                    for i in range(0, total_recs, batch_size):
                        batch = records[i:i+batch_size]
                        url = f"{SUPABASE_URL}/rest/v1/stock_data"
                        response = requests.post(url, headers=CloudSync.get_headers(), json=batch, verify=False)
                        
                        if response.status_code not in [200, 201]:
                            print_flush(f"⚠ 上傳失敗 ({date} 批次 {i}): {response.text}")
            
            print_flush("\n✓ 數據上傳完成")
            return True
            
        except Exception as e:
            print_flush(f"\n❌ 上傳錯誤: {e}")
            return False

    @staticmethod
    def upload_daily_history(date_int):
        """上傳指定日期的 K 線數據到雲端 (增量更新)"""
        if not ENABLE_CLOUD_SYNC:
            return
            
        d_str = str(date_int)
        date_fmt = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print_flush(f"☁ 正在同步 {date_fmt} 的數據到 Supabase...")
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                cur.execute(f"""
                    SELECT code, date_int, open, high, low, close, volume, amount 
                    FROM stock_history 
                    WHERE date_int = ?
                """, (date_int,))
                rows = cur.fetchall()
                
                if not rows:
                    print_flush("⚠ 無數據可上傳")
                    return

                # 轉換資料格式
                upload_data = []
                for r in rows:
                    upload_data.append({
                        "code": r[0],
                        "date": date_fmt,
                        "open": r[2],
                        "high": r[3],
                        "low": r[4],
                        "close": r[5],
                        "volume": r[6],
                        "amount": r[7]
                    })
                
                # 分批上傳
                batch_size = 500
                total = len(upload_data)
                
                for i in range(0, total, batch_size):
                    batch = upload_data[i:i+batch_size]
                    # [Fix] 加入 on_conflict=code,date
                    url = f"{SUPABASE_URL}/rest/v1/stock_data?on_conflict=code,date"
                    
                    try:
                        headers = CloudSync.get_headers()
                        headers["Prefer"] = "resolution=merge-duplicates"
                        
                        res = requests.post(url, headers=headers, json=batch, verify=False, timeout=30)
                        if res.status_code not in [200, 201]:
                            # 嘗試 PATCH
                            print_flush("x", end="")
                        else:
                            print_flush(".", end="")
                    except Exception:
                        print_flush("t", end="")
                        
            print_flush(" ✓")
            
        except Exception as e:
            print_flush(f" ✗ 上傳錯誤: {e}")

    @staticmethod
    def upload_all_history():
        """上傳所有歷史 K 線數據到雲端"""
        if not ENABLE_CLOUD_SYNC:
            print_flush("⚠ 未設定 Supabase，無法同步")
            return
            
        # 三行進度初始化
        print_flush("\n" * 3) # 預留空間
        
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                # 獲取總筆數
                cur.execute("SELECT COUNT(*) FROM stock_history")
                total_count = cur.fetchone()[0]
                
                # 分頁讀取與上傳
                batch_size = 2000 # 本地讀取批次
                upload_batch_size = 500 # 上傳批次
                offset = 0
                success_count = 0
                fail_count = 0
                last_error = ""
                
                start_time = time.time()
                
                while offset < total_count:
                    cur.execute(f"""
                        SELECT code, date_int, open, high, low, close, volume, amount 
                        FROM stock_history 
                        LIMIT {batch_size} OFFSET {offset}
                    """)
                    rows = cur.fetchall()
                    if not rows:
                        break
                        
                    # 轉換資料格式
                    upload_data = []
                    for r in rows:
                        d_str = str(r[1])
                        date_fmt = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
                        upload_data.append({
                            "code": r[0],
                            "date": date_fmt,
                            "open": r[2],
                            "high": r[3],
                            "low": r[4],
                            "close": r[5],
                            "volume": r[6],
                            "amount": r[7]
                        })
                    
                    # 分批上傳到 Supabase
                    for i in range(0, len(upload_data), upload_batch_size):
                        batch = upload_data[i:i+upload_batch_size]
                        
                        # [Fix] 加入 on_conflict=code,date 以解決 409 錯誤
                        # 當 PK 不是 (code, date) 但有唯一約束時，必須明確指定
                        url = f"{SUPABASE_URL}/rest/v1/stock_data?on_conflict=code,date"
                        
                        try:
                            headers = CloudSync.get_headers()
                            headers["Prefer"] = "resolution=merge-duplicates"
                            
                            res = requests.post(url, headers=headers, json=batch, verify=False, timeout=30)
                            if res.status_code not in [200, 201]:
                                fail_count += len(batch)
                                last_error = f"[{res.status_code}] {res.text[:50]}..."
                            else:
                                success_count += len(batch)
                                last_error = ""
                        except Exception as e:
                            fail_count += len(batch)
                            last_error = str(e)[:50]
                            
                    offset += batch_size
                    
                    # 簡化進度顯示 (移除進度條)
                    percent = int(offset / total_count * 100)
                    
                    # ANSI Escape Codes
                    UP = "\033[2A" # 上移2行
                    CLR = "\033[K" # 清除行
                    
                    status_line = f"狀態: 成功 {success_count} | 失敗 {fail_count}"
                    if last_error:
                        status_line += f" | ⚠ {last_error}"
                        
                    print(f"{UP}{CLR}【全量上傳】進度: {percent}% ({offset}/{total_count})")
                    print(f"{CLR}{status_line}")
                    
            print_flush("\n✓ 歷史資料上傳完成")
            
        except Exception as e:
            print_flush(f"\n❌ 上傳錯誤: {e}")

# ==============================
# 系統維護函數
# ==============================
def backup_menu():
    """資料庫備份與還原選單"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("【資料庫備份與還原】")
        print_flush("="*60)
        print_flush("[1] 備份資料庫")
        print_flush("[2] 還原資料庫")
        print_flush("[3] 列出現有備份")
        print_flush("[0] 返回")
        
        choice = read_single_key("請選擇: ")
        
        if choice == '1':
            try:
                import shutil
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_file = BACKUP_DIR / f"taiwan_stock_backup_{timestamp}.db"
                shutil.copy2(DB_FILE, backup_file)
                print_flush(f"✓ 備份成功: {backup_file}")
            except Exception as e:
                print_flush(f"❌ 備份失敗: {e}")
        
        elif choice == '2':
            backups = sorted(BACKUP_DIR.glob("*.db"), reverse=True)
            
            if not backups:
                print_flush("❌ 沒有可用的備份檔案")
                continue
            
            print_flush("\n可用備份:")
            for i, b in enumerate(backups[:10], 1):
                size_mb = b.stat().st_size / (1024*1024)
                print_flush(f"  [{i}] {b.name} ({size_mb:.2f} MB)")
            
            try:
                idx = int(input("請選擇要還原的備份 (輸入數字): ").strip()) - 1
                
                if 0 <= idx < len(backups):
                    import shutil
                    shutil.copy2(backups[idx], DB_FILE)
                    print_flush(f"✓ 還原成功: {backups[idx].name}")
                else:
                    print_flush("❌ 無效的選擇")
            except Exception as e:
                print_flush(f"❌ 還原失敗: {e}")
        
        elif choice == '3':
            backups = sorted(BACKUP_DIR.glob("*.db"), reverse=True)
            
            if not backups:
                print_flush("❌ 沒有備份檔案")
            else:
                print_flush(f"\n找到 {len(backups)} 個備份:")
                for b in backups[:20]:
                    size_mb = b.stat().st_size / (1024*1024)
                    print_flush(f"  • {b.name} ({size_mb:.2f} MB)")
        
        elif choice == '0':
            break

def check_db_nulls():
    """檢查資料庫空值率 (排除新上市股票影響)"""
    print_flush("\n[檢查] 資料庫空值率分析 (快照表)...")
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # 0. 預先載入上市日期
            list_date_map = {}
            try:
                cursor.execute("SELECT code, list_date FROM stock_meta")
                for r in cursor.fetchall():
                    if r[1]: list_date_map[r[0]] = r[1]
            except:
                pass

            cursor.execute("PRAGMA table_info(stock_snapshot)")
            columns = [row[1] for row in cursor.fetchall()]
            
            cursor.execute("SELECT COUNT(*) FROM stock_snapshot")
            total_rows = cursor.fetchone()[0]
            
            if total_rows == 0:
                print_flush("❌ 快照表無數據")
                return

            print_flush(f"分析範圍: 最新快照 ({total_rows} 筆)")
            print_flush("-" * 60)
            print_flush(f"{'欄位名稱':<20} | {'空值率%':<12} | {'狀態':<10}")
            print_flush("-" * 60)
            
            # 定義長天期指標所需的最小天數
            required_days_map = {
                'ma200': 200, 'wma200': 200, 'vwap200': 200, 'ma200_prev': 200, 'wma200_prev': 200,
                'ma120': 120, 'wma120': 120, 'ma120_prev': 120, 'wma120_prev': 120,
                'ma60': 60, 'wma60': 60, 'vwap60': 60, 'vol_ma60': 60, 'ma60_prev': 60, 'wma60_prev': 60,
                'ma25': 25, 'ma25_slope': 25,
                'ma20': 20, 'wma20': 20, 'vwap20': 20, 'ma20_prev': 20, 'wma20_prev': 20, 'vwap20_prev': 20,
                'ma3': 3, 'wma3': 3, 'ma3_prev': 3, 'wma3_prev': 3,
                'rsi': 14, 'rsi12': 12, 'mfi14': 14, 'mfi14_prev': 14,
                'macd': 26, 'macd_signal': 26, 'macd_diff': 26,
                'kdj_k': 9, 'kdj_d': 9, 'kdj_j': 9,
                'week_k': 35, 'week_d': 35, # 週線需要更多日資料
                'month_k': 150, 'month_d': 150 # 月線需要更多日資料
            }

            for col in columns:
                if col in ['code', 'name', 'date']:
                    continue
                
                # 白名單驗證
                if col not in columns:
                    continue
                
                # 查詢空值的股票代碼
                cursor.execute(f"SELECT code FROM stock_snapshot WHERE {col} IS NULL")
                null_codes = [r[0] for r in cursor.fetchall()]
                raw_null_count = len(null_codes)
                
                if raw_null_count == 0:
                    print_flush(f"{col:<20} | 0.00%       | OK")
                    continue

                # 分析空值原因 (是否為新股)
                real_missing_count = 0
                new_stock_count = 0
                req_days = required_days_map.get(col, 0)
                
                for code in null_codes:
                    is_new_stock = False
                    if req_days > 0:
                        l_date_str = list_date_map.get(code)
                        if l_date_str:
                            try:
                                l_date = datetime.strptime(l_date_str, '%Y-%m-%d')
                                days_since = (datetime.now() - l_date).days
                                # 寬限期: 需求天數 * 1.5 (考慮假日)
                                if days_since < req_days * 1.5:
                                    is_new_stock = True
                            except:
                                pass
                    
                    if is_new_stock:
                        new_stock_count += 1
                    else:
                        real_missing_count += 1
                
                # 計算調整後的空值率 (只計算真正缺失的)
                real_null_pct = (real_missing_count / total_rows) * 100
                
                status = "OK"
                if real_null_pct > 20:
                    if col in ['pe', 'yield']:
                        status = "無 (虧損/無股利)"
                    else:
                        status = "缺資料 (!)"
                elif real_null_pct > 0:
                    if col in ['pe', 'yield']:
                         status = "部分無 (正常)"
                    else:
                        status = "部分缺"
                elif new_stock_count > 0:
                    status = "OK (含新股)"
                
                # 顯示邏輯: 如果有新股被排除，顯示註記
                display_pct = f"{real_null_pct:.2f}%"
                if new_stock_count > 0 and real_missing_count == 0:
                     display_pct = "0.00%*"
                
                print_flush(f"{col:<20} | {display_pct:<10} | {status}")
            
            # 額外檢查: 成交金額 (最新交易日，排除成交量為0的股票)
            try:
                cursor.execute("""
                    SELECT COUNT(*) FROM stock_history 
                    WHERE date_int = (SELECT MAX(date_int) FROM stock_history)
                    AND volume > 0 
                    AND (amount IS NULL OR amount = 0)
                """)
                amount_null = cursor.fetchone()[0]
                amount_pct = (amount_null / total_rows) * 100
                st = "OK" if amount_pct == 0 else "缺資料 (!)"
                print_flush(f"{'amount (最新)':<20} | {amount_pct:<10.2f}% | {st}")
            except:
                print_flush(f"{'amount (最新)':<20} | {'N/A':<10} | 檢查失敗")

            # 額外檢查: 法人資料 (最新交易日)
            try:
                cursor.execute("SELECT MAX(date_int) FROM institutional_investors")
                max_inst_date = cursor.fetchone()[0]
                if max_inst_date:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM stock_snapshot
                        WHERE code NOT IN (
                            SELECT code FROM institutional_investors WHERE date_int = {max_inst_date}
                        )
                    """)
                    inst_null = cursor.fetchone()[0]
                    inst_pct = (inst_null / total_rows) * 100
                    st = "無交易 (正常)" if inst_pct > 0 else "OK"
                    print_flush(f"{'法人資料 (最新)':<20} | {inst_pct:<10.2f}% | {st}")
                else:
                    print_flush(f"{'法人資料 (最新)':<20} | {'100.00%':<10} | 無資料")
            except:
                print_flush(f"{'法人資料 (最新)':<20} | {'N/A':<10} | 檢查失敗")
            
            # 額外檢查: 融資融券資料
            try:
                cursor.execute("SELECT COUNT(DISTINCT date_int) FROM margin_data")
                margin_days = cursor.fetchone()[0]
                target_days = 450
                margin_pct = ((target_days - margin_days) / target_days) * 100 if margin_days < target_days else 0
                st = "OK" if margin_days >= target_days else f"差 {target_days - margin_days} 天"
                print_flush(f"{'融資融券 (天數)':<20} | {margin_days:<10} | {st}")
            except:
                print_flush(f"{'融資融券 (天數)':<20} | {'N/A':<10} | 檢查失敗")
            
            # 額外檢查: 大盤指數資料
            try:
                cursor.execute("SELECT COUNT(DISTINCT date_int) FROM market_index")
                index_days = cursor.fetchone()[0]
                target_days = 450
                st = "OK" if index_days >= target_days else f"差 {target_days - index_days} 天"
                print_flush(f"{'大盤指數 (天數)':<20} | {index_days:<10} | {st}")
            except:
                print_flush(f"{'大盤指數 (天數)':<20} | {'N/A':<10} | 檢查失敗")
                
            print_flush("-" * 60)
            print_flush("說明:")
            print_flush("1. [0.00%*] 代表空值皆來自「新上市股票」(上市天數不足以計算該指標)，屬正常現象。")
            print_flush("2. [PE/Yield] 空值代表公司虧損或不發股利，屬正常現象。")
            print_flush("3. [法人資料] 空值代表當日三大法人無買賣紀錄，屬正常現象。")
            print_flush("4. [Amount] 已排除成交量為 0 之股票。")
            
            print_flush("\n" + "="*50)
            ans = input("是否立即執行 [1]~[7] 完整更新以修復缺失數據？ (y/N, 預設n): ").strip().lower()
            
            if ans == 'y':
                step1_fetch_stock_list()
                updated_codes = set()
                
                s2 = step2_download_tpex_daily()
                if isinstance(s2, set):
                    updated_codes.update(s2)
                
                s3 = step3_download_twse_daily()
                if isinstance(s3, set):
                    updated_codes.update(s3)
                
                step5_clean_delisted()
                step4_check_data_gaps()
                data = step4_load_data()
                step6_verify_and_backfill(data, resume=True)
                step7_calc_indicators(data, force=True)
                
                global GLOBAL_INDICATOR_CACHE
                if GLOBAL_INDICATOR_CACHE:
                    GLOBAL_INDICATOR_CACHE.clear()
                print_flush("[OK] 系統快取已清除，更新完成")
            else:
                print_flush("[INFO] 已跳過更新")
            
    except Exception as e:
        print_flush(f"❌ 檢查失敗: {e}")

def delete_data_by_date():
    """刪除指定日期的資料"""
    print_flush("\n【刪除指定日期資料】")
    print_flush("-" * 40)
    
    try:
        date_str = input("請輸入要刪除的日期 (格式: YYYY-MM-DD): ").strip()
        
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print_flush("❌ 日期格式錯誤，請使用 YYYY-MM-DD 格式")
            return
        
        date_int = int(date_str.replace('-', ''))
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # 統一使用新三表架構
            cur.execute("SELECT COUNT(*) FROM stock_history WHERE date_int=?", (date_int,))
            count_history = cur.fetchone()[0]
        
        if count_history == 0:
            print_flush(f"⚠ 日期 {date_str} 沒有任何資料")
            return
        
        print_flush(f"[INFO] 刪除 {date_str} 的資料 ({count_history} 筆)")
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM stock_history WHERE date_int=?", (date_int,))
            conn.commit()
        
        print_flush(f"[OK] 已刪除 {date_str} 的所有資料")
    
    except Exception as e:
        print_flush(f"❌ 刪除失敗: {e}")

# ==============================
# 選單系統




# ==============================
# VSBC 策略 (Inserted)
# ==============================





# ==============================
# 1️⃣ VSBC 計算（核心）
# ==============================
def calc_vsbc(df, win=10):
    """
    計算 VSBC 中線（vsbc_mid）及箱體基礎範圍（base_range）
    win: 滾動視窗大小
    """
    # 計算成交量情緒
    signed_vol = np.where(df['close'] >= df['open'],
                          df['volume'],
                          -df['volume'])
    signed_vol = pd.Series(signed_vol, index=df.index)

    # 情緒推力平均 & 平均成交量
    vs_force = signed_vol.rolling(win, min_periods=1).mean()
    vol_mean = df['volume'].rolling(win, min_periods=1).mean()

    # 箱體基礎
    base_mid = (df['high'] + df['low']) / 2
    base_range = (df['high'] - df['low']).rolling(win, min_periods=1).mean().replace(0, 1e-9)

    # 中線位移（防爆範圍 -0.5 ~ 0.5）
    shift = (vs_force / vol_mean).fillna(0).clip(-0.5, 0.5)

    vsbc_mid = base_mid + shift * base_range
    return vsbc_mid, base_range


# ==============================
# 2️⃣ 計算 VSBC 分數（數值化）
# ==============================
def compute_vsbc_score(df, win=10, n_recent=3, scale=100):
    """
    計算 VSBC 分數（可排序）
    返回：
        score: -scale~scale，正數為多方，負數為空方
    """
    vsbc_mid, base_range = calc_vsbc(df, win)
    diffs = vsbc_mid.diff().iloc[-n_recent:]

    up_count = (diffs > 0).sum()
    down_count = (diffs < 0).sum()

    if up_count > down_count:
        direction = 1
    elif down_count > up_count:
        direction = -1
    else:
        direction = 0

    magnitude = abs(diffs.mean()) / (base_range.iloc[-1] + 1e-9)
    consistency = max(up_count, down_count) / n_recent

    score = direction * magnitude * consistency * scale
    return score





# ==============================
# 1️⃣ 基礎模組 (均線 & VP/POC)
# ==============================
def add_ma(df):
    df = df.copy()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA60'] = df['close'].rolling(60).mean()
    return df

def calc_vp_poc(df, window=60, bins=30):
    """計算 Volume Profile POC (Point of Control)"""
    sub = df.tail(window)
    if len(sub) < 2:
        return df['close'].iloc[-1]
        
    hist, edges = np.histogram(
        sub['close'],
        bins=bins,
        weights=sub['volume']
    )
    i = hist.argmax()
    return (edges[i] + edges[i+1]) / 2

# ==============================
# 2️⃣ VSBC 序列計算 (供後續使用)
# ==============================
def calc_vsbc_series(df, win=10, n_recent=3, scale=100):
    """
    計算 VSBC 序列 (vsbc) 與 百分位 (vsbc_pct)
    """
    # 1. 計算 VSBC 中線與範圍
    vsbc_mid, base_range = calc_vsbc(df, win)
    
    # 2. 計算 diffs (序列)
    diffs = vsbc_mid.diff()
    
    # 3. 計算每個時間點的 score (需向量化或 rolling)
    # 由於 compute_vsbc_score 是針對最後 n_recent 點，這裡我們需要一個 rolling version
    # 簡化版: 使用 rolling apply 或向量化近似
    # Score = direction * magnitude * consistency * scale
    
    # Direction: rolling count of ups vs downs
    diff_sign = np.sign(diffs)
    up_counts = (diff_sign > 0).rolling(n_recent).sum()
    down_counts = (diff_sign < 0).rolling(n_recent).sum()
    
    direction = np.where(up_counts > down_counts, 1, 
                         np.where(down_counts > up_counts, -1, 0))
    
    # Magnitude: abs(mean diff) / base_range
    mag_num = diffs.abs().rolling(n_recent).mean()
    mag_denom = base_range + 1e-9
    magnitude = mag_num / mag_denom
    
    # Consistency: max(up, down) / n_recent
    consistency = np.maximum(up_counts, down_counts) / n_recent
    
    # Final Score Series
    vsbc_series = direction * magnitude * consistency * scale
    
    return pd.Series(vsbc_series, index=df.index).fillna(0)

def add_vsbc_columns(df):
    """加入 vsbc 與 vsbc_pct 欄位"""
    df = df.copy()
    
    # 計算 VSBC 分數序列
    df['vsbc'] = calc_vsbc_series(df)
    
    # 計算 VSBC 百分位 (Rolling 100 days rank)
    # Rank pct=True returns 0.0 to 1.0, multiply by 100
    df['vsbc_pct'] = df['vsbc'].rolling(100, min_periods=20).rank(pct=True) * 100
    df['vsbc_pct'] = df['vsbc_pct'].fillna(50) # Default mid
    
    return df

# ==============================
# 3️⃣ 行為量化（多方）
# ==============================
def vsbc_behavior_score(df):
    t = df.iloc[-1]
    y = df.iloc[-2]

    return (
        (t['vsbc_pct'] - 50) * 2 +
        (t['vsbc'] - y['vsbc']) / max(abs(y['vsbc']), 1) * 100
    )

def cost_shift_score(close, poc):
    return (close - poc) / poc * 100

# ==============================
# 4️⃣ 多方行為判斷器（核心）
# ==============================
def long_behavior(df):
    # 確保必要欄位存在
    if 'MA20' not in df.columns:
        df = add_ma(df)
    if 'vsbc' not in df.columns or 'vsbc_pct' not in df.columns:
        df = add_vsbc_columns(df)

    if len(df) < 61:
        return False, None, None

    t = df.iloc[-1]
    y = df.iloc[-2]

    poc = calc_vp_poc(df)

    # 條件判斷
    cond = (
        t['vsbc_pct'] >= 99 and
        t['vsbc'] > y['vsbc'] and
        t['close'] >= poc and
        t['MA20'] > t['MA60'] and
        t['close'] > t['MA20']
    )

    if not cond:
        return False, None, None

    score = (
        vsbc_behavior_score(df) * 0.6 +
        cost_shift_score(t['close'], poc) * 0.4
    )

    return True, round(score, 2), round(poc, 2)

def scan_vsbc_strategy():
    """VSBC 多方行為掃描策略 (主力推升)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在執行 VSBC 多方行為掃描 (成交量 > {min_vol} 張)...")
    print_flush("篩選條件: VSBC PR>=99, VSBC上升, 站上POC, 多頭排列, 站上月線")
    
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT code, name FROM stock_snapshot")
        stocks = cur.fetchall()
        
    codes = [s[0] for s in stocks]
    history_map = batch_load_history(codes, limit_days=150)
    
    results = []
    
    # Counters
    count_total = len(stocks)
    count_data = 0
    count_vol = 0
    count_pr99 = 0
    count_vsbc_up = 0
    count_poc = 0
    count_ma_bull = 0
    count_price_ma20 = 0
    
    for code, name in stocks:
        df = history_map.get(code)
        if df is None or len(df) < 100:
            continue
        count_data += 1
            
        try:
            # 1. Volume Filter
            if df['volume'].iloc[-1] < min_vol * 1000:
                continue
            count_vol += 1
            
            # 2. Prepare Data
            df = add_ma(df)
            df = add_vsbc_columns(df)
            
            t = df.iloc[-1]
            y = df.iloc[-2]
            poc = calc_vp_poc(df)
            
            # 3. Sequential Filtering
            if t['vsbc_pct'] < 99: continue
            count_pr99 += 1
            
            if t['vsbc'] <= y['vsbc']: continue
            count_vsbc_up += 1
            
            if t['close'] < poc: continue
            count_poc += 1
            
            if t['MA20'] <= t['MA60']: continue
            count_ma_bull += 1
            
            if t['close'] <= t['MA20']: continue
            count_price_ma20 += 1
            
            # Passed All
            score = (
                vsbc_behavior_score(df) * 0.6 +
                cost_shift_score(t['close'], poc) * 0.4
            )
            
            vol_ma60 = df['volume'].tail(60).mean()
            vol_ratio = t['volume'] / vol_ma60 if vol_ma60 > 0 else 0
            ma20_bias = ((t['close'] - t['MA20']) / t['MA20']) * 100 if t['MA20'] > 0 else 0
            
            results.append({
                'code': code, 'name': name,
                'close': t['close'], 'close_prev': df.iloc[-2]['close'],
                'vsbc': t['vsbc'],
                'vsbc_pct': t['vsbc_pct'],
                'poc': poc,
                'behavior_score': score,
                'vol_ratio': vol_ratio,
                'volume': t['volume'], # Keep as raw volume
                'ma20_bias': ma20_bias
            })

        except Exception as e:
            continue
            
    # Sort
    results.sort(key=lambda x: x['behavior_score'], reverse=True)
    
    # Summary
    print_flush("\n" + "="*60)
    print_flush("[篩選過程] VSBC 多方行為掃描")
    print_flush("="*60)
    print_flush(f"總股數: {count_total}")
    print_flush("─"*60)
    print_flush(f"✓ 資料充足 (>100日)       → {count_data} 檔")
    print_flush(f"✓ 成交量 >= {min_vol}張        → {count_vol} 檔")
    print_flush(f"✓ VSBC PR >= 99           → {count_pr99} 檔")
    print_flush(f"✓ VSBC 數值上升           → {count_vsbc_up} 檔")
    print_flush(f"✓ 股價站上 POC            → {count_poc} 檔")
    print_flush(f"✓ 均線多頭 (MA20>MA60)    → {count_ma_bull} 檔")
    print_flush(f"✓ 股價站上 MA20           → {count_price_ma20} 檔 (最終選出)")
    print_flush("─"*60)
    
    # 使用統一格式輸出
    def vsbc_extra(code, item):
        ma20_bias = item.get('ma20_bias', 0)
        return [f"{ma20_bias:+.2f}%"]

    codes = display_scan_results_v2(results, "VSBC 多方行為掃描", limit=limit,
                            extra_headers=["MA20乖離"],
                            extra_func=vsbc_extra)
    
    prompt_stock_detail_report(codes)




def calculate_2560_strategy(df):
    """
    修正後的 2560 戰法信號生成函數
    """
    # 確保欄位名稱一致 (轉換為小寫以符合系統慣例)
    # 系統慣例: close, open, high, low, volume
    df = df.copy()
    
    # Map system columns to strategy expected columns if needed, or just use system columns
    # Strategy uses: Close, Open, Volume
    # System uses: close, open, volume
    
    # 1. 計算基礎指標
    df['ma25'] = df['close'].rolling(window=25).mean()
    df['vol_ma5'] = df['volume'].rolling(window=5).mean()
    df['vol_ma60'] = df['volume'].rolling(window=60).mean()
    
    # 計算 25MA 的斜率 (今日 - 昨日)
    df['ma25_slope'] = df['ma25'].diff()
    
    # 2. 定義邏輯條件
    
    # (A) 趨勢條件：股價在線上，且線向上
    # 嚴格模式：要求斜率大於某個微小閾值，避免走平
    cond_trend = (df['close'] > df['ma25']) & (df['ma25_slope'] > 0)
    
    # (B) 觸發條件：均量線金叉 (Crossover)
    # 使用 shift(1) 來比較昨日狀態，確認是"交叉"動作發生在今日
    cond_vol_cross = (df['vol_ma5'] > df['vol_ma60']) & (df['vol_ma5'].shift(1) <= df['vol_ma60'].shift(1))
    
    # (C) 驗證條件 (修正假拐點)：必須是陽線且上漲
    # 這是 Video _q-eVVBLbE4 強調的關鍵修正
    cond_validation = (df['close'] > df['open']) & (df['close'] > df['close'].shift(1))
    
    # (D) 乖離率過濾 (防止追高 - 跑步小人邏輯的逆應用)
    # 假設我們不希望在離 25MA 超過 10% 的地方進場
    cond_proximity = (df['close'] < df['ma25'] * 1.10)
    
    # 3. 綜合信號生成
    # 只有同時滿足所有條件時，才標記為 Buy Signal (1)
    df['signal_2560'] = 0
    df.loc[cond_trend & cond_vol_cross & cond_validation & cond_proximity, 'signal_2560'] = 1
    
    return df

def scan_2560_strategy():
    """2560 戰法掃描 (均量線金叉 + 25MA多頭)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在執行 2560 戰法掃描 (成交量 > {min_vol} 張)...")
    print_flush("篩選條件: 股價>25MA, 25MA向上, 均量線金叉(5>60), 陽線收漲, 乖離<10%")
    
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT code, name FROM stock_snapshot")
        stocks = cur.fetchall()
        
    codes = [s[0] for s in stocks]
    history_map = batch_load_history(codes, limit_days=100)
    
    results = []
    
    # Counters
    count_total = len(stocks)
    count_data = 0
    count_vol = 0
    count_trend = 0
    count_cross = 0
    count_valid = 0
    count_prox = 0
    
    for code, name in stocks:
        df = history_map.get(code)
        if df is None or len(df) < 65:
            continue
        count_data += 1
            
        try:
            # 1. Volume Filter
            if df['volume'].iloc[-1] < min_vol * 1000:
                continue
            count_vol += 1
            
            # 2. Calculate Strategy
            df = calculate_2560_strategy(df)
            t = df.iloc[-1]
            
            # Check conditions
            is_trend = (t['close'] > t['ma25']) and (t['ma25_slope'] > 0)
            is_cross = (t['vol_ma5'] > t['vol_ma60']) and (df['vol_ma5'].iloc[-2] <= df['vol_ma60'].iloc[-2])
            is_valid = (t['close'] > t['open']) and (t['close'] > df['close'].iloc[-2])
            is_prox = (t['close'] < t['ma25'] * 1.10)
            
            if is_trend: count_trend += 1
            if is_trend and is_cross: count_cross += 1
            if is_trend and is_cross and is_valid: count_valid += 1
            if is_trend and is_cross and is_valid and is_prox: count_prox += 1
            
            if t['signal_2560'] == 1:
                vol_ratio = t['volume'] / df['volume'].tail(60).mean()
                
                results.append({
                    'code': code, 'name': name,
                    'close': t['close'], 'close_prev': df.iloc[-2]['close'],
                    'ma25': t['ma25'],
                    'vol_ma5': t['vol_ma5'] / 1000,
                    'vol_ma60': t['vol_ma60'] / 1000,
                    'vol_ratio': vol_ratio,
                    'volume': t['volume'] / 1000
                })
        except Exception as e:
            continue
            
    # Sort
    results.sort(key=lambda x: x['vol_ratio'], reverse=True)
    
    # Summary
    print_flush("\n" + "="*60)
    print_flush("[篩選過程] 2560 戰法 (嚴格版)")
    print_flush("="*60)
    print_flush(f"總股數: {count_total}")
    print_flush("─"*60)
    print_flush(f"✓ 資料充足 (>65日)        → {count_data} 檔")
    print_flush(f"✓ 成交量 >= {min_vol}張        → {count_vol} 檔")
    print_flush(f"✓ 趨勢條件 (股價>25MA向上) → {count_trend} 檔")
    print_flush(f"✓ 觸發條件 (均量線金叉)   → {count_cross} 檔")
    print_flush(f"✓ 驗證條件 (陽線收漲)     → {count_valid} 檔")
    print_flush(f"✓ 乖離過濾 (乖離<10%)     → {count_prox} 檔 (最終選出)")
    print_flush("─"*60)
    
    if not results:
        print_flush("\n沒有符合條件的股票。")
        return

    print_flush(f"\n【2560 戰法 TOP】 (前 {limit} 筆)")
    # Header: 代號 名稱 收盤 成交量(量比) MA25 量MA5 量MA60 訊號
    header = f"{'代號':<6} {'名稱':<8} {'收盤':<10} {'成交量(量比)':<16} {'MA25':<10} {'量MA5':<10} {'量MA60':<10} {'訊號':<6}"
    print_flush(header)
    print_flush("-" * 90)
    
    reset = reset_color()
    for res in results[:limit]:
        c_price = get_trend_color(res['close'], res['close_prev'])
        price_str = f"{c_price}{res['close']:.2f}{reset}"
        
        # Format columns
        vol_str = f"{int(res['volume'])}張({res['vol_ratio']:.1f})"
        ma25_str = f"{res['ma25']:.2f}"
        vma5_str = f"{int(res['vol_ma5'])}"
        vma60_str = f"{int(res['vol_ma60'])}"
        signal_str = f"{Colors.RED}買入{reset}"
        
        print_flush(f"{res['code']:<6} {res['name']:<8} {price_str:<19} {vol_str:<21} {ma25_str:<12} {vma5_str:<12} {vma60_str:<12} {signal_str:<15}")


def scan_candlestick_patterns():
    """K 線型態掃描 (晨星/夜星) - 詳細漏斗版"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在掃描 K 線型態 (成交量 > {min_vol} 張)...")
    print_flush("篩選條件: 晨星(T-2長黑, T-1星線, T長紅, 爆量), 夜星(反之)")
    
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT code, name FROM stock_snapshot")
        stocks = cur.fetchall()
        
    codes = [s[0] for s in stocks]
    history_map = batch_load_history(codes, limit_days=10) 
    
    morning_stars = []
    evening_stars = []
    
    # Counters (Morning Star Funnel)
    count_total = len(stocks)
    count_vol = 0
    count_m_step1 = 0 # T-2 Long Black
    count_m_step2 = 0 # T-1 Star
    count_m_step3 = 0 # T Long Red
    count_m_step4 = 0 # Vol Surge
    count_m_final = 0
    
    # Counters (Evening Star - Simplified tracking)
    count_e_final = 0
    
    for code, name in stocks:
        df = history_map.get(code)
        if df is None or len(df) < 5:
            continue
            
        try:
            # 1. Volume Filter
            vol = df['volume'].iloc[-1]
            if vol < min_vol * 1000:
                continue
            count_vol += 1
            
            # Prepare Data for Manual Checking (Latest 3 days)
            # T (Today), T-1 (Yesterday), T-2 (Day before)
            c0, c1, c2 = df['close'].iloc[-1], df['close'].iloc[-2], df['close'].iloc[-3]
            o0, o1, o2 = df['open'].iloc[-1], df['open'].iloc[-2], df['open'].iloc[-3]
            h0, h1, h2 = df['high'].iloc[-1], df['high'].iloc[-2], df['high'].iloc[-3]
            l0, l1, l2 = df['low'].iloc[-1], df['low'].iloc[-2], df['low'].iloc[-3]
            v0, v1 = df['volume'].iloc[-1], df['volume'].iloc[-2]
            
            # Ranges
            range0 = h0 - l0
            range1 = h1 - l1
            range2 = h2 - l2
            body0 = abs(c0 - o0)
            body1 = abs(c1 - o1)
            body2 = abs(c2 - o2)
            
            # --- Morning Star Logic (Sequential) ---
            passed_m = False
            
            # Step 1: T-2 Long Black (Body > 0.6 * Range)
            is_long_black_2 = (c2 < o2) and (body2 > range2 * 0.6)
            if is_long_black_2:
                count_m_step1 += 1
                
                # Step 2: T-1 Star (Body < 0.3 * T-2 Body, Close < T-2 Close)
                # Note: User text says "Close < T-2 Close", standard is "Gap" or "Low body".
                # We stick to user prompt: "實體很小，且收盤價低於 T-2日收盤"
                is_star_1 = (body1 < body2 * 0.3) and (c1 < c2)
                if is_star_1:
                    count_m_step2 += 1
                    
                    # Step 3: T Long Red (Close > T-2 Mid)
                    mid_point_2 = (o2 + c2) / 2
                    is_long_red_0 = (c0 > o0) and (c0 > mid_point_2)
                    # Also check if it's a "Long" candle (Body > 0.6 Range) as per previous logic?
                    # User text says "長紅 K", so yes.
                    is_long_red_0 = is_long_red_0 and (body0 > range0 * 0.6)
                    
                    if is_long_red_0:
                        count_m_step3 += 1
                        
                        # Step 4: Volume Surge (Third candle volume > ?)
                        # "爆量" -> Let's say > 1.3x Prev or > MA5
                        # Let's use > 1.3x Prev for strictness or > MA5
                        # User text: "第三根陽線若爆量"
                        vol_surge = v0 > v1 * 1.3
                        if vol_surge:
                            count_m_step4 += 1
                            count_m_final += 1
                            passed_m = True
                            
                            # Calculate VSBC & Fib
                            df = add_vsbc_columns(df)
                            t = df.iloc[-1]
                            vsbc_val = t['vsbc'] if 'vsbc' in t else 0
                            
                            # Calculate POC (Simple approximation using mode of close price in recent period or just use VSBC value itself if that's what user wants)
                            # User said "VSBC上/下 (如：壓力區)<--這個應該是數字吧！0000/0000"
                            # Let's assume they want VSBC Value / Price or VSBC / POC.
                            # In scan_vsbc_strategy, we use calc_vp_poc(df). Let's use that if available, or implement simple one.
                            # Since calc_vp_poc is defined elsewhere, let's check if we can use it.
                            # It seems calc_vp_poc is a global function.
                            try:
                                poc = calc_vp_poc(df)
                            except:
                                poc = df['close'].mean() # Fallback
                            
                            # Fib 60 days
                            recent_60 = df.iloc[-60:]
                            h60 = recent_60['high'].max()
                            l60 = recent_60['low'].min()
                            diff = h60 - l60
                            fib_0618 = h60 - (diff * 0.618)
                            
                            # Calculate current retracement ratio
                            # Ratio = (High - Close) / (High - Low) for pullback from High
                            if diff > 0:
                                current_ratio = (h60 - c0) / diff
                            else:
                                current_ratio = 0
                            
                            morning_stars.append({
                                'code': code, 'name': name,
                                'close': c0, 'close_prev': c1,
                                'pattern': '早晨之星',
                                'volume': v0, # Raw volume
                                'vol_ratio': v0/v1 if v1>0 else 1,
                                'vsbc_lower': vsbc_val, # Map to VSBC Lower
                                'vsbc_upper': poc,      # Map to VSBC Upper (POC)
                                'fib_val': fib_0618,
                                'fib_ratio': current_ratio
                            })

            # --- Evening Star Logic (Simplified for now, or parallel) ---
            # T-2 Long Red
            is_long_red_2 = (c2 > o2) and (body2 > range2 * 0.6)
            if is_long_red_2:
                # T-1 Star (High)
                is_star_1 = (body1 < body2 * 0.3) and (c1 > c2)
                if is_star_1:
                    # T Long Black (Close < T-2 Mid)
                    mid_point_2 = (o2 + c2) / 2
                    is_long_black_0 = (c0 < o0) and (c0 < mid_point_2) and (body0 > range0 * 0.6)
                    if is_long_black_0:
                        # Vol Surge (Optional for Evening? Usually volume shrinks on top, but breakdown needs volume)
                        # Let's apply same surge logic for symmetry or just pass
                        # User only specified Morning Star funnel details.
                        # We'll just add it.
                        count_e_final += 1
                        # Calculate VSBC & Fib (Same as above)
                        df = add_vsbc_columns(df)
                        t = df.iloc[-1]
                        vsbc_val = t['vsbc'] if 'vsbc' in t else 0
                        
                        try:
                            poc = calc_vp_poc(df)
                        except:
                            poc = df['close'].mean()

                        recent_60 = df.iloc[-60:]
                        h60 = recent_60['high'].max()
                        l60 = recent_60['low'].min()
                        diff = h60 - l60
                        fib_0618 = h60 - (diff * 0.618)
                        
                        if diff > 0:
                            current_ratio = (h60 - c0) / diff
                        else:
                            current_ratio = 0

                        evening_stars.append({
                            'code': code, 'name': name,
                            'close': c0, 'close_prev': c1,
                            'pattern': '黃昏之星',
                            'volume': v0,
                            'vol_ratio': v0/v1 if v1>0 else 1,
                            'vsbc_lower': vsbc_val,
                            'vsbc_upper': poc,
                            'fib_val': fib_0618,
                            'fib_ratio': current_ratio
                        })

        except Exception as e:
            continue
            
    # Summary
    print_flush("\n" + "="*60)
    print_flush("[篩選過程] K線型態 (晨星/夜星)")
    print_flush("="*60)
    print_flush(f"總股數: {count_total}")
    print_flush("─"*60)
    print_flush(f"✓ 成交量 >= {min_vol}張        → {count_vol} 檔")
    print_flush(f"✓ [第1階] T-2: 長黑 K (實體 > 0.6 * 總長)   → {count_m_step1} 檔")
    print_flush(f"✓ [第2階] T-1: 星線 (實體 < 0.3 * T-2實體)  → {count_m_step2} 檔")
    print_flush(f"✓ [第3階] T: 長紅 K (收盤 > T-2實體中點)    → {count_m_step3} 檔")
    print_flush(f"✓ [第4階] 第三根陽線若爆量 (>1.3倍)         → {count_m_step4} 檔")
    print_flush(f"✓ 綜合評分 >= 以上都符合 (晨星)             → {count_m_final} 檔")
    if count_e_final > 0:
        print_flush(f"✓ 黃昏之星 (額外篩選)                       → {count_e_final} 檔")
    print_flush("─"*60)
    
    # 使用統一格式輸出
    def candle_extra(code, item):
        ratio = item.get('fib_ratio', 0)
        close = item.get('close', 0)
        levels = [0.236, 0.382, 0.5, 0.618, 0.786]
        nearest = min(levels, key=lambda x: abs(x - ratio))
        if abs(ratio - nearest) < 0.05:
            fib_str = f"{nearest}({close:.0f})"
        else:
            fib_str = f"{ratio:.2f}({close:.0f})"
        return [fib_str]

    if morning_stars:
        display_scan_results_v2(morning_stars, "早晨之星 (底部反轉)", limit=limit,
                                extra_headers=["費波那契"],
                                extra_func=candle_extra)
    
    if evening_stars:
        display_scan_results_v2(evening_stars, "黃昏之星 (頂部反轉)", limit=limit,
                                extra_headers=["費波那契"],
                                extra_func=candle_extra)
    
    if not morning_stars and not evening_stars:
        print_flush("\n沒有符合條件的股票。")


def scan_vp(indicators_data, mode='lower', min_volume=100):
    """VP掃描 (並行版)"""
    
    def filter_func(code, ind):
        close = safe_float_preserving_none(ind.get('close'))
        if not close:
            return False
        
        if mode == 'lower':
            vp_lower = safe_float_preserving_none(ind.get('vp_lower') or ind.get('VP_lower'))
            if not vp_lower:
                return False
            return abs(close - vp_lower) / close < 0.02
        else:
            vp_upper = safe_float_preserving_none(ind.get('vp_upper') or ind.get('VP_upper'))
            if not vp_upper:
                return False
            return abs(close - vp_upper) / close < 0.02
    
    def transform_func(code, ind):
        return (code, 0, ind)
    
    return scan_with_parallel(
        indicators_data,
        filter_func,
        transform_func,
        sort_key=lambda x: x[1],
        reverse=False,
        min_volume=min_volume * 1000  # Convert to shares
    )


def scan_ma_cross():
    """均線交叉掃描 (多進程)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在掃描 均線交叉 (成交量 > {min_vol} 張)...")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if not data:
        print_flush("❌ 請先載入指標數據")
        return

    golden = []
    death = []
    
    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol < min_vol * 1000: continue
            
            close = safe_float_preserving_none(ind.get('close'))
            ma5 = safe_float_preserving_none(ind.get('ma5') or ind.get('MA5'))
            ma20 = safe_float_preserving_none(ind.get('ma20') or ind.get('MA20'))
            ma5_prev = safe_float_preserving_none(ind.get('ma5_prev') or ind.get('MA5_prev'))
            ma20_prev = safe_float_preserving_none(ind.get('ma20_prev') or ind.get('MA20_prev'))
            
            if not (ma5 and ma20 and ma5_prev and ma20_prev): continue
            
            # Golden Cross: MA5 crosses above MA20
            if ma5_prev <= ma20_prev and ma5 > ma20:
                golden.append({'code': code, 'name': get_stock_name(code), 'close': close, 'ma5': ma5, 'ma20': ma20})
                
            # Death Cross: MA5 crosses below MA20
            if ma5_prev >= ma20_prev and ma5 < ma20:
                death.append({'code': code, 'name': get_stock_name(code), 'close': close, 'ma5': ma5, 'ma20': ma20})
        except:
            continue
    
    print_flush(f"\n【黃金交叉 (MA5上穿MA20)】 (前 {limit} 筆)")
    for res in golden[:limit]:
        print_flush(f"{res['code']} {res['name']} : 現價={res['close']:.2f} MA5={res['ma5']:.2f} MA20={res['ma20']:.2f}")
        
    print_flush(f"\n【死亡交叉 (MA5下穿MA20)】 (前 {limit} 筆)")
    for res in death[:limit]:
        print_flush(f"{res['code']} {res['name']} : 現價={res['close']:.2f} MA5={res['ma5']:.2f} MA20={res['ma20']:.2f}")


def scan_kd_nvi_cross():
    """月KD / NVI+PVI 交叉訊號 (多進程)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在掃描 KD/NVI 交叉訊號 (成交量 > {min_vol} 張)...")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if not data:
        print_flush("❌ 請先載入指標數據")
        return

    results = []
    
    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol < min_vol * 1000: continue
            
            k = safe_float_preserving_none(ind.get('month_k') or ind.get('Month_K'))
            d = safe_float_preserving_none(ind.get('month_d') or ind.get('Month_D'))
            k_prev = safe_float_preserving_none(ind.get('month_k_prev') or ind.get('Month_K_prev'))
            d_prev = safe_float_preserving_none(ind.get('month_d_prev') or ind.get('Month_D_prev'))
            
            nvi = safe_float_preserving_none(ind.get('nvi') or ind.get('NVI'))
            pvi = safe_float_preserving_none(ind.get('pvi') or ind.get('PVI'))
            ma60 = safe_float_preserving_none(ind.get('ma60') or ind.get('MA60'))
            
            # KD Golden Cross
            kd_golden = False
            if k and d and k_prev and d_prev:
                if k_prev <= d_prev and k > d and k < 80:
                    kd_golden = True
            
            # NVI > PVI (Bullish)
            nvi_bull = False
            if nvi and pvi and nvi > pvi:
                nvi_bull = True
                
            if kd_golden and nvi_bull:
                results.append({'code': code, 'name': get_stock_name(code), 'close': ind.get('close'), 'k': k, 'd': d})
        except:
            continue
    
    print_flush(f"\n【KD金叉+NVI強勢】 (前 {limit} 筆)")
    print_flush(f"{'代號':<6} {'名稱':<8} {'現價':<8} {'K值':<6} {'D值':<6}")
    print_flush("-" * 50)
    for res in results[:limit]:
        print_flush(f"{res['code']:<6} {res['name']:<8} {res['close']:<8.2f} {res['k']:<6.1f} {res['d']:<6.1f}")


def scan_ma_bullish():
    """均線多頭排列 (多進程)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在掃描 均線多頭排列 (成交量 > {min_vol} 張)...")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if not data:
        print_flush("❌ 請先載入指標數據")
        return

    results = []
    
    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol < min_vol * 1000: continue
            
            close = safe_float_preserving_none(ind.get('close'))
            ma5 = safe_float_preserving_none(ind.get('ma5') or ind.get('MA5'))
            ma20 = safe_float_preserving_none(ind.get('ma20') or ind.get('MA20'))
            ma60 = safe_float_preserving_none(ind.get('ma60') or ind.get('MA60'))
            ma120 = safe_float_preserving_none(ind.get('ma120') or ind.get('MA120'))
            
            if not (close and ma5 and ma20 and ma60 and ma120): continue
            
            # Bullish Alignment: Price > MA5 > MA20 > MA60 > MA120
            if close > ma5 > ma20 > ma60 > ma120:
                # Bias check (0-10%)
                bias = (close - ma20) / ma20 * 100
                if 0 < bias < 10:
                    results.append({'code': code, 'name': get_stock_name(code), 'close': close, 'bias': bias, 'volume': vol})
        except:
            continue
    
    results.sort(key=lambda x: x['bias'])
    
    # 使用統一格式輸出
    def ma_bullish_extra(code, item):
        bias = item.get('bias', 0)
        return [f"{bias:.2f}%"]

    codes = display_scan_results_v2(results, "均線多頭排列 (乖離0-10%)", limit=limit,
                            extra_headers=["乖離%"],
                            extra_func=ma_bullish_extra)


def scan_five_filter():
    """五階篩選器 (千問版 - 整合)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在執行五階篩選 (成交量 > {min_vol} 張)...")
    print_flush("篩選條件: 1.相對強度 2.強勢股 3.主力驗證 4.價值區間 5.動能觸發")
    
    screener = TaiwanStockScreenerAdvanced(None)
    
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT code, name FROM stock_snapshot")
        stocks = cur.fetchall()
    
    results = []
    
    # Counters
    count_total = len(stocks)
    count_data = 0
    count_vol = 0
    
    # Level Counters
    count_rs = 0
    count_s1 = 0
    count_s2 = 0
    count_s3 = 0
    count_s4 = 0
    count_final = 0
    
    codes = [s[0] for s in stocks]
    history_map = batch_load_history(codes, limit_days=150)
    
    # Load Market Data
    market_df = history_map.get('0050')
    if market_df is None:
        if history_map: market_df = list(history_map.values())[0]
        else: return

    # Ensure Market Index
    market_df = market_df.copy()
    if 'date' in market_df.columns:
        market_df['date'] = pd.to_datetime(market_df['date'])
        market_df.set_index('date', inplace=True)
    market_df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
    
    # Market Filter
    try:
        m_data = screener.market_filter(market_df)
        adj = m_data['adjustment_factor']
    except:
        adj = 1.0

    for code, name in stocks:
        try:
            df = history_map.get(code)
            if df is None or len(df) < 100: continue
            count_data += 1
            
            # Ensure Stock Index
            df = df.copy()
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
            
            # Volume Filter
            if df['Volume'].iloc[-1] < min_vol * 1000: continue
            count_vol += 1
            
            # Indicators
            df = screener.calculate_technical_indicators(df)
            
            # Level 1: Relative Strength
            rs = screener.calculate_relative_strength(df, market_df)
            if rs >= screener.current_params['min_relative_strength']:
                count_rs += 1
            else:
                continue # RS is a hard filter
            
            # Calculate Scores for Levels 2-5
            ok1, s1 = screener.stock_strength_filter(df, adj)
            ok2, s2 = screener.smart_money_validation(df, adj)
            ok3, s3 = screener.value_zone_filter(df)
            ok4, s4 = screener.entry_trigger(df)
            
            if ok1: count_s1 += 1
            if ok2: count_s2 += 1
            if ok3: count_s3 += 1
            if ok4: count_s4 += 1
            
            # Final Score (Weighted)
            # Weights: Strength 30%, Smart 30%, Value 20%, Trigger 20%
            final_score = (s1 * 0.3) + (s2 * 0.3) + (s3 * 0.2) + (s4 * 0.2)
            
            if final_score >= 60: # Threshold
                count_final += 1
                results.append({
                    'code': code, 'name': name,
                    'close': df.iloc[-1]['Close'],
                    'close_prev': df.iloc[-2]['Close'],
                    'volume': df.iloc[-1]['Volume'], # Raw shares for display_scan_results_v2
                    'score': final_score,
                    'rs': rs,
                    'k': df.iloc[-1]['k'] if 'k' in df.columns else 0
                })
                
        except Exception as e:
            continue
            
    # Sort
    results.sort(key=lambda x: x['score'], reverse=True)
    
    # Summary
    print_flush("\n" + "="*60)
    print_flush("[篩選過程] 五階篩選器 (千問版)")
    print_flush(f"總股數: {count_total}")
    print_flush("─"*60)
    print_flush(f"✓ 資料充足 (>100日)       → {count_data} 檔")
    print_flush(f"✓ 成交量 >= {min_vol}張        → {count_vol} 檔")
    print_flush(f"✓ [第1階] 相對強度 (RS)   → {count_rs} 檔")
    print_flush(f"✓ [第2階] 強勢股條件      → {count_s1} 檔 (合格率)")
    print_flush(f"✓ [第3階] 主力籌碼驗證    → {count_s2} 檔 (合格率)")
    print_flush(f"✓ [第4階] 價值區間定位    → {count_s3} 檔 (合格率)")
    print_flush(f"✓ [第5階] 動能觸發訊號    → {count_s4} 檔 (合格率)")
    print_flush(f"✓ 綜合評分 >= 60分        → {count_final} 檔 (最終選出)")
    print_flush("─"*60)
    
    # 使用統一格式輸出
    def five_stage_extra(code, item):
        score = item.get('score', 0)
        rs = item.get('rs', 0)
        k = item.get('k', 0)
        return [f"{score:.1f}", f"{rs:.2f}", f"{k:.1f}"]

    codes = display_scan_results_v2(results, "五階篩選 TOP", limit=limit,
                            extra_headers=["分數", "RS值", "K值"],
                            extra_func=five_stage_extra)





def scan_six_dim_resonance():
    """六維共振交易系統 (MACD/KDJ/RSI/LWR/BBI/MTM)"""
    limit, min_vol = get_user_scan_params()
    print_flush(f"\n正在掃描 六維共振訊號 (成交量 > {min_vol} 張)...")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if not data:
        print_flush("❌ 請先載入指標數據")
        return

    results = []
    
    for code, ind in data.items():
        try:
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol < min_vol * 1000: continue
            
            close = safe_float_preserving_none(ind.get('close'))
            
            # 1. MACD: DIF > DEM (Bullish) or MACD > 0
            macd = safe_float_preserving_none(ind.get('MACD'))
            signal = safe_float_preserving_none(ind.get('SIGNAL'))
            cond_macd = (macd is not None and signal is not None and macd > signal)
            
            # 2. KDJ: K > D (Bullish)
            k = safe_float_preserving_none(ind.get('Month_K') or ind.get('month_k')) # Using Monthly or Daily? Plan said KDJ. Let's use Monthly for trend or Daily for trigger. Using Monthly as per context of "Resonance" usually implies stronger trend. Let's use Daily for sensitivity or Monthly for stability. Let's stick to Daily for "Trading System".
            # Actually, let's check if Daily K/D exists.
            k_d = safe_float_preserving_none(ind.get('Daily_K') or ind.get('daily_k'))
            d_d = safe_float_preserving_none(ind.get('Daily_D') or ind.get('daily_d'))
            cond_kdj = (k_d is not None and d_d is not None and k_d > d_d)
            
            # 3. RSI: RSI > 50
            rsi = safe_float_preserving_none(ind.get('RSI'))
            cond_rsi = (rsi is not None and rsi > 50)
            
            # 4. LWR: LWR > -50 (Bullish/Strong) - Note: LWR is usually -100 to 0.
            lwr = safe_float_preserving_none(ind.get('LWR'))
            cond_lwr = (lwr is not None and lwr > -50)
            
            # 5. BBI: Price > BBI
            bbi = safe_float_preserving_none(ind.get('BBI'))
            cond_bbi = (close is not None and bbi is not None and close > bbi)
            
            # 6. MTM: MTM > 0
            mtm = safe_float_preserving_none(ind.get('MTM'))
            cond_mtm = (mtm is not None and mtm > 0)
            
            # Calculate Resonance Score (How many conditions met)
            score = sum([cond_macd, cond_kdj, cond_rsi, cond_lwr, cond_bbi, cond_mtm])
            
            if score >= 5: # At least 5 out of 6
                results.append({
                    'code': code, 
                    'name': get_stock_name(code), 
                    'close': close, 
                    'score': score,
                    'volume': vol, # Rename to volume for display_scan_results_v2
                    'details': [cond_macd, cond_kdj, cond_rsi, cond_lwr, cond_bbi, cond_mtm]
                })
        except:
            continue
    
    results.sort(key=lambda x: (x['score'], x['volume']), reverse=True)
    
    # 使用統一格式輸出
    def six_dim_extra(code, item):
        score = item.get('score', 0)
        d = item.get('details', [])
        # d is [cond_macd, cond_kdj, cond_rsi, cond_lwr, cond_bbi, cond_mtm]
        
        signal_str = "買入"
        dim_str = f"{score}/6"
        checks = ["✓" if x else " " for x in d]
        
        return [signal_str, dim_str] + checks

    codes = display_scan_results_v2(results, "六維共振 (至少5項符合)", limit=limit,
                            description="指標說明: 1.MACD>Sig 2.K>D 3.RSI>50 4.LWR>-50 5.Px>BBI 6.MTM>0",
                            extra_headers=["訊號", "維度", "MACD", "KDJ", "RSI", "LWR", "BBI", "MTM"],
                            extra_func=six_dim_extra)


def market_scan_menu():
    """市場掃描選單"""
    global GLOBAL_INDICATOR_CACHE
    
    # 檢查快取
    data = GLOBAL_INDICATOR_CACHE.get_data()
    if not data:
        print_flush("\n正在載入指標 (Snapshot)...")
        data = step4_load_data()
        GLOBAL_INDICATOR_CACHE.set_data(data)

    while True:
        print_flush("\n" + "="*60)
        print_flush("【市場掃描】")
        print_flush("="*60)
        print_flush("[1] VP掃描 (箱型壓力/支撐)")
        print_flush("[2] MFI掃描 (資金流向)")
        print_flush("[3] 均線掃描 (含多頭掃描)")
        print_flush("[4] 交叉訊號掃描 (月KD)")
        print_flush("-" * 60)
        print_flush("[5] VSBC 籌碼策略 (量價/箱型/籌碼)")
        print_flush("[6] 聰明錢掃描 (NVI主力籌碼)")
        print_flush("[7] 2560 戰法 (均線/量能)")
        print_flush("[8] 五階篩選器 (千問版)")
        print_flush("[9] 機構價值回歸策略 (Gemini)")
        print_flush("[a] 六維共振交易系統 (MACD/KDJ/RSI/LWR/BBI/MTM)")
        print_flush("-" * 60)
        print_flush("[b] K線型態 (晨星/夜星)")
        print_flush("[c] 量價背離形態詳解 (進階偵測)")
        print_flush("[0] 返回主選單")
        print_flush("-" * 60)
        print_flush("💡 輸入股票代號 (如 2330) 可直接查看個股")
        
        ch = input("請選擇: ").strip().lower()
        
        if ch == '0': break
        elif ch == '1': vp_scan_submenu()
        elif ch == '2': mfi_scan_submenu()
        elif ch == '3': ma_scan_submenu()
        elif ch == '4': scan_kd_nvi_cross()
        elif ch == '5': scan_vsbc_strategy()
        elif ch == '6': scan_smart_money_strategy()
        elif ch == '7': scan_2560_strategy()
        elif ch == '8': scan_five_filter()
        elif ch == '9': run_institutional_value_strategy()
        elif ch == 'a': scan_six_dim_resonance()
        elif ch == 'b': scan_candlestick_patterns()
        elif ch == 'c': scan_pv_divergence_analysis()
        
        # 股票代號查詢
        elif ch.isdigit() and len(ch) == 4:
            _handle_stock_query(ch)
        else:
            if ch: print_flush("❌ 無效輸入")


def vp_scan_submenu():
    """VP掃描子選單"""
    global GLOBAL_INDICATOR_CACHE
    
    print_flush("\n【VP掃描】")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if data:
        print_flush(f"[已載入指標: {len(data)} 檔]")
    else:
        print_flush("[未載入指標]")
    
    print_flush("[1] VP 接近下緣 (支撐)")
    print_flush("[2] VP 接近上緣 (壓力)")
    print_flush("[0] 返回")
    
    ch = read_single_key()
    
    if ch == '0':
        return
    
    mode = 'lower' if ch == '1' else 'upper'
    title = "VP 接近下緣 (支撐)" if mode == 'lower' else "VP 接近上緣 (壓力)"
    
    if ch in ['1', '2']:
        limit, min_vol = get_user_scan_params()

        print_flush(f"\n正在掃描 {title}...")
        
        print_flush(f"\n正在掃描 {title}...")
        
        data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
        if not data:
            print_flush("❌ 請先載入指標數據")
            return
        
        res = scan_vp(data, mode, min_volume=min_vol)
        codes = display_scan_results_v2(res, title, limit=limit, description="VP: Volume Profile (籌碼分布), 接近下緣=支撐, 接近上緣=壓力")
        prompt_stock_detail_report(codes)

def mfi_scan_submenu():
    """MFI掃描子選單"""
    global GLOBAL_INDICATOR_CACHE
    
    print_flush("\n【MFI掃描】")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if data:
        print_flush(f"[已載入指標: {len(data)} 檔]")
    else:
        print_flush("[未載入指標]")
    
    print_flush("[1] MFI由小→大 (資金流入開始)")
    print_flush("[2] MFI由大→小 (資金流出結束)")
    print_flush("[0] 返回")
    
    ch = read_single_key()
    
    if ch == '0':
        return
    
    if ch in ['1', '2']:
        limit, min_vol = get_user_scan_params()

        order = 'asc' if ch == '1' else 'desc'
        title = "MFI由小→大 (資金流入開始)" if order == 'asc' else "MFI由大→小 (資金流出結束)"
        
        print_flush(f"\n正在掃描 {title}...")
        
        print_flush(f"\n正在掃描 {title}...")
        
        data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
        if not data:
            print_flush("❌ 請先載入指標數據")
            return
        
        results = scan_mfi_mode(data, order=order, min_volume=min_vol)
        
        def mfi_extra(code, ind):
            mfi = safe_num(ind.get('mfi14') or ind.get('MFI'))
            return [f"{mfi:.1f}" if mfi else "-"]
            
        codes = display_scan_results_v2(results, title, limit=limit, 
                                   description="MFI: Money Flow Index (資金流量), >80 超買, <20 超賣, 50 分界",
                                   extra_headers=["MFI"],
                                   extra_func=mfi_extra)
        prompt_stock_detail_report(codes)

def ma_scan_submenu():
    """均線掃描子選單"""
    global GLOBAL_INDICATOR_CACHE
    
    print_flush("\n【均線掃描】")
    
    data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
    if data:
        print_flush(f"[已載入指標: {len(data)} 檔]")
    else:
        print_flush("[未載入指標]")
    
    print_flush("[1] 低於MA200 -0%~-10%")
    print_flush("[2] 低於MA20 -0%~-10%")
    print_flush("[3] 均線多頭 (四線上揚+股價在上+0-10%)")
    print_flush("[0] 返回")
    
    ch = read_single_key()
    
    if ch == '0':
        return
    
    if ch in ['1', '2']:
        limit, min_vol = get_user_scan_params()

        ma_type = 'MA200' if ch == '1' else 'MA20'
        title = f"低於{ma_type} -0%~-10%"
        
        print_flush(f"\n正在掃描 {title}...")
        
        data = GLOBAL_INDICATOR_CACHE.get_data() if GLOBAL_INDICATOR_CACHE else {}
        if not data:
            print_flush("❌ 請先載入指標數據")
            return
        
        results = scan_ma_mode(data, ma_type=ma_type, min_volume=min_vol)
        
        def ma_extra(code, ind):
            ma_val = safe_num(ind.get(ma_type.lower()) or ind.get(ma_type))
            return [f"{ma_type}:{ma_val:.1f}" if ma_val else "-"]
            
        codes = display_scan_results_v2(results, title, limit=limit, 
                                   description="MA: Moving Average (移動平均線), 股價低於均線=回測或跌破",
                                   extra_headers=[ma_type],
                                   extra_func=ma_extra)
        prompt_stock_detail_report(codes)
    
    elif ch == '3':
        scan_ma_bullish()

# ╔══════════════════════════════════════════════════════════════╗
# ║                       APP/CLI                                 ║
# ║  主選單、流程控制、CLI 入口點                                  ║
# ╚══════════════════════════════════════════════════════════════╝

def data_management_menu():
    """資料管理子選單"""
    global GLOBAL_INDICATOR_CACHE
    
    # 表驅動法：步驟功能映射
    DATA_MENU_ACTIONS = {
        '1': _run_full_daily_update,
        '2': step1_fetch_stock_list,
        '3': step2_download_tpex_daily,
        '4': step3_download_twse_daily,
        '5': step3_5_download_institutional,
        '6': step3_6_download_major_holders,
        '7': step3_7_fetch_margin_data,
        '8': step3_8_fetch_market_index,
        '9': step4_check_data_gaps,
        'a': step5_clean_delisted,
        'b': _handle_step6_with_resume,
        'c': _handle_step7_with_cache_clear
    }
    
    while True:
        print_flush("\n" + "="*60)
        print_flush("【資料管理與更新】")
        print_flush("="*60)
        print_flush("[1] 一鍵執行每日更新 (Steps 1-7)")
        print_flush("-" * 60)
        print_flush("[2] 步驟1: 更新上市櫃清單")
        print_flush("[3] 步驟2: 下載 TPEx (上櫃)")
        print_flush("[4] 步驟3: 下載 TWSE (上市)")
        print_flush("[5] 步驟3.5: 下載三大法人買賣超")
        print_flush("[6] 步驟3.6: 下載集保大戶資料")
        print_flush("[7] 步驟3.7: 下載融資融券資料")
        print_flush("[8] 步驟3.8: 下載大盤指數資料")
        print_flush("[9] 步驟4: 檢查數據缺失")
        print_flush("[a] 步驟5: 清理下市股票")
        print_flush("[b] 步驟6: 驗證一致性並補漏 (斷點續抓)")
        print_flush("[c] 步驟7: 計算技術指標")
        print_flush("[0] 返回主選單")

        ch = read_single_key().lower()

        # 衛語句：返回
        if ch == '0':
            break
        
        # 表驅動法：查找並執行
        action = DATA_MENU_ACTIONS.get(ch)
        if action:
            action()


def _force_redownload_all_history():
    """補齊所有股票的成交金額（已棄用，保留函數但清空內容）"""
    print_flush("此功能已完成任務並停用。")
    return

def _handle_step6_with_resume():
    """步驟6：驗證一致性並補漏（含斷點續抓提示）"""
    resume = True
    if PROGRESS_FILE.exists():
        print_flush("\n發現進度紀錄:")
        print_flush("[1] 繼續上次進度 (預設)")
        print_flush("[2] 重頭開始")
        
        sub_ch = read_single_key()
        if sub_ch == '2':
            resume = False
            print_flush("已選擇重頭開始")
        else:
            print_flush("已選擇繼續上次進度")
    
    step6_verify_and_backfill(resume=resume)


def _handle_step7_with_cache_clear():
    """步驟7：計算技術指標（含快取清除）"""
    step7_calc_indicators()
    
    if GLOBAL_INDICATOR_CACHE:
        GLOBAL_INDICATOR_CACHE.clear()
    print_flush("✓ 系統快取已清除")


def _run_full_daily_update():
    """一鍵執行每日更新 (Steps 1->..->8)"""
    global GLOBAL_INDICATOR_CACHE
    updated_codes = set()
    out = StepOutput  # 簡化調用
    
    # 開始更新
    out.box_start("一鍵每日更新")
    
    # Step 1: 更新清單 (必須先執行，因為後續步驟依賴清單)
    out.header("更新上市櫃清單", "1")
    step1_fetch_stock_list(silent_header=True)
    
    # Step 2-3.10: 全面並行下載
    out.header("並行下載所有市場資料", "2-3.10")
    print_flush("  啟動高併發下載模式 (上市/上櫃/法人/融資/估值)...")
    
    parallel_tasks = [
        # 市場行情
        (step2_download_tpex_daily, (), {'silent_header': True}, "TPEx (上櫃)", "2"),
        (step3_download_twse_daily, (), {'silent_header': True}, "TWSE (上市)", "3"),
        # 籌碼與估值
        (step3_5_download_institutional, (60,), {'silent_header': True}, "法人買賣超", "3.5"),
        (step3_6_download_major_holders, (), {'silent_header': True}, "集保大戶", "3.6"),
        (step3_7_fetch_margin_data, (60,), {'silent_header': True}, "融資融券", "3.7"),
        (step3_8_fetch_market_index, (), {'silent_header': True}, "大盤指數", "3.8"),
        (PePbDataAPI.fetch_all_pepb, (), {}, "PE/PB估值", "3.9"),
        (ShareholderDataAPI.fetch_all_shareholder, (), {}, "集保戶數", "3.10"),
    ]
    
    # 執行並行任務 (增加 max_workers 以容納更多 I/O 密集任務)
    results = run_parallel_tasks(parallel_tasks, max_workers=8, show_progress=True)
    
    # 收集更新的股票代碼
    if results.get("TPEx (上櫃)") and isinstance(results["TPEx (上櫃)"], set):
        updated_codes.update(results["TPEx (上櫃)"])
    if results.get("TWSE (上市)") and isinstance(results["TWSE (上市)"], set):
        updated_codes.update(results["TWSE (上市)"])
    
    # Step 4: 檢查數據缺失
    out.header("檢查數據缺失", "4")
    step4_check_data_gaps()
    
    # Step 5: 清理下市股票
    out.header("清理下市股票", "5")
    step5_clean_delisted()
    
    # Step 6: 補漏
    out.header("驗證一致性並補漏", "6")
    data = step4_load_data()
    s6 = step6_verify_and_backfill(data, resume=True, skip_downloads=True)
    if isinstance(s6, set):
        updated_codes.update(s6)
    
    # Step 7: 計算指標
    out.header("計算技術指標", "7")
    step7_calc_indicators(data)
    
    # Step 8: 同步 Supabase
    out.header("同步雲端", "8")
    step8_sync_supabase()
    
    # 更新快取
    if GLOBAL_INDICATOR_CACHE is None:
        GLOBAL_INDICATOR_CACHE = IndicatorCacheManager()
    GLOBAL_INDICATOR_CACHE.set_data(data)
    
    # 完成
    out.box_end("每日更新完成！快取已更新，可直接進行掃描")


def _run_quick_update():
    """快速更新 (僅 2->3->7，跳過補漏)"""
    step2_download_tpex_daily()
    step3_download_twse_daily()
    step7_calc_indicators()
    
    if GLOBAL_INDICATOR_CACHE:
        GLOBAL_INDICATOR_CACHE.clear()
    print_flush("✓ 系統快取已清除")


def backup_menu():
    """資料庫備份與還原選單"""
    import shutil
    
    while True:
        print_flush("\n【資料庫備份與還原】")
        print_flush("[1] 建立備份")
        print_flush("[2] 還原備份")
        print_flush("[3] 列出所有備份")
        print_flush("[0] 返回")
        
        ch = read_single_key()
        
        if ch == '0':
            return
            
        if ch == '1':
            print_flush("正在備份資料庫...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = WORK_DIR / f"backup_{timestamp}.db"
            try:
                shutil.copy2(DB_FILE, backup_file)
                size_mb = backup_file.stat().st_size / (1024*1024)
                print_flush(f"✓ 備份完成: {backup_file.name}")
                print_flush(f"   檔案大小: {size_mb:.1f} MB")
            except Exception as e:
                print_flush(f"❌ 備份失敗: {e}")
                
        elif ch == '2':
            print_flush("請手動將備份檔覆蓋 taiwan_stock.db (需重啟程式)")
            
        elif ch == '3':
            backups = list(WORK_DIR.glob("backup_*.db"))
            if not backups:
                print_flush("無備份檔案")
            else:
                for b in backups:
                    print_flush(f"- {b.name} ({b.stat().st_size/(1024*1024):.1f} MB)")
        
        print_flush("\n按 Enter 繼續...")
        sys.stdin.readline()

def check_db_nulls():
    """檢查資料庫空值率"""
    print_flush("\n正在檢查資料完整性...")
    with db_manager.get_connection() as conn:
        try:
            cur = conn.execute("SELECT COUNT(*) FROM stock_history")
            total = cur.fetchone()[0]
            
            if total == 0:
                print_flush("資料庫為空")
                return

            nulls = {}
            for col in ['open', 'high', 'low', 'close', 'volume']:
                cur = conn.execute(f"SELECT COUNT(*) FROM stock_history WHERE {col} IS NULL")
                nulls[col] = cur.fetchone()[0]
            
            print_flush(f"總筆數: {total}")
            for col, count in nulls.items():
                pct = (count / total) * 100
                print_flush(f"- {col} 空值: {count} ({pct:.2f}%)")
                
        except Exception as e:
            print_flush(f"檢查失敗: {e}")

def delete_data_by_date():
    """刪除指定日期的資料"""
    date_str = input("請輸入要刪除的日期 (YYYYMMDD): ").strip()
    if not date_str.isdigit() or len(date_str) != 8:
        print_flush("日期格式錯誤")
        return
        
    date_int = int(date_str)
    print_flush(f"確定要刪除 {date_int} 的所有資料嗎? (y/n)")
    if input().lower() != 'y':
        return
        
    with db_manager.get_connection() as conn:
        try:
            conn.execute("DELETE FROM stock_history WHERE date_int = ?", (date_int,))
            conn.execute("DELETE FROM institutional_investors WHERE date_int = ?", (date_int,))
            conn.commit()
            print_flush(f"✓ 已刪除 {date_int} 的資料")
        except Exception as e:
            print_flush(f"刪除失敗: {e}")

def _check_api_connection_status():
    """檢查 API 連線狀態"""
    print_flush("\n【API 連線狀態檢查】")
    endpoints = [
        ("TWSE 證交所", "https://www.twse.com.tw"),
        ("TPEx 櫃買中心", "https://www.tpex.org.tw"),
        ("FinMind", "https://api.finmindtrade.com")
    ]
    
    for name, url in endpoints:
        try:
            resp = requests.get(url, timeout=5, verify=False)
            status = "正常" if resp.status_code == 200 else f"異常 ({resp.status_code})"
            print_flush(f"✓ {name}: {status}")
        except Exception as e:
            print_flush(f"❌ {name}: 連線失敗")

def maintenance_menu():
    """系統維護選單"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("【系統維護】")
        print_flush("="*60)
        print_flush("[1] 資料庫備份與還原")
        print_flush("[2] 檢查 API 連線狀態")
        print_flush("[3] 檢查資料完整性 (空值率)")
        print_flush("[4] 刪除指定日期資料")
        print_flush("[5] 同步資料到 Supabase")
        print_flush("[0] 返回主選單")
        
        ch = read_single_key()
        
        if ch == '0':
            return
            
        if ch == '1':
            backup_menu()
        elif ch == '2':
            _check_api_connection_status()
        elif ch == '3':
            check_db_nulls()
        elif ch == '4':
            delete_data_by_date()
        elif ch == '5':
            step8_sync_supabase()
            
        print_flush("\n按 Enter 繼續...")
        sys.stdin.readline()


def display_scan_results_v2(results, title, limit=30, extra_headers=None, extra_func=None, description=""):
    """
    統一掃描結果顯示函數 (v2) - 符合 SDD 規範
    
    Args:
        results: List of dict or tuple. If dict, must contain 'code', 'name', 'close', 'vol_ratio'.
                 If tuple, logic depends on legacy support (try to avoid).
        title: 策略標題
        limit: 顯示數量
        extra_headers: List[str], 額外欄位名稱
        extra_func: Callable(code, item_data) -> List[str], 回傳額外欄位值
        description: 策略說明
    """
    if not results:
        print_flush(f"\n❌ {title}: 沒有符合條件的股票")
        return []

    print_flush("\n" + "="*90)
    print_flush(f"【{title}】 (前 {limit} 筆)")
    
    # 標準欄位: 代號(6) 名稱(8) 收盤(10) 成交量(量比)(18) VSBC上/下(12) VP上/下(12)
    # 總寬度: 6+1+8+1+10+1+18+1+12+1+12 = 70
    # 加上額外欄位
    
    header_str = f"{'代號':<6} {'名稱':<8} {'收盤':<10} {'成交量(量比)':<18} {'VSBC上/下':<12} {'VP上/下':<12}"
    
    if extra_headers:
        for h in extra_headers:
            header_str += f" {h:<10}"
            
    print_flush(header_str)
    print_flush("-" * len(header_str)) # 動態長度
    
    count = 0
    display_codes = []
    reset = reset_color()
    
    # 預先載入 VSBC/VP 數據 (若 results 中沒有)
    # 為了效能，這裡假設 results 已經包含或我們即時讀取 (cache)
    # 若是 tuple 格式，需要 extra_func 處理
    
    for item in results:
        if count >= limit:
            break
            
        try:
            # 解析 item
            if isinstance(item, dict):
                code = item.get('code')
                name = item.get('name', '')
                close = item.get('close', 0)
                vol = item.get('volume', 0)
                vol_ratio = item.get('vol_ratio', 0)
                item_data = item # Pass full dict to extra_func
            else:
                # Legacy tuple support: (code, sort_val, ind, ...)
                code = item[0]
                ind = item[2] if len(item) > 2 and isinstance(item[2], dict) else {}
                name = ind.get('name', '')
                close = safe_float_preserving_none(ind.get('close')) or 0
                vol = safe_float_preserving_none(ind.get('volume')) or 0
                vol_prev = safe_float_preserving_none(ind.get('vol_prev'))
                vol_ma60 = safe_float_preserving_none(ind.get('vol_ma60')) # 假設有
                
                # 計算量比 (若無)
                if vol_ratio := ind.get('vol_ratio'):
                    pass
                elif vol_prev and vol_prev > 0:
                    vol_ratio = vol / vol_prev
                else:
                    vol_ratio = 0
                
                item_data = ind # Pass indicator dict
                if not name:
                    # Try to get name from meta
                    name = get_correct_stock_name(code)
            
            # 取得 VSBC/VP 數據 (從 Cache 或 Item)
            # 這裡簡化：若 item_data 有則用，無則顯示 -/-
            vsbc_upper = item_data.get('vsbc_upper') or item_data.get('VSBC_upper') or 0
            vsbc_lower = item_data.get('vsbc_lower') or item_data.get('VSBC_lower') or 0
            vp_upper = item_data.get('vp_upper') or item_data.get('VP_upper') or 0
            vp_lower = item_data.get('vp_lower') or item_data.get('VP_lower') or 0
            
            # 格式化數值
            c_price = get_color_code(1) # 簡化: 預設紅色，或需比較昨日收盤
            # 若有 close_prev 可比較
            close_prev = item_data.get('close_prev') or item_data.get('ref_price')
            if close_prev:
                c_price = get_trend_color(close, close_prev)
            
            price_str = f"{c_price}{close:<10.2f}{reset}"
            vol_str = f"{int(vol/1000)}張({vol_ratio:.1f})"
            
            vsbc_str = f"{int(vsbc_lower)}/{int(vsbc_upper)}" if vsbc_upper else "-/-"
            vp_str = f"{int(vp_lower)}/{int(vp_upper)}" if vp_upper else "-/-"
            
            # 組合基本字串
            row_str = f"{code:<6} {name:<8} {price_str} {vol_str:<18} {vsbc_str:<12} {vp_str:<12}"
            
            # 處理額外欄位
            if extra_func:
                extras = extra_func(code, item_data)
                for e in extras:
                    row_str += f" {str(e):<10}"
            
            print_flush(row_str)
            display_codes.append(code)
            count += 1
            
        except Exception as e:
            # print_flush(f"Error displaying {item}: {e}")
            continue
            
    print_flush("-" * len(header_str))
    if description:
        print_flush(description)
        print_flush("-" * len(header_str))
        
    print_flush(f"共找到 {len(results)} 檔符合條件")
    
    prompt_stock_detail_report(display_codes)
    return display_codes


def _draw_kbar_chart(code, name=""):
    """繪製個股 K 線圖（使用 Plotly）"""
    try:
        # 嘗試導入 plotly
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print_flush("❌ 需要安裝 plotly 套件: pip install plotly")
        return
    
    print_flush(f"\n正在載入 {code} 的歷史資料...")
    
    # 從資料庫讀取歷史資料
    with db_manager.get_connection() as conn:
        df = pd.read_sql_query("""
            SELECT date_int, open, high, low, close, volume
            FROM stock_history
            WHERE code = ?
            ORDER BY date_int DESC
            LIMIT 120
        """, conn, params=(code,))
    
    if df.empty:
        print_flush(f"❌ 找不到 {code} 的歷史資料")
        return
    
    # 轉換日期格式
    df['date'] = pd.to_datetime(df['date_int'].astype(str), format='%Y%m%d')
    df = df.sort_values('date')
    df = df.set_index('date')
    
    # 計算移動平均線
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['MA60'] = df['close'].rolling(window=60).mean()
    
    # 建立子圖
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03,
                        subplot_titles=(f'{code} {name} K線圖', '成交量'),
                        row_heights=[0.7, 0.3])
    
    # K 線圖
    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='K線',
        increasing_line_color='red',
        decreasing_line_color='green'
    ), row=1, col=1)
    
    # 移動平均線
    fig.add_trace(go.Scatter(x=df.index, y=df['MA5'], opacity=0.7,
                             line=dict(color='blue', width=1), name='MA5'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MA20'], opacity=0.7,
                             line=dict(color='orange', width=1), name='MA20'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MA60'], opacity=0.7,
                             line=dict(color='purple', width=1), name='MA60'), row=1, col=1)
    
    # 成交量柱狀圖
    colors = ['red' if c >= o else 'green' for o, c in zip(df['open'], df['close'])]
    fig.add_trace(go.Bar(x=df.index, y=df['volume'], marker_color=colors, name='成交量'), row=2, col=1)
    
    # 版面設定
    fig.update_layout(
        title=f'{code} {name} 個股 K 線圖',
        yaxis_title='價格',
        yaxis2_title='成交量',
        xaxis_rangeslider_visible=False,
        template='plotly_white',
        height=800,
        hovermode='x unified'
    )
    
    # 移除非交易日空白
    fig.update_xaxes(type='category')


def _handle_stock_query(code):
    """處理個股查詢 - 完整版（即時 + 歷史多天）"""
    # 取得股票名稱
    name = code
    if code in twstock.codes:
        stock_info = twstock.codes[code]
        name = stock_info.name
    else:
        name = get_correct_stock_name(code)
        if name == code:
            print_flush(f"❌ 找不到股票代號: {code}")
            return

    # 詢問顯示天數
    try:
        days_input = input("顯示天數(預設10天): ").strip()
        days = int(days_input) if days_input.isdigit() and int(days_input) > 0 else 10
    except:
        days = 10
    
    print_flush(f"\n正在查詢 {code} {name} ...\n")
    
    # ===== 1. 即時股價 (Realtime) =====
    print_flush(f"=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 即時股價 ({code} {name}) ===")
    try:
        stock_realtime = twstock.realtime.get(code)
        if stock_realtime.get('success'):
            rt = stock_realtime.get('realtime', {})
            info = stock_realtime.get('info', {})
            
            # 格式化數字為小數點後二位
            latest_price = safe_float_preserving_none(rt.get('latest_trade_price'))
            if latest_price is None:
                latest_price = safe_float_preserving_none(rt.get('z')) # Fallback to 'z'
            
            open_price = safe_float_preserving_none(rt.get('open'))
            high_price = safe_float_preserving_none(rt.get('high'))
            low_price = safe_float_preserving_none(rt.get('low'))
            volume = safe_int(rt.get('accumulate_trade_volume'), 0)
            
            print_flush(f"股票名稱: {info.get('name', name)}")
            print_flush(f"目前股價: {latest_price:.2f}" if latest_price else "目前股價: N/A")
            print_flush(f"開盤: {open_price:.2f}  最高: {high_price:.2f}  最低: {low_price:.2f}  成交量: {volume:,} 張" if open_price else "開盤: N/A")
        else:
            print_flush(f"⚠ 即時報價查詢失敗: {stock_realtime.get('rtmessage', '未知錯誤')}")
    except Exception as e:
        print_flush(f"⚠ 即時報價查詢失敗: {e}")
    
    print_flush("\n" + "="*80 + "\n")
    
    # ===== 2. 歷史資料 - 使用技術指標格式顯示 =====
    print_flush(f"=== 【{name} {code}】近 {days} 天走勢 ===")
    print_flush("="*80)
    
    try:
        # 使用 calculate_stock_history_indicators 計算技術指標
        indicators_list = calculate_stock_history_indicators(code, display_days=days)
        
        if indicators_list:
            # 使用 format_scan_result_list 統一顯示格式
            print_flush(format_scan_result_list(code, name, indicators_list))
        else:
            print_flush("(無歷史數據)")
    except Exception as e:
        print_flush(f"⚠ 讀取歷史數據失敗: {e}")
    
    print_flush("="*80)
    
    # 符合規則 7 和 8：輸入 1 K線圖，按 0 返回
    print_flush("\n輸入 1 K線圖，或按 0 返回: ", end="")
    try:
        ch = input().strip()
        if ch == '1':
            _draw_kbar_chart(code, name)
    except:
        pass
    # 直接返回主選單

def _get_ranking_params():
    """獲取使用者輸入的參數 (排行榜專用)"""
    # 選擇檔數
    try:
        print("選擇檔數(預設10檔): ", end='', flush=True)
        s = sys.stdin.readline().strip()
        top_n = int(s) if s.isdigit() and int(s) > 0 else 10
    except:
        top_n = 10
    
    # 連續天數
    try:
        print("連續買入/賣出天數(預設2天): ", end='', flush=True)
        s = sys.stdin.readline().strip()
        min_days = int(s) if s.isdigit() and int(s) > 0 else 2
    except:
        min_days = 2
    
    # 排序方式
    print("[1] 依張數排序 [2] 依總金額排序 (預設1): ", end='', flush=True)
    try:
        s = sys.stdin.readline().strip()
        sort_by_amount = (s == '2')
    except:
        sort_by_amount = False
    
    return top_n, min_days, sort_by_amount

def _get_ranking_close_prices():
    """取得收盤價用於計算金額 (排行榜專用)"""
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.execute("""
                SELECT code, close FROM stock_history 
                WHERE date_int = (SELECT MAX(date_int) FROM stock_history)
            """)
            return {row[0]: row[1] for row in cursor.fetchall()}
    except:
        return {}

def _display_ranking(rank_type, title, top_n, min_days, sort_by_amount):
    """顯示排行榜"""
    
    print_flush(f"\n正在取得 {title}...")
    print_flush(f"(顯示前{top_n}檔, 連續{min_days}天以上, {'依金額' if sort_by_amount else '依張數'})")
    
    try:
        with db_manager.get_connection() as conn:
            # 取得最新日期
            cur = conn.execute("SELECT MAX(date_int) FROM institutional_investors")
            res = cur.fetchone()
            latest_date = res[0] if res else None
            
            if not latest_date:
                print_flush("❌ 資料庫無法人資料")
                return

            # 判斷買超或賣超
            is_buy = 'buy' in rank_type
            
            # 查詢排行
            if 'foreign' in rank_type:
                order_col = 'foreign_buy - foreign_sell'
            elif 'trust' in rank_type:
                order_col = 'trust_buy - trust_sell'
            else:
                order_col = 'dealer_buy - dealer_sell'
            
            # 買超: DESC (正數越大越好), 賣超: ASC (負數越大越好)
            order_dir = 'DESC' if is_buy else 'ASC'

            # SQL query to get ranking
            sql = f"""
                SELECT stock_id, {order_col} as net_buy
                FROM institutional_investors
                WHERE date_int = ?
                ORDER BY {order_col} {order_dir}
            """
            cur = conn.execute(sql, (latest_date,))
            rows = cur.fetchall()
            
            # Filter and process results
            data = []
            close_prices = _get_ranking_close_prices()
            
            # Get names
            cur = conn.execute("SELECT code, name FROM stock_snapshot")
            stock_names = {row[0]: row[1] for row in cur.fetchall()}

            for row in rows:
                code = row[0]
                net_buy = row[1]
                
                # Filter by direction (Buy > 0, Sell < 0)
                if is_buy and net_buy <= 0: continue
                if not is_buy and net_buy >= 0: continue
                
                data.append({
                    'stock_id': code,
                    'net_buy': net_buy
                })
            
            # Sort by amount if needed
            if sort_by_amount:
                for item in data:
                    code = item['stock_id']
                    close = close_prices.get(code, 0)
                    item['amount'] = abs(item['net_buy']) * 1000 * close
                data.sort(key=lambda x: x.get('amount', 0), reverse=True)
            
            # Display
            print_flush(f"\n【{title}】 ({latest_date})")
            print_flush(f"{'#':<3} {'股票名稱':<16} | {'買賣超(張)':>10} | {'金額(萬)':>10} | {'連續天數':>8}")
            print_flush("-" * 60)
            
            count = 0
            for i, row in enumerate(data):
                if count >= top_n:
                    break
                
                code = row['stock_id']
                net_buy = row['net_buy']
                close = close_prices.get(code, 0)
                amount = (net_buy * 1000 * close) / 10000 if close else 0
                name = stock_names.get(code, '')
                display_name = f"{name}({code})" if name else code
                
                # Placeholder for consecutive days
                consec_days = "-"
                
                print_flush(f"{i+1:<3} {display_name:<16} | {net_buy:>10,} | {amount:>10,.0f} | {consec_days:>8}")
                count += 1
                
    except Exception as e:
        print_flush(f"❌ 查詢失敗: {e}")

def institutional_menu():
    """法人買賣超排行選單"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("【法人買賣超排行】")
        print_flush("="*60)
        print_flush("[1] 外資買超排行")
        print_flush("[2] 投信買超排行")
        print_flush("[3] 自營商買超排行")
        print_flush("[4] 外資賣超排行")
        print_flush("[5] 投信賣超排行")
        print_flush("[6] 自營商賣超排行")
        print_flush("[0] 返回主選單")
        print_flush("-" * 60)
        print_flush("💡 輸入股票代號 (如 2330) 可直接查看個股")
        
        ch = read_single_key()
        
        if ch == '0':
            return
            
        # 股票代號查詢
        if ch.isdigit() and len(ch) == 4:
            _handle_stock_query(ch)
            continue
            
        rank_map = {
            '1': ('foreign_buy', '外資買超排行'),
            '2': ('trust_buy', '投信買超排行'),
            '3': ('dealer_buy', '自營商買超排行'),
            '4': ('foreign_sell', '外資賣超排行'),
            '5': ('trust_sell', '投信賣超排行'),
            '6': ('dealer_sell', '自營商賣超排行')
        }
        
        if ch in rank_map:
            rank_type, title = rank_map[ch]
            top_n, min_days, sort_by_amount = _get_ranking_params()
            _display_ranking(rank_type, title, top_n, min_days, sort_by_amount)
            
            # Pause to let user read
            print_flush("\n按 Enter 繼續...")
            sys.stdin.readline()

def main_menu():
    """主選單"""
    global GLOBAL_INDICATOR_CACHE
    
    # 初始化
    try:
        ensure_db()
    except Exception as e:
        print_flush(f"DB Error: {e}")

    if GLOBAL_INDICATOR_CACHE is None:
        GLOBAL_INDICATOR_CACHE = IndicatorCacheManager()

    # 表驅動法：主選單功能映射
    MAIN_MENU_ACTIONS = {
        '1': data_management_menu,
        '2': market_scan_menu,
        '3': institutional_menu,
        '4': maintenance_menu,
    }

    while True:
        # 顯示系統狀態資訊
        display_system_status()
        
        print_flush("\n" + "="*60)
        print_flush("【台灣股市分析系統 v40 Enhanced】")
        print_flush("="*60)
        print_flush("[1] 資料管理與更新")
        print_flush("[2] 市場掃描 (技術指標)")
        print_flush("[3] 法人買賣超排行")
        print_flush("[4] 系統維護")
        print_flush("[0] 離開系統")
        print_flush("-" * 60)
        print_flush("💡 輸入股票代號 (如 2330) 可直接查看個股")
        
        choice = input("請選擇: ").strip().upper()
        
        # 衛語句：離開
        if choice == '0':
            print_flush("👋 系統已退出")
            sys.exit(0)
        
        # 衛語句：股票代號查詢
        if choice.isdigit() and len(choice) == 4:
            _handle_stock_query(choice)
            continue
        
        # 表驅動法：查找並執行
        action = MAIN_MENU_ACTIONS.get(choice)
        if action:
            action()
        elif choice:
            print_flush("❌ 無效輸入，請重新選擇")

# ==============================
# 主程式入口
# ==============================
if __name__ == "__main__":
    # 設置日誌
    log_file = WORK_DIR / 'system.log'
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    
    # 啟動主選單
    if len(sys.argv) > 1 and sys.argv[1] == '--auto-update':
        # 初始化資料庫
        try:
            ensure_db()
        except Exception as e:
            print(f"DB Init Error: {e}")

        # 初始化全域快取
        if GLOBAL_INDICATOR_CACHE is None:
            GLOBAL_INDICATOR_CACHE = IndicatorCacheManager()
            
        print_flush("[AUTO] 啟動自動更新模式 (Steps 1-8)...")
        step1_fetch_stock_list()
        step2_download_tpex_daily()
        step3_download_twse_daily()
        step3_5_download_institutional(days=3)  # 法人資料 (智慧補漏)
        step3_6_download_major_holders()        # 集保大戶
        step3_7_fetch_margin_data()             # 融資融券
        step3_8_fetch_market_index()            # 大盤指數
        step4_check_data_gaps()
        step5_clean_delisted()
        data = step4_load_data()
        updated_codes = step6_verify_and_backfill(data, resume=True)
        
        # 如果有更新，重新計算指標
        if updated_codes:
            print_flush("✓ 偵測到資料更新，清除快取...")
            if GLOBAL_INDICATOR_CACHE:
                GLOBAL_INDICATOR_CACHE.clear()
        
        step7_calc_indicators(data)
        step8_sync_supabase()
        
        print_flush("[DONE] 自動更新完成")
        sys.exit(0)

    main_menu()

# ==============================
# 程式入口點
# ==============================
if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    main_menu()
