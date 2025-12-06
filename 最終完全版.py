#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台灣股票分析系統 v40 (Fast Timeout Edition)
架構師:資深軟體架構師
修正項目:
1. [role] 規則嚴格遵守（繁體中文、A規則、三行進度、斷檔續讀、資料顯示方式、使用官方的真實數據抓到什麼就輸出什麼，不要有按任意鍵返回/繼續，一律直接進入選單或顯示)
"""
import os
import sys
import time
import json
import sqlite3
import logging
import requests
import threading
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
# import twstock (Removed)

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
try:
    requests.packages.urllib3.disable_warnings()
    # Monkeypatch requests.get to always use verify=False (Fix for twstock SSL error)
    _original_get = requests.get
    def _patched_get(*args, **kwargs):
        kwargs['verify'] = False
        return _original_get(*args, **kwargs)
    requests.get = _patched_get
except Exception:
    pass

if os.name == 'nt':
    try:
        import ctypes
        import msvcrt
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# ==============================
# 配置參數
# ==============================
def get_work_directory():
    if os.name == 'nt':
        return Path(__file__).parent.absolute()
    android_path = Path('/sdcard/Download/stock_app')
    if android_path.exists() or Path('/sdcard').exists():
        android_path.mkdir(parents=True, exist_ok=True)
        return android_path
    return Path(__file__).parent.absolute()

WORK_DIR = get_work_directory()
# [Architectural Fix] 環境感知: 檢測是否為 Android 環境
# Android 的 /sdcard 通常不支援 WAL 模式所需的 mmap
IS_ANDROID = '/sdcard' in str(WORK_DIR) or os.path.exists('/data/data/com.termux')

if not WORK_DIR.exists():
    WORK_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = WORK_DIR / 'taiwan_stock.db'
STOCK_LIST_PATH = WORK_DIR / 'stock_list.csv'
PROGRESS_FILE = WORK_DIR / 'download_progress.json'
BACKUP_DIR = WORK_DIR / 'backups'
BACKUP_DIR.mkdir(exist_ok=True)
REQUEST_TIMEOUT = 30

# API 設定
# [Config] FinMind API Token (User Provided)
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMi0wMiAyMDo0MzozMyIsInVzZXJfaWQiOiJ5dW5ndGFuZyAiLCJpcCI6IjIyMy4xMzYuNzguMzQifQ.-dsoTH27eOx4akAmKmHfoEso5g5EMZ-UXTcq59l2_Ds"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
TWSE_BWIBBU_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TWSE_STOCK_DAY_ALL_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"
TPEX_MAINBOARD_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
TPEX_DAILY_TRADING_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

# 全域快取
GLOBAL_INDICATOR_CACHE = {
    "data": None,
    "timestamp": None,
    "cache_duration": 3600
}

COFFEE_COLOR = '\033[38;5;130m'

# 雲端同步設定
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_publishable_yXSGYxyxPMaoVu4MbGK5Vw_IuZsl5yu"
ENABLE_CLOUD_SYNC = bool(SUPABASE_URL and SUPABASE_KEY)

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
            # 取得最後更新日期
            res = conn.execute("SELECT MAX(date) FROM stock_data").fetchone()
            if res and res[0]:
                status_info['last_update'] = res[0]
            
            # 取得總股票數
            res = conn.execute("SELECT COUNT(DISTINCT code) FROM stock_data").fetchone()
            status_info['total_stocks'] = res[0] if res else 0
            
            # 取得符合 A 規則的股票數 (優化: 改為只查最新日期的股票，避免全表掃描)
            latest_date = status_info['last_update']
            if latest_date != '無資料':
                res = conn.execute("SELECT code, name FROM stock_data WHERE date=?", (latest_date,)).fetchall()
                status_info['a_rule_stocks'] = sum(1 for row in res if is_normal_stock(row[0], row[1]))
            else:
                status_info['a_rule_stocks'] = 0
            
            # 取得日期範圍
            res = conn.execute("SELECT MIN(date), MAX(date) FROM stock_data").fetchone()
            if res:
                min_date = res[0] if res[0] != 'None' else 'N/A'
                max_date = res[1] if res[1] != 'None' else 'N/A'
                status_info['date_range'] = (min_date or 'N/A', max_date or 'N/A')
    
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
        url = f"{FINMIND_URL}?dataset=TaiwanStockPrice&stock_id=2330&date=2024-01-01&token={FINMIND_TOKEN}"
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
    
    # [優化] 跳過 API 檢查以加快啟動速度
    print_flush("-" * 80)
    print_flush("🚀 系統已就緒 (已略過 API 連線檢查)")
    print_flush("=" * 80)

# ... (skip to main_menu)

def main_menu():
    # 顯示系統狀態 (已優化，不檢查 API)
    display_system_status()
    
    while True:
        print_flush("\n【台股分析系統 v4.0】")
        print_flush(f"  {sup_icon} Supabase: {sup_status}")
    else:
        print_flush(f"  ⚪ Supabase: 未啟用")
    
    print_flush("=" * 80)

# ==============================
# 基礎設施層
# ==============================
class DatabaseManager:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    @contextmanager
    def get_connection(self, timeout=30):
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE, timeout=timeout)
            # Android 環境下不使用 WAL 模式，避免文件鎖定問題
            if not IS_ANDROID:
                conn.execute("PRAGMA journal_mode=WAL;")
            else:
                conn.execute("PRAGMA journal_mode=DELETE;")
            yield conn
        except sqlite3.Error as e:
            print_flush(f"❌ 資料庫錯誤: {e}")
            raise
        finally:
            if conn: conn.close()

db_manager = DatabaseManager()

class ProgressTracker:
    """
    強健版進度追蹤器 (Architectural Enhanced)
    1. Windows VT100 支援 (解決亂碼/捲動問題)
    2. 線程安全 (解決並發輸出衝突)
    3. 自動限流 (解決 IO 阻塞與效能問題)
    """
    _lock = threading.Lock()
    _last_update_time = 0
    _UPDATE_INTERVAL = 0.1  # 限制最大刷新率為 10 FPS
    
    def __init__(self, total_lines=3):
        self.total_lines = total_lines
        self._initialized = False
        self._enable_windows_vt()
        
    def _enable_windows_vt(self):
        """啟用 Windows 虛擬終端處理 (Virtual Terminal Processing)"""
        if os.name == 'nt':
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                hOut = kernel32.GetStdHandle(-11)
                out_mode = ctypes.c_ulong()
                kernel32.GetConsoleMode(hOut, ctypes.byref(out_mode))
                ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
                kernel32.SetConsoleMode(hOut, out_mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
            except Exception:
                pass # 如果失敗，退回標準模式 (可能會有亂碼，但至少不崩潰)

    def __enter__(self):
        self.reset()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 確保最後一次更新被顯示，並換行
        sys.stdout.write('\n' * self.total_lines)
        sys.stdout.flush()

    def update_lines(self, *messages, force=False):
        """
        更新多行進度
        :param messages: 要顯示的訊息行
        :param force: 是否強制更新 (忽略限流)
        """
        current_time = time.time()
        if not force and (current_time - self._last_update_time < self._UPDATE_INTERVAL):
            return

        with self._lock:
            # 準備內容，不足補空行
            lines = list(messages) + [""] * (self.total_lines - len(messages))
            lines = lines[:self.total_lines]
            
            # 游標控制
            if self._initialized:
                # 上移 N 行
                sys.stdout.write(f'\033[{self.total_lines}A')
            
            for line in lines:
                # 清除整行 (2K) -> 移到行首 (\r) -> 輸出內容 -> 換行 (\n)
                # 注意: 最後一行不應該換行，否則會多出一行空行，但為了簡單起見，我們這裡控制游標
                # 更好的做法是: 印出內容，然後游標自動會到下一行
                sys.stdout.write(f'\033[2K\r{line}\n')
            
            sys.stdout.flush()
            self._initialized = True
            self._last_update_time = current_time

    def reset(self):
        self._initialized = False

    def info(self, message, level=1):
        """兼容性接口: 顯示一般訊息"""
        # 這裡簡化處理，將訊息顯示在指定行
        # level 1 -> 第一行, level 2 -> 第二行, level 3 -> 第三行
        # 但 update_lines 是同時更新多行，這裡我們需要維護一個內部狀態來保存各行內容
        self._update_single_line(message, level)

    def warning(self, message, level=1):
        """兼容性接口: 顯示警告訊息"""
        self._update_single_line(f"⚠ {message}", level)

    def success(self, message, level=1):
        """兼容性接口: 顯示成功訊息"""
        self._update_single_line(f"✓ {message}", level)

    def error(self, message, level=1):
        """兼容性接口: 顯示錯誤訊息"""
        self._update_single_line(f"❌ {message}", level)

    _lines_buffer = ["", "", ""]

    def _update_single_line(self, message, level):
        """更新單行內容並刷新顯示"""
        idx = max(0, min(level - 1, 2)) # 限制在 0-2 之間
        self._lines_buffer[idx] = message
        self.update_lines(*self._lines_buffer)


# ==============================
# 進度追蹤與資料層 (從 v34 補充)
# ==============================

# 進度追蹤函數
def load_progress():
    """載入進度追蹤系統"""
    try:
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
                # 確保所有必要的鍵存在
                if "last_code_index" not in progress:
                    progress["last_code_index"] = 0
                if "missing_stocks" not in progress:
                    progress["missing_stocks"] = []
                if "new_stocks" not in progress:
                    progress["new_stocks"] = []
                if "failed_stocks" not in progress:
                    progress["failed_stocks"] = []
                if "in_progress" not in progress:
                    progress["in_progress"] = None
                # 新增: stock_list 進度
                if "stock_list_last_idx" not in progress:
                    progress["stock_list_last_idx"] = 0
                if "stock_list_processed" not in progress:
                    progress["stock_list_processed"] = []
                # 新增: batch_calculate 進度
                if "calc_last_idx" not in progress:
                    progress["calc_last_idx"] = 0
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
    """儲存進度追蹤系統 - 擴展版本支援多種進度類型"""
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

def reset_progress():
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception as e:
        print_flush(f"⚠ 無法重置進度檔: {e}")

def ensure_db():
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        cur = conn.cursor()
        
        # 1. 建立基本表結構
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                code TEXT,
                name TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                close_prev REAL,
                vol_prev INTEGER,
                PRIMARY KEY (code, date)
            )
        """)
        
        # 2. 檢查並添加缺失的欄位
        columns_to_add = [
            # 基礎指標
            ("ma3", "REAL"), ("ma20", "REAL"), ("ma60", "REAL"), ("ma120", "REAL"), ("ma200", "REAL"),
            ("wma3", "REAL"), ("wma20", "REAL"), ("wma60", "REAL"), ("wma120", "REAL"), ("wma200", "REAL"),
            ("mfi14", "REAL"), ("vwap20", "REAL"), ("chg14_pct", "REAL"), 
            ("rsi", "REAL"), ("macd", "REAL"), ("signal", "REAL"),
            ("vp_poc", "REAL"), ("vp_upper", "REAL"), ("vp_lower", "REAL"),
            ("month_k", "REAL"), ("month_d", "REAL"), # 月KD
            ("daily_k", "REAL"), ("daily_d", "REAL"), # 日KD
            ("week_k", "REAL"), ("week_d", "REAL"),   # 周KD
            # 前日指標 (用於趨勢判斷)
            ("ma3_prev", "REAL"), ("ma20_prev", "REAL"), ("ma60_prev", "REAL"), ("ma120_prev", "REAL"), ("ma200_prev", "REAL"),
            ("wma3_prev", "REAL"), ("wma20_prev", "REAL"), ("wma60_prev", "REAL"), ("wma120_prev", "REAL"), ("wma200_prev", "REAL"),
            ("mfi14_prev", "REAL"), ("vwap20_prev", "REAL"), ("chg14_pct_prev", "REAL"),
            ("month_k_prev", "REAL"), ("month_d_prev", "REAL"), # 月KD前值
            ("daily_k_prev", "REAL"), ("daily_d_prev", "REAL"), # 日KD前值
            ("week_k_prev", "REAL"), ("week_d_prev", "REAL")    # 周KD前值
        ]
        
        # 獲取現有欄位
        cur.execute("PRAGMA table_info(stock_data)")
        existing_columns = {row[1] for row in cur.fetchall()}
        
        for col_name, col_type in columns_to_add:
            if col_name not in existing_columns:
                try:
                    # print_flush(f"正在添加欄位: {col_name}...")
                    cur.execute(f"ALTER TABLE stock_data ADD COLUMN {col_name} {col_type}")
                except Exception as e:
                    print_flush(f"⚠ 添加欄位 {col_name} 失敗: {e}")
                    
        # 3. 建立索引
        cur.execute("CREATE INDEX IF NOT EXISTS idx_code_date ON stock_data(code, date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_date ON stock_data(date)")
        
        conn.commit()

# get_latest_date_for_code
def get_latest_date_for_code(code):
    try:
        with sqlite3.connect(DB_FILE, timeout=30) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM stock_data WHERE code=?", (code,))
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        print_flush(f"⚠ 獲取最新日期失敗 {code}: {e}")
        return None


def safe_num(x):
    try:
        if x is None: return None
        return float(str(x).replace(',', ''))
    except:
        return None

def safe_int(x):
    try:
        if x is None: return None
        # 處理 bytes 類型（SQLite INTEGER 可能回傳 bytes）
        if isinstance(x, bytes):
            return int.from_bytes(x, byteorder='little', signed=True)
        return int(float(str(x).replace(',', '')))
    except:
        return None

def safe_json_parse(text):
    try:
        return json.loads(text)
    except:
        return None

# DataSource 架構
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
        self.silent = silent  # 靜默模式，不輸出詳細進度
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """從FinMind取得歷史資料 - 優化版本，只取250個交易日"""
        try:
            # 如果沒有指定開始日期，計算250個交易日所需的時間（約1年）
            if start_date is None:
                # 250個交易日約等於365天
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
                        return None # 直接放棄，讓 Manager 切換
                    if response.status_code != 200:
                        logging.error(f"{self.name}: 狀態碼 {response.status_code} - {response.text}") # Debug
                        if not self.silent:
                            self.progress.warning(f"{self.name}: 狀態碼 {response.status_code}", 1)
                        if attempt < retry - 1:
                            time.sleep(1)
                        continue
                    try:
                        data = json.loads(response.text)
                    except Exception as e:
                        data = None
                    
                    if data is None or data.get('status') != 200:
                        logging.error(f"{self.name}: API 響應無效 - {response.text[:100]}") # Debug
                        if not self.silent:
                            self.progress.warning(f"{self.name}: API 響應無效", 1)
                        if attempt < retry - 1:
                            time.sleep(1)
                        continue
                    if not data.get('data') or len(data['data']) == 0:
                        logging.warning(f"{self.name}: {stock_code} 無數據") # Debug
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
                        except Exception as e:
                            if not self.silent:
                                self.progress.warning(f"處理數據時出錯: {str(e)}", 2)
                            continue
                    
                    if not rows:
                        return None
                    
                    df = pd.DataFrame(rows)
                    
                    # 按日期去重: 保留第一條（最完整的數據）
                    df = df.drop_duplicates(subset=['date'], keep='first')
                    
                    # 驗證資料完整性: 檢查是否有 NaN 或 0 的核心字段
                    df = df[df['close'] > 0]
                    
                    # 按日期排序
                    df = df.sort_values('date').reset_index(drop=True)
                    
                    # 按日期排序
                    df = df.sort_values('date').reset_index(drop=True)
                    
                    if not self.silent:
                        self.progress.success(f"{self.name}: 獲取 {len(df)} 筆 {stock_code} 數據 (去重後)", 1)
                    return df
                except Exception as e:
                    logging.error(f"{self.name} 錯誤: {str(e)}") # Debug
                    if not self.silent:
                        self.progress.warning(f"{self.name} 錯誤: {str(e)}", 1)
                    if attempt < retry - 1:
                        time.sleep(1)
                    continue
            return None
        except Exception as e:
            logging.error(f"{self.name} 致命錯誤: {str(e)}") # Debug
            if not self.silent:
                self.progress.error(f"{self.name} 致命錯誤: {str(e)}", 1)
            return None

class OfficialAPIDataSource(DataSource):
    """官方API數據源 (TWSE & TPEx)"""
    def __init__(self, progress_tracker=None, silent=False):
        super().__init__(progress_tracker)
        self.name = "OfficialAPI"
        self.silent = silent  # 靜默模式，不輸出詳細進度
    
    def fetch_raw_data_twse(self, stock_code, year_month):
        """從TWSE獲取原始數據"""
        date_str = f"{year_month}01"
        params = {
            'response': 'json',
            'date': date_str,
            'stockNo': stock_code
        }
        response = requests.get(
            TWSE_STOCK_DAY_URL, 
            params=params, 
            verify=False, 
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    
    def parse_twse_data(self, response_data):
        """解析TWSE回應"""
        if 'data' not in response_data or not response_data['data']:
            return None
        columns = ['日期', '成交股數', '成交金額', '成交筆數', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
        return pd.DataFrame(response_data['data'], columns=columns)
    
    def fetch_raw_data_tpex(self, stock_code, year_month):
        """從TPEx獲取原始數據"""
        if '/' not in year_month:
            year_month = f"{year_month[:4]}/{year_month[4:]}"
        date_str = f"{year_month}/01"
        params = {
            'd': date_str,
            'stk_code': stock_code,
            'o': 'json'
        }
        response = requests.get(
            TPEX_DAILY_TRADING_URL, 
            params=params, 
            verify=False, 
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    
    def parse_tpex_data(self, response_data):
        """解析TPEx回應"""
        # 處理兩種可能的回應格式
        if 'aaData' in response_data and response_data['aaData']:
            columns = ['日期', '成交股數', '成交金額', '成交筆數', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
            df_data = []
            for row in response_data['aaData']:
                if len(row) >= 9:
                    df_data.append(row[:9])
                elif len(row) > 0:
                    padded_row = row + ['0'] * (9 - len(row))
                    df_data.append(padded_row[:9])
            return pd.DataFrame(df_data, columns=columns)
        elif 'reportData' in response_data and 'data' in response_data['reportData']:
            columns = ['日期', '成交股數', '成交金額', '成交筆數', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差']
            return pd.DataFrame(response_data['reportData']['data'], columns=columns)
        return None
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """從官方API獲取歷史資料 (確保250交易日)"""
        try:
            # 設置時間範圍 - 確保獲取足夠的歷史數據
            if end_date is None:
                end_date = datetime.now()
            else:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            # 設置開始日期，確保至少有250個交易日
            if start_date is None:
                start_date = end_date - timedelta(days=370)
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            
            # 準備收集所有月份的數據
            all_data = pd.DataFrame()
            current_date = start_date
            month_count = 0
            
            while current_date <= end_date and month_count < 12:  # 最多嘗試12個月 (約250個交易日)
                year_month = current_date.strftime("%Y%m")
                if not self.silent:
                    self.progress.info(f"{self.name}: 嘗試獲取 {stock_code} {year_month}", 1)
                
                # 嘗試TWSE
                try:
                    if not self.silent:
                        self.progress.info("嘗試TWSE API...", 2)
                    twse_data = self.fetch_raw_data_twse(stock_code, year_month)
                    df = self.parse_twse_data(twse_data)
                    if df is not None and not df.empty:
                        df = convert_numeric_columns(df)
                        df = convert_dates_to_western(df)
                        df = standardize_dataframe(df, "TWSE", stock_code)
                        all_data = pd.concat([all_data, df])
                        if not self.silent:
                            self.progress.success(f"TWSE: 獲取 {len(df)} 筆數據", 2)
                        current_date = current_date + timedelta(days=31)
                        month_count += 1
                        continue
                except Exception as e:
                    if not self.silent:
                        self.progress.warning(f"TWSE 錯誤: {str(e)}", 2)
                
                # 嘗試TPEx
                try:
                    if not self.silent:
                        self.progress.info("嘗試TPEx API...", 2)
                    tpex_data = self.fetch_raw_data_tpex(stock_code, year_month)
                    df = self.parse_tpex_data(tpex_data)
                    if df is not None and not df.empty:
                        df = convert_numeric_columns(df)
                        df = convert_dates_to_western(df)
                        df = standardize_dataframe(df, "TPEx", stock_code)
                        all_data = pd.concat([all_data, df])
                        if not self.silent:
                            self.progress.success(f"TPEx: 獲取 {len(df)} 筆數據", 2)
                        month_count += 1
                except Exception as e:
                    if not self.silent:
                        self.progress.warning(f"TPEx 錯誤: {str(e)}", 2)
                
                # 移動到下個月
                current_date = current_date + timedelta(days=31)
                time.sleep(2.0) # 增加延遲以避免被封鎖 (F1-005)
            
            # 檢查是否獲取足夠數據
            if not all_data.empty:
                # 確保日期排序
                all_data = all_data.sort_index()
                # 只保留最近的250個交易日
                if len(all_data) > 250:
                    all_data = all_data.tail(250)
                
                if not self.silent:
                    self.progress.success(f"{self.name}: 總共獲取 {len(all_data)} 筆 {stock_code} 數據", 1)
                return all_data.reset_index().rename(columns={'日期': 'date'})
            return None
        except Exception as e:
            if not self.silent:
                self.progress.error(f"{self.name} 致命錯誤: {str(e)}", 1)
            return None

class DataSourceManager:
    """數據源管理器 - 實現失敗轉移 (修正: 僅使用 FinMind)"""
    def __init__(self, progress_tracker=None, silent=False):
        self.progress = progress_tracker or ProgressTracker()
        self.silent = silent
        # 修正: 僅使用 FinMind (使用者要求取消備援)
        self.sources = [
            FinMindDataSource(progress_tracker, silent=silent)
        ]
    
    def fetch_history(self, stock_code, start_date=None, end_date=None, retry=3):
        """嘗試所有數據源，直到成功或全部失敗"""
        for i, source in enumerate(self.sources):
            if not self.silent:
                self.progress.info(f"嘗試使用 {source.name} 獲取 {stock_code} 數據...", 1)
            df = source.fetch_history(stock_code, start_date, end_date, retry)
            if df is not None and not df.empty:
                return df
            
            if not self.silent:
                self.progress.warning(f"{source.name} 無法獲取 {stock_code} 數據", 1)
            if i < len(self.sources) - 1:
                self.progress.info(f"正在切換至下一個數據源: {self.sources[i+1].name}...", 1)
                
        self.progress.error(f"所有數據源都無法獲取 {stock_code} 數據", 1)
        return None


def setup_logging():
    log_file = WORK_DIR / 'system.log'
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if logger.handlers: return
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

setup_logging()
def print_flush(s="", end="\n"):
    print(s, end=end)
    sys.stdout.flush()

# ==============================
# 工具函數
# ==============================
def safe_num(v):
    if v is None: return 0.0
    if isinstance(v, (int, float)): return float(v)
    try:
        v_str = str(v).replace(',', '').strip()
        if v_str == '' or v_str == '--': return 0.0
        return float(v_str)
    except ValueError: return 0.0

def safe_int(v):
    if v is None: return 0
    if isinstance(v, (int, float)): return int(v)
    try:
        v_str = str(v).replace(',', '').strip()
        if v_str == '' or v_str == '--': return 0
        return int(float(v_str))
    except ValueError: return 0

def safe_float_preserving_none(value, default=None):
    if value is None: return default
    if isinstance(value, float) and np.isnan(value): return default
    try:
        return float(value)
    except: return default

def roc_to_western_date(roc_date_str):
    if pd.isna(roc_date_str) or roc_date_str is None: return "1970-01-01"
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
    except: pass
    return roc_date_str

def read_single_key(prompt="請選擇: "):
    print(prompt, end='', flush=True)
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
                if ch in [b'\x00', b'\xe0']:
                    msvcrt.getch(); continue
                decoded = ch.decode('utf-8', errors='ignore')
                if decoded:
                    print(decoded)
                    return decoded
            except: continue

def get_display_limit(default=30):
    try:
        limit = input(f"請輸入顯示檔數 (預設{default}): ").strip()
        return int(limit) if limit.isdigit() and int(limit) > 0 else default
    except: return default

def get_volume_limit(default=500):
    try:
        limit = input(f"請輸入最小成交量(張) (預設{default}): ").strip()
        return int(limit) * 1000 if limit.isdigit() else default * 1000
    except: return default * 1000

def get_correct_stock_name(code, current_name=None):
    if current_name and current_name != code and current_name != "未知": return current_name
    return current_name if current_name else code

# ==============================
# 指標計算類別 (保持數學核心)
# ==============================
class IndicatorCalculator:
    @staticmethod
    def calculate_wma(series, period):
        if len(series) < period: return np.full(len(series), np.nan)
        weights = np.arange(1, period + 1)
        wma = []
        for i in range(len(series)):
            if i < period - 1: wma.append(np.nan)
            else:
                window = series[i-period+1:i+1]
                wma.append(np.dot(window, weights) / weights.sum())
        return np.array(wma)

    @staticmethod
    def calculate_wma_for_df(df, period):
        if df.empty or len(df) < period: return None
        try:
            vals = df['close'].dropna().values
            wma = IndicatorCalculator.calculate_wma(vals, period)
            return round(wma[-1], 2) if not np.isnan(wma[-1]) else None
        except: return None

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
            
            # 計算信號線
            macd_series_for_signal = []
            for i in range(len(macd_line)):
                if np.isnan(macd_line[i]):
                    macd_series_for_signal.append(np.nan)
                else:
                    macd_series_for_signal.append(macd_line[i])
            
            signal_line = IndicatorCalculator.calculate_wma(np.array(macd_series_for_signal), signal)
            
            return pd.Series(macd_line, index=df.index), pd.Series(signal_line, index=df.index)
        except:
            return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_ma(df, period):
        if df.empty or len(df) < period: return None
        ma = df['close'].rolling(window=period).mean().iloc[-1]
        return round(ma, 2) if not pd.isna(ma) else None

    @staticmethod
    def calculate_rsi(df, period=14):
        if df.empty or len(df) < period + 1: return None
        try:
            deltas = np.diff(df['close'].values)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            avg_gain = IndicatorCalculator.calculate_wma(gains, period)[-1]
            avg_loss = IndicatorCalculator.calculate_wma(losses, period)[-1]
            if avg_loss == 0: return 100.0 if avg_gain > 0 else 50.0
            rs = avg_gain / avg_loss
            return round(100 - (100 / (1 + rs)), 2)
        except: return None

    @staticmethod
    def calculate_rsi_series(df, period=14):
        """計算 RSI 指標序列"""
        if df.empty or len(df) < period + 1:
            return pd.Series(np.nan, index=df.index)
        try:
            deltas = np.diff(df['close'].values)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            # 補齊長度 (diff 會少一個元素)
            gains = np.insert(gains, 0, 0)
            losses = np.insert(losses, 0, 0)
            
            avg_gains = IndicatorCalculator.calculate_wma(gains, period)
            avg_losses = IndicatorCalculator.calculate_wma(losses, period)
            
            rsi_values = []
            for i in range(len(df)):
                if np.isnan(avg_gains[i]) or np.isnan(avg_losses[i]):
                    rsi_values.append(np.nan)
                elif avg_losses[i] == 0:
                    rsi_values.append(100.0 if avg_gains[i] > 0 else 50.0)
                else:
                    rs = avg_gains[i] / avg_losses[i]
                    rsi_values.append(100 - (100 / (1 + rs)))
            
            return pd.Series(rsi_values, index=df.index)
        except Exception as e:
            return pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_macd(df, fast=12, slow=26, signal=9):
        if df.empty or len(df) < slow: return None, None
        try:
            closes = df['close'].values
            wma_f = IndicatorCalculator.calculate_wma(closes, fast)
            wma_s = IndicatorCalculator.calculate_wma(closes, slow)
            macd_line = wma_f - wma_s
            valid_macd = macd_line[slow-1:] 
            if len(valid_macd) < signal: return round(macd_line[-1], 2), None
            sig_vals = IndicatorCalculator.calculate_wma(valid_macd, signal)
            return round(macd_line[-1], 2), round(sig_vals[-1], 2)
        except: return None, None

    @staticmethod
    def calculate_mfi(df, period=14):
        if df.empty or len(df) < period: return pd.Series(np.nan, index=df.index)
        try:
            tp = (df['high'] + df['low'] + df['close']) / 3
            mf = tp * df['volume']
            pos = np.where(tp > tp.shift(1), mf, 0)
            neg = np.where(tp < tp.shift(1), mf, 0)
            pos_wma = IndicatorCalculator.calculate_wma(pos, period)
            neg_wma = IndicatorCalculator.calculate_wma(neg, period)
            mfi = []
            for p, n in zip(pos_wma, neg_wma):
                if np.isnan(p) or np.isnan(n): mfi.append(50.0)
                elif n == 0: mfi.append(100.0 if p > 0 else 50.0)
                else: mfi.append(100 - (100 / (1 + p/n)))
            return pd.Series(mfi, index=df.index)
        except: return pd.Series(np.full(len(df), 50.0), index=df.index)

    @staticmethod
    def calculate_vwap(df, lookback=20):
        if df.empty or len(df) < lookback: return None
        try:
            recent = df.tail(lookback)
            tp = (recent['high'] + recent['low'] + recent['close']) / 3
            return round((tp * recent['volume']).sum() / recent['volume'].sum(), 2)
        except: return None

    @staticmethod
    def calculate_vwap_series(df, lookback=20):
        """計算 VWAP 指標序列"""
        if df.empty: return pd.Series(np.nan, index=df.index)
        try:
            typical_price = (df['high'] + df['low'] + df['close']) / 3
            pv = typical_price * df['volume']
            
            # 使用 rolling sum 計算
            pv_sum = pv.rolling(window=lookback).sum()
            vol_sum = df['volume'].rolling(window=lookback).sum()
            
            vwap_series = pv_sum / vol_sum
            return vwap_series
        except:
            return pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_chg14_series(df):
        """計算 14日漲跌幅序列"""
        if df.empty: return pd.Series(np.nan, index=df.index)
        try:
            # 14日前的收盤價 (shift 14)
            prev_close = df['close'].shift(14)
            chg = (df['close'] - prev_close) / prev_close * 100
            return chg
        except:
            return pd.Series(np.nan, index=df.index)

    @staticmethod
    def calculate_chg14(df):
        if len(df) < 15: return None
        curr = df.iloc[-1]['close']
        prev = df.iloc[-15]['close']
        if prev == 0: return None
        return round((curr - prev) / prev * 100, 2)

    @staticmethod
    def calculate_vp_scheme3(df, lookback=20):
        if df.empty or len(df) < lookback: return {'POC': None, 'VP_upper': None, 'VP_lower': None}
        try:
            recent = df.tail(lookback)
            low, high = recent['low'].min(), recent['high'].max()
            if low >= high: return {'POC': None, 'VP_upper': None, 'VP_lower': None}
            bins = np.arange(low, high + 0.01, 0.01)
            vol_dist = np.zeros(len(bins))
            for _, row in recent.iterrows():
                mask = (bins >= row['low']) & (bins <= row['high'])
                cnt = mask.sum()
                if cnt > 0: vol_dist[mask] += row['volume'] / cnt
            if vol_dist.sum() == 0: return {'POC': None, 'VP_upper': None, 'VP_lower': None}
            poc_idx = np.argmax(vol_dist)
            poc = bins[poc_idx]
            sorted_idx = np.argsort(vol_dist)[::-1]
            cum_vol = 0
            total_vol = vol_dist.sum()
            va_prices = []
            for i in sorted_idx:
                cum_vol += vol_dist[i]
                va_prices.append(bins[i])
                if cum_vol >= total_vol * 0.7: break
            return {'POC': round(poc, 2), 'VP_upper': round(max(va_prices), 2), 'VP_lower': round(min(va_prices), 2)}
        except: return {'POC': None, 'VP_upper': None, 'VP_lower': None}

    @staticmethod
    def calculate_monthly_kd(df, k_period=9):
        if df.empty or len(df) < 20: return {'K': 0, 'D': 0, 'golden_cross': False}
        try:
            close_month = df['close'].rolling(20).mean()
            low_min  = df['low'].rolling(k_period).min()
            high_max = df['high'].rolling(k_period).max()
            rsv = (close_month - low_min) / (high_max - low_min) * 100
            rsv = rsv.fillna(50)
            k = rsv.ewm(com=2).mean()
            d = k.ewm(com=2).mean()
            if len(k) < 2 or len(d) < 2:
                return {'K': round(k.iloc[-1],2), 'D': round(d.iloc[-1],2), 'golden_cross': False}
            cross = (k.iloc[-2] < d.iloc[-2]) and (k.iloc[-1] >= d.iloc[-1])
            return {'K': round(k.iloc[-1],2), 'D': round(d.iloc[-1],2), 'golden_cross': cross}
        except: return {'K': 0, 'D': 0, 'golden_cross': False}

    @staticmethod

    @staticmethod
    def calculate_daily_kd_series(df, n=9):
        """計算日KD指標 (回傳 Series)"""
        try:
            high = df['high']
            low = df['low']
            close = df['close']
            
            # RSV = (Close - Lowest_Low_9) / (Highest_High_9 - Lowest_Low_9) * 100
            lowest_low = low.rolling(window=n, min_periods=1).min()
            highest_high = high.rolling(window=n, min_periods=1).max()
            rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
            rsv = rsv.fillna(50) # 缺值補50
            
            # K = 2/3 * Prev_K + 1/3 * RSV
            # D = 2/3 * Prev_D + 1/3 * K
            k_series = []
            d_series = []
            k_curr = 50
            d_curr = 50
            
            for val in rsv:
                k_curr = (2/3) * k_curr + (1/3) * val
                d_curr = (2/3) * d_curr + (1/3) * k_curr
                k_series.append(k_curr)
                d_series.append(d_curr)
                
            return pd.Series(k_series, index=df.index), pd.Series(d_series, index=df.index)
        except Exception as e:
            # print(f"Daily KD Error: {e}")
            return pd.Series([None]*len(df), index=df.index), pd.Series([None]*len(df), index=df.index)

    @staticmethod
    def resample_to_weekly(df):
        """將日線數據轉換為周線數據"""
        try:
            # 確保索引是 DatetimeIndex
            df_copy = df.copy()
            df_copy.index = pd.to_datetime(df_copy['date'])
            
            # Resample logic
            weekly = df_copy.resample('W-FRI').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            })
            return weekly.dropna()
        except Exception as e:
            # print(f"Resample Error: {e}")
            return pd.DataFrame()

    @staticmethod
    def calculate_weekly_kd_series(df, n=9):
        """計算周KD指標 (回傳 Series，已對齊日線)"""
        try:
            # 1. 轉周線
            weekly_df = IndicatorCalculator.resample_to_weekly(df)
            if weekly_df.empty or len(weekly_df) < n:
                return pd.Series([None]*len(df), index=df.index), pd.Series([None]*len(df), index=df.index)
            
            # 2. 計算周KD
            wk, wd = IndicatorCalculator.calculate_daily_kd_series(weekly_df, n) # 邏輯相同
            
            # 3. 將周KD回填到日線 (使用 reindex + ffill)
            # 注意：weekly_df 的 index 是每周五，我們需要將其映射回日線
            # 使用 asof 或 merge_asof 可能更準確，但這裡簡單用 reindex
            
            # 建立一個包含所有日期的 DataFrame
            df_dates = pd.DataFrame(index=pd.to_datetime(df['date']))
            
            # 將周線數據合併進來
            wk.name = 'week_k'
            wd.name = 'week_d'
            
            merged = df_dates.join(wk, how='left').join(wd, how='left')
            
            # 向前填充 (本周五的值在下周五出來前都有效？或者該周每天都用該周五的值？)
            # 傳統上，周KD是該周結束才確定。但在進行中，通常用當前計算值。
            # 這裡簡單處理：ffill (過去的值延續)
            merged = merged.fillna(method='ffill')
            
            return merged['week_k'].values, merged['week_d'].values
            
        except Exception as e:
            # print(f"Weekly KD Error: {e}")
            return pd.Series([None]*len(df), index=df.index), pd.Series([None]*len(df), index=df.index)


    @staticmethod
    def calculate_smi_series(df, period=14):
        """計算 Smart Money Index (SMI)"""
        try:
            # 簡化版 SMI: (收盤 - 開盤) / (最高 - 最低) 的累計
            # 真正的 SMI 需要分時資料，這裡用日線模擬
            # 概念: 收盤 > 開盤 代表散戶追高? 不，通常 Intraday Momentum Index 
            # 這裡採用: 
            # Smart Money = (Close - Low) - (High - Close) = 2*Close - High - Low
            # SMI = EMA(Smart Money, period)
            
            high = df['high']
            low = df['low']
            close = df['close']
            
            # Smart Money Flow
            smf = (2 * close) - high - low
            
            # SMI
            smi = smf.ewm(span=period, adjust=False).mean()
            return smi
        except:
            return pd.Series(0, index=df.index)

    @staticmethod
    def calculate_nvi_series(df):
        """計算 Negative Volume Index (NVI)"""
        try:
            close = df['close']
            volume = df['volume']
            
            nvi = pd.Series(1000.0, index=df.index)
            
            for i in range(1, len(df)):
                if volume.iloc[i] < volume.iloc[i-1]:
                    # 成交量縮小，聰明錢進場?
                    pct_change = (close.iloc[i] - close.iloc[i-1]) / close.iloc[i-1]
                    nvi.iloc[i] = nvi.iloc[i-1] * (1 + pct_change)
                else:
                    nvi.iloc[i] = nvi.iloc[i-1]
            
            # 加上 EMA 訊號線
            nvi_ema = nvi.ewm(span=255, adjust=False).mean()
            return nvi, nvi_ema
        except:
            return pd.Series(1000.0, index=df.index), pd.Series(1000.0, index=df.index)

    @staticmethod
    def calculate_vsa_signal_series(df):
        """計算 VSA (Volume Spread Analysis) 訊號"""
        try:
            # 簡單 VSA 邏輯:
            # 1. 停止行為 (Stopping Volume): 下跌趨勢中 + 爆量 + 收下影線
            # 2. 努力無果 (Effort No Result): 上漲趨勢中 + 爆量 + 收上影線/小實體
            
            signals = pd.Series(0, index=df.index)
            
            close = df['close']
            high = df['high']
            low = df['low']
            open_ = df['open']
            volume = df['volume']
            
            vol_ma = volume.rolling(20).mean()
            spread = high - low
            avg_spread = spread.rolling(20).mean()
            
            # 向量化計算
            is_high_vol = volume > (vol_ma * 1.5)
            is_wide_spread = spread > (avg_spread * 1.2)
            is_down_bar = close < close.shift(1)
            is_up_bar = close > close.shift(1)
            
            # Stopping Volume (訊號 1)
            # 下跌 + 爆量 + 收盤在下半部但有支撐 (這裡簡化為收盤離低點有段距離)
            # 修正: Stopping Volume 通常是收盤在相對高位 (收腳)
            close_pos = (close - low) / (high - low + 0.0001)
            
            cond_stopping = is_down_bar & is_high_vol & (close_pos > 0.6)
            signals[cond_stopping] = 1
            
            # No Demand (訊號 2)
            # 上漲 + 量縮 + 窄幅
            cond_no_demand = is_up_bar & (volume < vol_ma * 0.8) & (spread < avg_spread * 0.8)
            signals[cond_no_demand] = 2
            
            return signals
        except:
            return pd.Series(0, index=df.index)

    @staticmethod
    def calculate_smart_score_series(df):
        """計算綜合 Smart Score (0-5分)"""
        try:
            # 1. SMI
            smi = IndicatorCalculator.calculate_smi_series(df)
            smi_signal = (smi > smi.shift(1)).astype(int)
            
            # 2. NVI
            nvi, nvi_ema = IndicatorCalculator.calculate_nvi_series(df)
            nvi_signal = (nvi > nvi_ema).astype(int)
            
            # 3. VSA
            vsa_signal = IndicatorCalculator.calculate_vsa_signal_series(df)
            
            # 4. SVI (Smart Volume Index) - 簡化版: 量縮價漲 or 量增價跌(吸籌?)
            # 這裡用: 價漲量縮 (惜售) = 1
            close_up = df['close'] > df['close'].shift(1)
            vol_down = df['volume'] < df['volume'].shift(1)
            svi_signal = (close_up & vol_down).astype(int)
            
            # 5. 綜合評分
            # 基礎分: 趨勢向上 (MA20 > MA60)
            ma20 = df['close'].rolling(20).mean()
            ma60 = df['close'].rolling(60).mean()
            trend_score = (ma20 > ma60).astype(int)
            
            # 總分
            total_score = smi_signal + nvi_signal + (vsa_signal > 0).astype(int) + svi_signal + trend_score
            
            return total_score, smi_signal, nvi_signal, vsa_signal, svi_signal
        except Exception as e:
            print(f"Smart Score Calc Error: {e}")
            idx = df.index
            return (pd.Series(0, index=idx), pd.Series(0, index=idx), 
                    pd.Series(0, index=idx), pd.Series(0, index=idx), pd.Series(0, index=idx))

    def calculate_monthly_kd_series(df, k_period=9):
        """計算月KD指標序列 (修正: 返回 Series)"""
        if df.empty or len(df) < 20: 
            return pd.Series(0, index=df.index), pd.Series(0, index=df.index)
        try:
            close_month = df['close'].rolling(20).mean()
            low_min  = df['low'].rolling(k_period).min()
            high_max = df['high'].rolling(k_period).max()
            
            # [修正] 處理分母為 0 的情況 (High == Low)
            denominator = high_max - low_min
            denominator = denominator.replace(0, np.nan)
            
            rsv = (close_month - low_min) / denominator * 100
            
            # [修正] 強制限制 RSV 在 0~100 之間 (避免 MA20 超出 High/Low 範圍導致數值異常)
            rsv = rsv.clip(0, 100)
            
            rsv = rsv.fillna(50)
            k = rsv.ewm(com=2).mean()
            d = k.ewm(com=2).mean()
            return k, d
        except: 
            return pd.Series(0, index=df.index), pd.Series(0, index=df.index)

# ==============================
# 步驟函數
# ==============================
def get_latest_market_date():
    """獲取市場最新交易日期 (比對 TWSE 與 TPEx)"""
    dates = []
    
    # 1. Check TWSE (Website API)
    try:
        url = f"{TWSE_STOCK_DAY_ALL_URL}&_={int(time.time())}"
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_ALL.html'
        })
        try:
            session.get('https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_ALL.html', timeout=5, verify=False)
        except: pass
        
        res = session.get(url, timeout=10, verify=False)
        if res.status_code == 200:
            data = res.json()
            if 'date' in data and len(data['date']) == 8:
                d = data['date'] # YYYYMMDD
                dates.append(f"{d[:4]}-{d[4:6]}-{d[6:]}")
    except: pass
    
    # 2. Check TPEx
    try:
        url = f"{TPEX_DAILY_TRADING_URL}?d=&stk_code=&o=json&_={int(time.time())}"
        res = requests.get(url, timeout=10, verify=False)
        if res.status_code == 200:
            data = res.json()
            if 'reportDate' in data:
                dates.append(roc_to_western_date(data['reportDate']))
            elif 'aaData' in data and data['aaData']:
                pass
    except: pass
    
    if not dates:
        return datetime.now().strftime("%Y-%m-%d")
        
    return max(dates)

def step1_fetch_stock_list():
    print_flush("\n[Step 1] 更新上市櫃清單...")
    stocks = []
    try:
        print_flush("  -> 下載 TWSE...", end="")
        res = requests.get(TWSE_BWIBBU_URL, timeout=30, verify=False)
        for i in res.json():
            if is_normal_stock(i.get('Code'), i.get('Name')):
                stocks.append({'code': i['Code'], 'name': i['Name'], 'market': 'TWSE'})
        print_flush(" ✓")
    except: print_flush(" ✗")
    try:
        print_flush("  -> 下載 TPEx...", end="")
        res = requests.get(TPEX_MAINBOARD_URL, timeout=30, verify=False)
        for i in res.json():
            if is_normal_stock(i.get('SecuritiesCompanyCode'), i.get('CompanyName')):
                stocks.append({'code': i['SecuritiesCompanyCode'], 'name': i['CompanyName'], 'market': 'TPEX'})
        print_flush(" ✓")
    except: print_flush(" ✗")

    if stocks:
        pd.DataFrame(stocks).to_csv(STOCK_LIST_PATH, index=False)
        print_flush(f"✓ 已更新 {len(stocks)} 檔股票至清單")
    else:
        print_flush("❌ 更新失敗")

def step2_download_tpex_daily():
    print_flush("\n[Step 2] 下載 TPEx (上櫃) 本日行情...")
    try:
        res = requests.get(TPEX_MAINBOARD_URL, timeout=30, verify=False)
        data = res.json()
        if not data: return
        trade_date = roc_to_western_date(data[0].get('Date') or data[0].get('date'))
        print_flush(f"  -> 日期: {trade_date}")
        
        new_count = 0
        update_count = 0
        skip_count = 0
        
        updated_codes = set()
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            for item in data:
                code = item.get('SecuritiesCompanyCode', '').strip()
                name = item.get('CompanyName', '').strip()
                if not is_normal_stock(code, name): continue
                
                close = safe_num(item.get('Close'))
                vol = safe_int(item.get('TradingShares'))
                
                # 檢查是否存在
                cur.execute("SELECT close FROM stock_data WHERE code=? AND date=?", (code, trade_date))
                existing = cur.fetchone()
                
                should_update = False
                if existing is None:
                    new_count += 1
                    should_update = True
                elif existing[0] != close:
                    update_count += 1
                    should_update = True
                else:
                    skip_count += 1
                
                if should_update:
                    updated_codes.add(code)
                    cur.execute("SELECT close, volume FROM stock_data WHERE code=? AND date<? ORDER BY date DESC LIMIT 1", (code, trade_date))
                    prev = cur.fetchone()
                    pc, pv = (prev[0], prev[1]) if prev else (close, vol)
                    
                    cur.execute("""
                        INSERT OR REPLACE INTO stock_data 
                        (code, name, date, open, high, low, close, volume, close_prev, vol_prev)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (code, name, trade_date, safe_num(item.get('Open')), safe_num(item.get('High')), safe_num(item.get('Low')), close, vol, pc, pv))
            conn.commit()
        print_flush(f"✓ TPEx 更新: 新增 {new_count} 筆 | 更新 {update_count} 筆 | 跳過 {skip_count} 筆")
        return updated_codes
    except Exception as e: 
        print_flush(f"❌ 失敗: {e}")
        return set()

def step3_download_twse_daily():
    print_flush("\n[Step 3] 下載 TWSE (上市) 本日行情...")
    try:
        # 使用 Session 保持連線與 Cookies
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_ALL.html',
            'X-Requested-With': 'XMLHttpRequest'
        })
        
        # 先訪問首頁取得 Cookies
        try:
            session.get('https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY_ALL.html', timeout=10, verify=False)
        except: pass
        
        # 再請求資料
        url = f"{TWSE_STOCK_DAY_ALL_URL}&_={int(time.time())}"
        res = session.get(url, timeout=30, verify=False)
        try:
            data = res.json()
        except json.JSONDecodeError:
            print_flush(f"❌ TWSE 回應非 JSON 格式: {res.text[:100]}...")
            return
            
        if not data or 'data' not in data: return
        
        # Website API date format: YYYYMMDD
        raw_date = data.get('date', '')
        if len(raw_date) == 8:
            trade_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        else:
            trade_date = datetime.now().strftime("%Y-%m-%d")
            
        print_flush(f"  -> 日期: {trade_date}")

        new_count = 0
        update_count = 0
        skip_count = 0

        updated_codes = set()
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            # Website API data structure: List of Lists
            # Indices: 0=Code, 1=Name, 2=Vol, 3=Val, 4=Open, 5=High, 6=Low, 7=Close, 8=Change, 9=Trans
            for item in data['data']:
                if len(item) < 8: continue
                code = item[0].strip()
                name = item[1].strip()
                if not is_normal_stock(code, name): continue
                
                close = safe_num(item[7])
                vol = safe_int(item[2])
                
                # 檢查是否存在
                cur.execute("SELECT close FROM stock_data WHERE code=? AND date=?", (code, trade_date))
                existing = cur.fetchone()
                
                should_update = False
                if existing is None:
                    new_count += 1
                    should_update = True
                elif existing[0] != close:
                    update_count += 1
                    should_update = True
                else:
                    skip_count += 1
                
                if should_update:
                    updated_codes.add(code)
                    cur.execute("SELECT close, volume FROM stock_data WHERE code=? AND date<? ORDER BY date DESC LIMIT 1", (code, trade_date))
                    prev = cur.fetchone()
                    pc, pv = (prev[0], prev[1]) if prev else (close, vol)
                    
                    cur.execute("""
                        INSERT OR REPLACE INTO stock_data 
                        (code, name, date, open, high, low, close, volume, close_prev, vol_prev)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (code, name, trade_date, safe_num(item[4]), safe_num(item[5]), safe_num(item[6]), close, vol, pc, pv))
            conn.commit()
        print_flush(f"✓ TWSE 更新: 新增 {new_count} 筆 | 更新 {update_count} 筆 | 跳過 {skip_count} 筆")
        return updated_codes
    except Exception as e: 
        print_flush(f"❌ 失敗: {e}")
        return set()

MIN_DATA_COUNT = 450

def step4_check_data_gaps():
    print_flush("\n[Step 4] 檢查數據缺失...")
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        rows = cur.execute("SELECT code, COUNT(*) as cnt FROM stock_data GROUP BY code").fetchall()
        
    gaps = [r for r in rows if r[1] < MIN_DATA_COUNT]
    if not gaps:
        print_flush(f"✓ 所有股票資料皆充足 (>= {MIN_DATA_COUNT} 筆)")
    else:
        print_flush(f"⚠ 發現 {len(gaps)} 檔股票資料不足:")
        for r in gaps[:5]:
            print_flush(f"  - {r[0]}: {r[1]} 筆")
        if len(gaps) > 5: print_flush(f"  ... 等共 {len(gaps)} 檔")

def step5_clean_delisted():
    print_flush("\n[Step 5] 清理下市股票...")
    if not STOCK_LIST_PATH.exists():
        print_flush("⚠ 找不到股票清單，跳過清理")
        return
        
    try:
        df = pd.read_csv(STOCK_LIST_PATH)
        valid_codes = set(df['code'].astype(str))
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            db_codes = set(row[0] for row in cur.execute("SELECT DISTINCT code FROM stock_data").fetchall())
            
            delisted = db_codes - valid_codes
            if delisted:
                print_flush(f"發現 {len(delisted)} 檔下市股票，準備清理...")
                for code in delisted:
                    cur.execute("DELETE FROM stock_data WHERE code=?", (code,))
                conn.commit()
                print_flush(f"✓ 已清除 {len(delisted)} 檔下市股票資料")
            else:
                print_flush("✓ 無下市股票殘留")
                
    except Exception as e:
        print_flush(f"❌ 清理失敗: {e}")

def step4_load_data():
    print_flush("\n[Step 4] 載入分析資料...")
    data = {}
    with db_manager.get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        rows = cur.execute("""
            SELECT T1.* FROM stock_data T1
            JOIN (SELECT code, MAX(date) as max_date FROM stock_data GROUP BY code) T2 
            ON T1.code = T2.code AND T1.date = T2.max_date
        """).fetchall()
        
        for row in rows:
            data[row['code']] = dict(row)
            
    print_flush(f"✓ 已載入 {len(data)} 檔股票資料")
    return data
def reset_color():
    return '\033[0m'

def get_volume_color(vol_ratio):
    """成交量專用: 倍數 >1 紅(增量), <=1 綠(縮量)"""
    return '\033[91m' if vol_ratio > 1.0 else '\033[92m'

def get_trend_color(current, previous):
    """趨勢專用: 上揚紅, 下跌綠, 平盤白"""
    if current is None or previous is None:
        return '\033[0m'
    return '\033[91m' if current > previous else ('\033[92m' if current < previous else '\033[0m')

def get_arrow(today_val, prev_val):
    if today_val is None or prev_val is None:
        return " "
    if today_val > prev_val:
        return "↑"
    elif today_val < prev_val:
        return "↓"
    else:
        return " "

def get_colored_value(value, change, arrow=None):
    """根據漲跌返回帶顏色的值和箭頭"""
    color = get_color_code(change)
    
    if isinstance(value, (int, float)):
        val_str = f"{value:.2f}"
    else:
        val_str = str(value)
        
    if arrow:
        return f"{color}{arrow}{val_str}{reset_color()}"
    return f"{color}{val_str}{reset_color()}"

# Helper Functions for Formatting
def get_color_code(val):
    if val is None: return ""
    if val > 0: return "\033[91m" # Red
    if val < 0: return "\033[92m" # Green
    return "\033[97m" # White

def reset_color():
    return "\033[0m"

def get_arrow(curr, prev):
    if curr is None or prev is None: return ""
    if curr > prev: return "↑"
    if curr < prev: return "↓"
    return "-"

def get_volume_color(ratio):
    if ratio >= 2.0: return "\033[95m" # Magenta
    if ratio >= 1.5: return "\033[91m" # Red
    if ratio >= 1.2: return "\033[93m" # Yellow
    return "\033[97m"

def get_colored_value(text, change, arrow):
    color = get_color_code(change)
    return f"{color}{text}{arrow}{reset_color()}"

def get_trend_color(curr, prev):
    if curr is None or prev is None: return ""
    if curr > prev: return "\033[91m"
    if curr < prev: return "\033[92m"
    return "\033[97m"

def safe_float_preserving_none(value):
    if value is None: return None
    try:
        return float(value)
    except:
        return None

def format_scan_result(code, name, indicators, show_date=False):
    """格式化單日技術指標 - 修正: Null 防禦"""
    if not indicators:
        return ""
    
    def safe_display(value, prefix="", suffix="", default="N/A"):
        """安全格式化 - None 顯示 N/A"""
        if value is None:
            return f"{prefix}{default}{suffix}"
        return f"{prefix}{value}{suffix}"
    
    def safe_float(value, default=0.0):
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    date = indicators.get('date', '')
    close = safe_float(indicators.get('close'))
    close_prev = safe_float(indicators.get('close_prev'))
    volume = safe_float(indicators.get('volume'))
    vol_prev = safe_float(indicators.get('vol_prev'))
    mfi = safe_float(indicators.get('mfi14') or indicators.get('MFI'))
    mfi_prev = safe_float(indicators.get('mfi14_prev') or indicators.get('MFI_prev'))
    chg14 = safe_float(indicators.get('chg14_pct') or indicators.get('CHG14'))
    chg14_prev = safe_float(indicators.get('chg14_pct_prev') or indicators.get('CHG14_prev'))
    poc = safe_float(indicators.get('vp_poc') or indicators.get('POC'))
    vp_upper = safe_float(indicators.get('vp_upper') or indicators.get('VP_upper'))
    vp_lower = safe_float(indicators.get('vp_lower') or indicators.get('VP_lower'))
    vwap = safe_float(indicators.get('vwap20') or indicators.get('VWAP'))
    vwap_prev = safe_float(indicators.get('vwap20_prev') or indicators.get('VWAP_prev'))
    
    # 修復: 使用 safe_float_preserving_none 保留 None，讓箭頭判斷正確
    ma3 = safe_float_preserving_none(indicators.get('ma3') or indicators.get('MA3'))
    ma3_prev = safe_float_preserving_none(indicators.get('ma3_prev') or indicators.get('MA3_prev'))
    ma20 = safe_float_preserving_none(indicators.get('ma20') or indicators.get('MA20'))
    ma20_prev = safe_float_preserving_none(indicators.get('ma20_prev') or indicators.get('MA20_prev'))
    ma60 = safe_float_preserving_none(indicators.get('ma60') or indicators.get('MA60'))
    ma60_prev = safe_float_preserving_none(indicators.get('ma60_prev') or indicators.get('MA60_prev'))
    ma200 = safe_float_preserving_none(indicators.get('ma200') or indicators.get('MA200'))
    ma200_prev = safe_float_preserving_none(indicators.get('ma200_prev') or indicators.get('MA200_prev'))
    
    change_pct = 0
    if close_prev and close_prev != 0:
        change_pct = (close - close_prev) / close_prev * 100
    
    # 修正成交量比值計算
    if vol_prev and vol_prev > 0:
        volume_ratio = volume / vol_prev
    else:
        volume_ratio = 1.0  # 改為1.0表示無變化,而非0
    
    color = get_color_code(change_pct)
    reset = reset_color()
    
    vol_in_lots = volume / 1000 if volume else 0
    
    mfi_change = mfi - mfi_prev if mfi is not None and mfi_prev is not None else 0
    chg14_change = chg14 - chg14_prev if chg14 is not None and chg14_prev is not None else 0
    vwap_change = vwap - vwap_prev if vwap is not None and vwap_prev is not None else 0
    
    mfi_arrow = get_arrow(mfi, mfi_prev)
    chg14_arrow = get_arrow(chg14, chg14_prev)
    vwap_arrow = get_arrow(vwap, vwap_prev)
    
    vol_color = get_volume_color(volume_ratio)
    vol_text = f"{vol_color}{volume_ratio:.1f}倍{reset_color()}"
    colored_volume_ratio = vol_text
    
    colored_mfi = get_colored_value(f"{mfi:.1f}", mfi_change, mfi_arrow)
    colored_chg14 = get_colored_value(f"{chg14:.1f}%", chg14_change, chg14_arrow)
    colored_vwap = get_colored_value(f"{vwap:.2f}", vwap_change, vwap_arrow)
    
    ma3_color = get_trend_color(ma3, ma3_prev)
    colored_ma3 = safe_display(ma3, prefix=ma3_color, suffix=reset_color())

    ma20_color = get_trend_color(ma20, ma20_prev)
    colored_ma20 = safe_display(ma20, prefix=ma20_color, suffix=reset_color())
    
    ma60_color = get_trend_color(ma60, ma60_prev)
    colored_ma60 = safe_display(ma60, prefix=ma60_color, suffix=reset_color())

    ma200_color = get_trend_color(ma200, ma200_prev)
    colored_ma200 = safe_display(ma200, prefix=ma200_color, suffix=reset_color())
    
    if show_date:
        line1 = f"{date} {name}({code}) 成交量:{vol_in_lots:,.0f}張({colored_volume_ratio}) MFI:{colored_mfi}"
    else:
        line1 = f"{name}({code}) 成交量:{vol_in_lots:,.0f}張({colored_volume_ratio}) MFI:{colored_mfi}"
    
    line2 = f"收盤價:{color}{close:.2f}({change_pct:+.2f}%){reset} POC:{poc:.2f} 14日:{colored_chg14}"
    
    # 月KD 黃金交叉提示
    if indicators.get('KD_GOLDEN_CROSS'):
        line2 += f"  {get_color_code(+1)}✅ K={indicators['KD_K']:.2f} D={indicators['KD_D']:.2f}{reset_color()}"
        
    line3 = f"VP上:{COFFEE_COLOR}{vp_upper:.2f}{reset} VWAP:{colored_vwap} VP下:{COFFEE_COLOR}{vp_lower:.2f}{reset}"
    line4 = f"MA3:{colored_ma3} MA20:{colored_ma20} MA60:{colored_ma60} MA200:{colored_ma200}"
    
    return f"{line1}\n{line2}\n{line3}\n{line4}"

def format_scan_result_list(code, name, indicators_list):
    # 格式化多天技術指標結果
    if not indicators_list:
        return ""
    output_lines = []
    for indicators in indicators_list:
        output_lines.append(format_scan_result(code, name, indicators, show_date=True))
    return "\n".join(output_lines)

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
            "Prefer": "resolution=merge-duplicates" # 用於 upsert
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
            
            # 分批上傳
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
        """上傳計算結果到雲端 (days=None 為全部)"""
        if not ENABLE_CLOUD_SYNC:
            print_flush("⚠ 未設定 Supabase，無法同步")
            return False
            
        range_str = f"最近 {days} 天" if days else "所有"
        print_flush(f"☁ 正在上傳 {range_str} 數據到雲端...")
        try:
            with db_manager.get_connection() as conn:
                # 讀取日期
                cur = conn.cursor()
                if days:
                    sql = f"SELECT DISTINCT date FROM stock_data ORDER BY date DESC LIMIT {days}"
                else:
                    sql = "SELECT DISTINCT date FROM stock_data ORDER BY date DESC"
                
                cur.execute(sql)
                dates = [row[0] for row in cur.fetchall()]
                
                if not dates:
                    print_flush("⚠ 本地無數據可上傳")
                    return False
                
                total_dates = len(dates)
                for idx, date in enumerate(dates):
                    print_flush(f"正在處理日期: {date} ({idx+1}/{total_dates})")
                    
                    # 讀取該日期的所有數據
                    df = pd.read_sql_query(f"SELECT * FROM stock_data WHERE date='{date}'", conn)
                    
                    # 數據清洗: 處理 bytes 類型的數據
                    def clean_value(x):
                        if isinstance(x, bytes):
                            try:
                                return int.from_bytes(x, byteorder='little')
                            except:
                                return str(x)
                        return x

                    # 應用清洗函數到所有欄位
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].apply(clean_value)
                            
                    # 強制轉換 volume 相關欄位為整數
                    vol_cols = ['volume', 'vol_prev', 'volume_prev']
                    for col in vol_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            df[col] = df[col].astype('Int64')
                            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

                    # 轉換為 JSON 格式 (records)
                    records = df.to_dict(orient='records')
                    
                    # 分批上傳
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

def calculate_stock_history_indicators(code, display_days=30):
    """計算股票歷史技術指標 - 強制重新計算版 (完全恢復)"""
    try:
        # 從 DB 讀取原始數據（不帶任何快取欄位）
        with db_manager.get_connection() as conn:
            query = """
                SELECT date, open, high, low, close, volume 
                FROM stock_data 
                WHERE code=? 
                ORDER BY date ASC
            """
            df = pd.read_sql_query(query, conn, params=(code,))
        
        if df.empty or len(df) < 20:
            # print_flush(f"⚠ {code} 資料不足: 僅有{len(df)}筆,需至少20筆")
            return None
        
        # 確保日期格式正確
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # === 向量化指標計算（強制重新計算）===
        # MA
        df['MA3'] = df['close'].rolling(3).mean().round(2)
        df['MA20'] = df['close'].rolling(20).mean().round(2)
        df['MA60'] = df['close'].rolling(60).mean().round(2)
        df['MA120'] = df['close'].rolling(120).mean().round(2)
        df['MA200'] = df['close'].rolling(200).mean().round(2)
        
        # WMA (使用 calculate_wma 取得序列)
        df['WMA3'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 3), index=df.index).round(2)
        df['WMA20'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 20), index=df.index).round(2)
        df['WMA60'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 60), index=df.index).round(2)
        df['WMA120'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 120), index=df.index).round(2)
        df['WMA200'] = pd.Series(IndicatorCalculator.calculate_wma(df['close'].values, 200), index=df.index).round(2)
        
        # MFI
        df['MFI'] = IndicatorCalculator.calculate_mfi(df, 14).round(2)
        
        # VWAP
        df['VWAP'] = IndicatorCalculator.calculate_vwap_series(df, lookback=20).round(2)
        
        # CHG14
        df['CHG14'] = IndicatorCalculator.calculate_chg14_series(df).round(2)
        
        # RSI
        df['RSI'] = IndicatorCalculator.calculate_rsi_series(df, 14).round(2)
        
        # MACD
        macd, signal = IndicatorCalculator.calculate_macd_series(df)
        df['MACD'] = macd.round(2)
        df['SIGNAL'] = signal.round(2)
        
        # KD
        # kd = IndicatorCalculator.calculate_monthly_kd(df) 
        # KD
        # kd = IndicatorCalculator.calculate_monthly_kd(df) 
        # KD
        # kd = IndicatorCalculator.calculate_monthly_kd(df) 
        k_series, d_series = IndicatorCalculator.calculate_monthly_kd_series(df)
        
        # 計算日KD
        daily_k, daily_d = IndicatorCalculator.calculate_daily_kd_series(df)
        
        # 計算周KD
        week_k, week_d = IndicatorCalculator.calculate_weekly_kd_series(df)
        
        # [新增] Smart Score 計算
        smart_score, smi_sig, nvi_sig, vsa_sig, svi_sig = IndicatorCalculator.calculate_smart_score_series(df)
        df['Smart_Score'] = smart_score
        df['SMI_Signal'] = smi_sig
        df['NVI_Signal'] = nvi_sig
        df['VSA_Signal'] = vsa_sig
        df['SVI_Signal'] = svi_sig
        df['Month_K'] = k_series.round(2)
        df['Month_D'] = d_series.round(2)
        df['Daily_K'] = daily_k.round(2)
        df['Daily_D'] = daily_d.round(2)
        # week_k/d are numpy arrays
        df['Week_K'] = pd.Series(week_k, index=df.index).round(2)
        df['Week_D'] = pd.Series(week_d, index=df.index).round(2)
        
        # [新增] 昨日收盤與成交量 (用於計算漲跌幅與增量)
        df['close_prev'] = df['close'].shift(1)
        df['vol_prev'] = df['volume'].shift(1)
        
        # === 準備結果列表 ===
        indicators_list = []
        # 如果 display_days 為 None 或 0，則處理所有資料
        start_index = 0 if not display_days else max(0, len(df) - display_days)
        
        for i in range(start_index, len(df)):
            row = df.iloc[i]
            prev_row = df.iloc[i-1] if i > 0 else row
            
            indicators = {
                'date': row['date'].strftime('%Y-%m-%d'),
                'close': row['close'],
                'volume': row['volume'],
                'close_prev': row['close_prev'] if pd.notnull(row['close_prev']) else None,
                'vol_prev': row['vol_prev'] if pd.notnull(row['vol_prev']) else None,
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
                'vol_prev': prev_row['volume'],
                'close_prev': prev_row['close'],
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
                'Smart_Score': row['Smart_Score'],
                'SMI_Signal': row['SMI_Signal'],
                'NVI_Signal': row['NVI_Signal'],
                'VSA_Signal': row['VSA_Signal'],
                'SVI_Signal': row['SVI_Signal']
            }
            
            # VP 需要單獨計算 (因為是動態區間)
            current_window = df.iloc[max(0, i-19):i+1]
            vp = IndicatorCalculator.calculate_vp_scheme3(current_window, lookback=20)
            indicators['POC'] = vp['POC']
            indicators['VP_upper'] = vp['VP_upper']
            indicators['VP_lower'] = vp['VP_lower']
            
            indicators_list.append(indicators)
        
        return indicators_list[::-1] # 反轉回由新到舊
        
    except Exception as e:
        logging.error(f"計算指標失敗 {code}: {e}", exc_info=True)
        return None

# ==============================
# 掃描邏輯函數
# ==============================
def scan_vp(indicators_data, mode='lower', min_volume=100):
    results = []
    for code, ind in indicators_data.items():
        # 成交量過濾
        vol = safe_num(ind.get('volume', 0))
        if vol < min_volume: continue

        close = safe_num(ind.get('close'))
        vp_lower = safe_num(ind.get('vp_lower') or ind.get('VP_lower'))
        vp_upper = safe_num(ind.get('vp_upper') or ind.get('VP_upper'))
        
        if not close: continue
        
        if mode == 'lower':
            if not vp_lower: continue
            # 接近下緣 (支撐)
            if abs(close - vp_lower) / close < 0.02: # 2% 內
                results.append((code, 0, ind))
        else:
            if not vp_upper: continue
            # 接近上緣 (壓力)
            if abs(close - vp_upper) / close < 0.02:
                results.append((code, 0, ind))
    return results

def scan_mfi_mode(indicators_data, order='asc', min_volume=0):
    results = []
    for code, ind in indicators_data.items():
        if not ind: continue
        # latest = ind_list[0] # [修正] 輸入是 dict 不是 list
        vol = safe_num(ind.get('volume', 0))
        if vol < min_volume: continue
        
        # [修正] 相容 DB key (mfi14) 與計算 key (MFI)
        mfi = safe_num(ind.get('mfi14') or ind.get('MFI'))
        mfi_prev = safe_num(ind.get('mfi14_prev') or ind.get('MFI_prev'))
        
        if mfi is None or mfi_prev is None: continue
        
        if order == 'asc': # 小到大 (流入)
            if mfi > mfi_prev and mfi < 30: # 低檔回升
                results.append((code, mfi, ind))
        else: # 大到小 (流出)
            if mfi < mfi_prev and mfi > 70: # 高檔反轉
                results.append((code, mfi, ind))
    return sorted(results, key=lambda x: x[1], reverse=(order=='desc'))

def scan_ma_mode(indicators_data, ma_type='MA200', min_volume=0):
    results = []
    for code, ind in indicators_data.items():
        if not ind: continue
        # latest = ind_list[0] # [修正] 輸入是 dict 不是 list
        vol = safe_num(ind.get('volume', 0))
        if vol < min_volume: continue
        
        close = safe_num(ind.get('close'))
        # [修正] 相容 DB key (ma200) 與計算 key (MA200)
        ma_key = ma_type.lower()
        ma_val = safe_num(ind.get(ma_key) or ind.get(ma_type))
        
        if not (close and ma_val): continue
        
        diff_pct = (close - ma_val) / ma_val * 100
        if -10 <= diff_pct <= 0:
            results.append((code, diff_pct, ind))
            
    return sorted(results, key=lambda x: x[1])


def scan_smart_money_strategy():
    """聰明錢指標掃描 (Smart Score >= 3) - 詳細版"""
    limit, min_vol = get_user_scan_params()
    
    print_flush(f"\n正在掃描 聰明錢指標 (Smart Score >= 3)...")
    
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
        'score_3': 0,
        'score_4': 0,
        'score_5': 0
    }
    
    data = GLOBAL_INDICATOR_CACHE["data"]
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    stats['total'] = len(data)
    
    for code, ind in data.items():
        try:
            # 成交量過濾
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue
            
            stats['vol_pass'] += 1
            
            # 取得 Smart Score (優先使用小寫鍵)
            score = safe_int(ind.get('smart_score') or ind.get('Smart_Score'))
            
            if score is None:
                continue
                
            stats['has_score'] += 1
            
            # 統計各項訊號
            if safe_int(ind.get('smi_signal') or ind.get('SMI_Signal')) == 1:
                stats['smi_sig'] += 1
            if safe_int(ind.get('svi_signal') or ind.get('SVI_Signal')) == 1:
                stats['svi_sig'] += 1
            if safe_int(ind.get('nvi_signal') or ind.get('NVI_Signal')) == 1:
                stats['nvi_sig'] += 1
            if safe_int(ind.get('vsa_signal') or ind.get('VSA_Signal')) > 0:
                stats['vsa_sig'] += 1
            # VWAP Signal 需從 score 反推或直接檢查 close > vwap
            if ind.get('close') and ind.get('VWAP'):
                if safe_float_preserving_none(ind.get('close')) > safe_float_preserving_none(ind.get('VWAP')):
                    stats['vwap_sig'] += 1
            
            # 統計 Score 分布
            if score >= 5:
                stats['score_5'] += 1
            if score >= 4:
                stats['score_4'] += 1
            if score >= 3:
                stats['score_3'] += 1
            
            # Filter: Smart Score >= 3
            if score >= 3:
                results.append((code, score, ind))
                
        except: continue
        
    # Sort by Score desc
    results.sort(key=lambda x: x[1], reverse=True)
    
    # 顯示詳細篩選過程
    print_flush("\n" + "=" * 60)
    print_flush("[篩選過程] 聰明錢指標多層篩選")
    print_flush("=" * 60)
    print_flush(f"總股數: {stats['total']}")
    print_flush("─" * 60)
    print_flush(f"✓ 成交量 >= {min_vol//1000}張            → {stats['vol_pass']} 檔")
    print_flush(f"✓ 有 Smart Score 數據       → {stats['has_score']} 檔")
    print_flush("─" * 60)
    print_flush("【各項訊號統計】(通過成交量門檻者)")
    print_flush(f"  • SMI 訊號 (動能上升)     → {stats['smi_sig']} 檔")
    print_flush(f"  • SVI 訊號 (相對爆量)     → {stats['svi_sig']} 檔")
    print_flush(f"  • NVI 訊號 (散戶離場)     → {stats['nvi_sig']} 檔")
    print_flush(f"  • VSA 訊號 (停止量/漲停)  → {stats['vsa_sig']} 檔")
    print_flush(f"  • VWAP訊號 (價>均價)      → {stats['vwap_sig']} 檔")
    print_flush("─" * 60)
    print_flush("【Smart Score 分布】(滿分5分)")
    print_flush(f"  • Score >= 3 (買入訊號)   → {stats['score_3']} 檔")
    print_flush(f"  • Score >= 4 (強烈買入)   → {stats['score_4']} 檔")
    print_flush(f"  • Score = 5  (極強訊號)   → {stats['score_5']} 檔")
    print_flush("=" * 60)
    
    print_flush(f"\n【聰明錢掃描結果】找到 {len(results)} 檔符合條件的股票 (顯示前{limit}檔)")
    print_flush("=" * 80)
    
    for i, (code, score, ind) in enumerate(results[:limit]):
        name = get_correct_stock_name(code, ind.get('name', code))
        print_flush(f"{i+1}. {format_smart_money_result(code, name, ind)}")
    
    print_flush("=" * 80)
    print_flush(f"[顯示檔數: {min(limit, len(results))}/{len(results)}]")
    print_flush("=" * 80)

def format_smart_money_result(code, name, ind):
    """格式化聰明錢掃描結果"""
    close = safe_float_preserving_none(ind.get('close'))
    score = safe_int(ind.get('smart_score') or ind.get('Smart_Score'))
    smi = safe_float_preserving_none(ind.get('smi') or ind.get('SMI'))
    svi = safe_float_preserving_none(ind.get('svi') or ind.get('SVI'))
    
    result = f"{code:6s} {name:10s} | Score: {score}/5"
    if close:
        result += f" | 收: {close:6.2f}"
    if smi:
        result += f" | SMI: {smi:6.1f}"
    
    return result

def scan_triple_filter_mode_v32(all_indicators, min_volume=500000, limit=20):
    """三重篩選進階版"""
    results = []
    total = len(all_indicators)
    
    stats = {
        "total": total, "volume_pass": 0, "trend_pass": 0, 
        "mfi_pass": 0, "breakout_pass": 0, "final_pass": 0
    }
    
    with ProgressTracker(3) as progress:
        for idx, (code, indicators) in enumerate(all_indicators.items()):
            if not indicators: continue
            # indicators = ind_list[0] # [修正] 輸入是 dict 不是 list
            
            # 1. 成交量檢查
            vol = safe_num(indicators.get('volume', 0))
            vol_prev = safe_num(indicators.get('vol_prev', 0))
            if vol < min_volume: continue
            if vol_prev and vol <= vol_prev: continue
            stats["volume_pass"] += 1
            
            # 2. 趨勢檢查
            if not is_wma20_rising(indicators.get('wma20') or indicators.get('WMA20'), 
                                 indicators.get('wma20_prev') or indicators.get('WMA20_prev')): continue
            if not is_vwap20_rising(indicators.get('vwap20') or indicators.get('VWAP'), 
                                  indicators.get('vwap20_prev') or indicators.get('VWAP_prev')): continue
            stats["trend_pass"] += 1
            
            # 3. MFI 動能檢查
            if not is_mfi14_rising(indicators.get('mfi14') or indicators.get('MFI'), 
                                 indicators.get('mfi14_prev') or indicators.get('MFI_prev')): continue
            stats["mfi_pass"] += 1
            
            # 4. 突破檢查
            close = safe_num(indicators.get('close'))
            vwap = safe_num(indicators.get('vwap20') or indicators.get('VWAP'))
            poc = safe_num(indicators.get('vp_poc') or indicators.get('POC'))
            is_breakout = (close and vwap and close > vwap) or (close and poc and close > poc)
            if not is_breakout: continue
            stats["breakout_pass"] += 1
            
            stats["final_pass"] += 1
            results.append((code, indicators.get('mfi14') or indicators.get('MFI', 0), indicators))
            
            if idx % 50 == 0:
                progress.update_lines(
                    f"掃描進度: {idx+1}/{total}",
                    f"通過: 量{stats['volume_pass']} 趨勢{stats['trend_pass']} MFI{stats['mfi_pass']} 突破{stats['breakout_pass']}",
                    f"Checking: {code}"
                )
            
    print_flush(f"\n[Debug] 篩選統計: 總數{stats['total']} -> 量{stats['volume_pass']} -> 趨勢{stats['trend_pass']} -> MFI{stats['mfi_pass']} -> 突破{stats['breakout_pass']}")
    
    print_flush(f"✓ 成交量 >= {int(min_volume/1000)}張 且 增量(今>昨) → {stats['volume_pass']}檔")
    print_flush(f"✓ WMA20 & VWAP 雙上揚             → {stats['trend_pass']}檔")
    print_flush(f"✓ MFI14 上揚 (資金流入)             → {stats['mfi_pass']}檔")
    print_flush(f"✓ 價格突破 (Close > VWAP or POC)    → {stats['breakout_pass']}檔")
    
    print_flush(f"\n【三重篩選結果】找到 {len(results)} 檔精選股票")
    print_flush("=" * 80)
    for i, (code, mfi, ind) in enumerate(results[:limit]):
        name = ind.get('name', code)
        print_flush(f"{i+1}. {format_scan_result(code, name, ind)}")
    print_flush("=" * 80)
    return results

def execute_kd_golden_scan():
    """月KD交叉掃描 (K↑穿越D↑ 或 D↑穿越K↑)"""
    # 1. 獲取參數
    limit, min_vol = get_user_scan_params()
    
    print_flush(f"\n正在掃描 月KD交叉 (K↑穿越D↑ 或 D↑穿越K↑)...")
    
    results = []
    data = GLOBAL_INDICATOR_CACHE["data"]
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    for code, ind in data.items():
        try:
            # 成交量過濾
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue

            k = safe_float_preserving_none(ind.get('month_k'))
            d = safe_float_preserving_none(ind.get('month_d'))
            k_prev = safe_float_preserving_none(ind.get('month_k_prev'))
            d_prev = safe_float_preserving_none(ind.get('month_d_prev'))
            
            if None in [k, d, k_prev, d_prev]:
                continue
            
            # 判斷趨勢 (是否向上)
            k_rising = k > k_prev
            d_rising = d > d_prev
            
            # 1. K↑穿越D↑ (黃金交叉且雙線向上)
            if (k > d and k_prev <= d_prev) and k_rising and d_rising:
                results.append((code, k, ind, "K↑穿越D↑"))
                
            # 2. D↑穿越K↑ (D 向上穿越 K，且雙線向上)
            elif (d > k and d_prev <= k_prev) and d_rising and k_rising:
                results.append((code, k, ind, "D↑穿越K↑"))
                
        except: continue
        
    # 按 K 值由小到大排序 (0% -> 100%)
    results.sort(key=lambda x: x[1]) 
    
    print_flush(f"\n月KD交叉: 找到 {len(results)} 檔符合條件的股票 (顯示前{limit}檔)")
    print_flush(f"排序方式: K值由小到大 (0% -> 100%)")
    print_flush("=" * 80)
    
    for i, (code, k, ind, type_str) in enumerate(results[:limit]):
        name = get_correct_stock_name(code, ind.get('name', code))
        # 顯示額外資訊
        extra_info = f"{type_str} MK:{k:.1f} MD:{ind.get('month_d'):.1f}"
        print_flush(f"{i+1}. {format_scan_result(code, name, ind)} [{extra_info}]")
    
    print_flush("=" * 80)
    print_flush(f"[顯示檔數: {min(limit, len(results))}/{len(results)}]")

def scan_ma_alignment_rising(check_price_above=True):
    """均線多頭排列掃描 (MA3 > MA20 > MA60 > MA120 > MA200)"""
    # 1. 獲取參數
    limit, min_vol = get_user_scan_params()

    title = "均線多頭且股價在均線之上" if check_price_above else "均線多頭排列"
    print_flush(f"\n正在掃描 {title}...")
    
    results = []
    data = GLOBAL_INDICATOR_CACHE["data"]
    if not data:
        print_flush("❌ 無指標數據，請先執行資料更新")
        return

    for code, ind in data.items():
        try:
            # 成交量過濾
            vol = safe_float_preserving_none(ind.get('volume', 0))
            if vol is None or vol < min_vol:
                continue

            close = safe_float_preserving_none(ind.get('close'))
            ma3 = safe_float_preserving_none(ind.get('ma3'))
            ma20 = safe_float_preserving_none(ind.get('ma20'))
            ma60 = safe_float_preserving_none(ind.get('ma60'))
            ma120 = safe_float_preserving_none(ind.get('ma120'))
            ma200 = safe_float_preserving_none(ind.get('ma200'))
            
            if None in [close, ma3, ma20, ma60, ma120, ma200]:
                continue
                
            # 多頭排列: MA3 > MA20 > MA60 > MA120 > MA200
            is_alignment = (ma3 > ma20 > ma60 > ma120 > ma200)
            
            # 股價在均線之上: Close > MA3 (因為 MA3 是最高的)
            is_above = (close > ma3) if check_price_above else True
            
            if is_alignment and is_above:
                results.append((code, 0, ind)) # 0 is placeholder for sort key
                
        except: continue
        
    print_flush(f"\n{title}: 找到 {len(results)} 檔符合條件的股票 (顯示前{limit}檔)")
    print_flush("=" * 80)
    
    for i, (code, _, ind) in enumerate(results[:limit]):
        name = get_correct_stock_name(code, ind.get('name', code))
        print_flush(f"{i+1}. {format_scan_result(code, name, ind)}")
    
    print_flush("=" * 80)
    print_flush(f"[顯示檔數: {min(limit, len(results))}/{len(results)}]")

def triple_filter_scan():
    """三重篩選入口"""
    # 1. 獲取參數
    limit, min_vol = get_user_scan_params()

    title = "三重篩選 (進階版)"
    print_flush(f"◇ 正在執行{title}... (最小成交量: {min_vol}張, 使用 {len(GLOBAL_INDICATOR_CACHE['data'])} 檔指標)")
    
    results = scan_triple_filter_mode_v32(GLOBAL_INDICATOR_CACHE["data"], min_volume=min_vol, limit=limit)

# ==============================
# 輔助判斷函數
# ==============================
def is_wma20_rising(curr, prev):
    return curr is not None and prev is not None and curr > prev

def is_vwap20_rising(curr, prev):
    return curr is not None and prev is not None and curr > prev

def is_mfi14_rising(curr, prev):
    return curr is not None and prev is not None and curr > prev

def get_display_limit():
    return 20

def get_volume_limit():
    return 1000

def get_user_scan_params():
    """獲取使用者輸入的掃描參數 (檔數與成交量)"""
    try:
        print("選擇檔數(預設30檔): ", end='', flush=True)
        l = sys.stdin.readline().strip()
        limit = int(l) if l else 30
    except: limit = 30
    
    try:
        print("大於成交量(預設大於100張): ", end='', flush=True)
        v = sys.stdin.readline().strip()
        min_vol_lots = int(v) if v else 100
        min_vol = min_vol_lots * 1000 # 轉換為股數
    except: min_vol = 100 * 1000
    
    return limit, min_vol

# ==============================
# 市場掃描功能 (還原)
# ==============================
def scan_mfi_divergence():
    print_flush("\n[掃描] MFI 背離偵測 (高檔背離/低檔背離)...")
    try:
        with db_manager.get_connection() as conn:
            # 讀取最近資料
            df = pd.read_sql("SELECT * FROM stock_data WHERE date >= date('now', '-60 days')", conn)
        
        if df.empty:
            print_flush("❌ 無資料可掃描")
            return

        results = []
        codes = df['code'].unique()
        
        for code in codes:
            sub = df[df['code'] == code].sort_values('date')
            if len(sub) < 20: continue
            
            curr_close = sub.iloc[-1]['close']
            curr_mfi = sub.iloc[-1]['mfi14']
            
            # 簡單背離邏輯: 價格創新高但 MFI 未創新高 (高檔背離)
            # 或 價格創新低但 MFI 未創新低 (低檔背離)
            # 這裡實作一個簡化版本
            if curr_mfi > 80:
                results.append((code, sub.iloc[-1]['name'], "MFI超買", curr_mfi))
            elif curr_mfi < 20:
                results.append((code, sub.iloc[-1]['name'], "MFI超賣", curr_mfi))
                
        print_flush(f"掃描完成，發現 {len(results)} 檔潛在訊號")
        for res in results[:10]:
            print_flush(f"  {res[0]} {res[1]}: {res[2]} ({res[3]:.1f})")
            
    except Exception as e:
        print_flush(f"❌ 掃描失敗: {e}")

def scan_volume_anomaly():
    print_flush("\n[掃描] 成交量異常 (爆量/縮量)...")
    try:
        with db_manager.get_connection() as conn:
            # 取得最新兩天資料
            dates = pd.read_sql("SELECT DISTINCT date FROM stock_data ORDER BY date DESC LIMIT 2", conn)['date'].tolist()
            if len(dates) < 2:
                print_flush("❌ 資料不足")
                return
                
            curr_date = dates[0]
            prev_date = dates[1]
            
            query = f"""
                SELECT T1.code, T1.name, T1.volume as vol_curr, T2.volume as vol_prev
                FROM stock_data T1
                JOIN stock_data T2 ON T1.code = T2.code
                WHERE T1.date = '{curr_date}' AND T2.date = '{prev_date}'
            """
            df = pd.read_sql(query, conn)
            
        anomalies = []
        for _, row in df.iterrows():
            if row['vol_prev'] > 0:
                ratio = row['vol_curr'] / row['vol_prev']
                if ratio > 3.0 and row['vol_curr'] > 1000000: # 3倍爆量且大於1000張
                    anomalies.append((row['code'], row['name'], "爆量", ratio))
                elif ratio < 0.3 and row['vol_prev'] > 1000000: # 0.3倍縮量
                    anomalies.append((row['code'], row['name'], "急縮", ratio))
                    
        print_flush(f"掃描完成，發現 {len(anomalies)} 檔異常")
        for res in sorted(anomalies, key=lambda x: x[3], reverse=True)[:10]:
            print_flush(f"  {res[0]} {res[1]}: {res[2]} {res[3]:.1f}倍")
            
    except Exception as e:
        print_flush(f"❌ 掃描失敗: {e}")

def scan_price_anomaly():
    print_flush("\n[掃描] 價格異常 (急漲/急跌)...")
    try:
        with db_manager.get_connection() as conn:
            # 取得最新資料
            df = pd.read_sql("SELECT * FROM stock_data WHERE date = (SELECT MAX(date) FROM stock_data)", conn)
            
        if df.empty: return
        
        # 計算漲跌幅
        df['pct_chg'] = (df['close'] - df['close_prev']) / df['close_prev'] * 100
        
        up = df[df['pct_chg'] > 9.0]
        down = df[df['pct_chg'] < -9.0]
        
        print_flush(f"漲停/急漲 (>9%): {len(up)} 檔")
        for _, row in up.head(5).iterrows():
            print_flush(f"  {row['code']} {row['name']}: +{row['pct_chg']:.2f}%")
            
        print_flush(f"跌停/急跌 (<-9%): {len(down)} 檔")
        for _, row in down.head(5).iterrows():
            print_flush(f"  {row['code']} {row['name']}: {row['pct_chg']:.2f}%")
            
    except Exception as e:
        print_flush(f"❌ 掃描失敗: {e}")

def scan_comprehensive():
    print_flush("\n[掃描] 綜合掃描 (MFI + 成交量 + 價格)...")
    scan_mfi_divergence()
    scan_volume_anomaly()
    scan_price_anomaly()

# ==============================
# 選單系統
# ==============================
def market_scan_menu():
    """市場掃描主選單"""
    while True:
        print_flush("\n" + "="*80)
        print_flush("【市場掃描】")
        print_flush("="*80)
        print_flush("[1] VP掃描")
        print_flush("[2] MFI掃描")
        print_flush("[3] 均線掃描")
        print_flush("[4] 三重篩選 (進階版)")
        print_flush("[5] 月KD交叉 (K↑穿越D↑ 或 D↑穿越K↑)")
        print_flush("[6] 均線多頭且股價在均線之上 (MA3/20/60/120/200)")
        print_flush("[7] 聰明錢掃描 (Smart Score >= 3)")
        print_flush("[8] 重新載入指標")
        print_flush("[0] 返回主選單")
        
        ch = read_single_key()
        
        # 檢查快取
        if ch in ['1', '2', '3', '4', '5', '6', '7'] and not GLOBAL_INDICATOR_CACHE["data"]:
            print_flush("\n正在載入指標...")
            GLOBAL_INDICATOR_CACHE["data"] = step4_load_data()
        
        if ch == '1':
            vp_scan_submenu()
        elif ch == '2':
            mfi_scan_submenu()
        elif ch == '3':
            ma_scan_submenu()
        elif ch == '4':
            triple_filter_scan()
        elif ch == '5':
            execute_kd_golden_scan()
        elif ch == '6':
            scan_ma_alignment_rising(check_price_above=True)
        elif ch == '7':
            scan_smart_money_strategy()
        elif ch == '8':
            print_flush("\n正在重新載入指標...")
            GLOBAL_INDICATOR_CACHE["data"] = step4_load_data()
        elif ch == '0':
            break

def vp_scan_submenu():
    """VP掃描子選單"""
    print_flush("\n【VP掃描】")
    print_flush(f"[已載入指標: {len(GLOBAL_INDICATOR_CACHE['data'])} 檔]")
    print_flush("[1] VP 接近下緣 (支撐)")
    print_flush("[2] VP 接近上緣 (壓力)")
    print_flush("[0] 返回")
    
    ch = read_single_key()
    if ch == '0': return
    
    mode = 'lower' if ch == '1' else 'upper'
    title = "VP 接近下緣 (支撐)" if mode == 'lower' else "VP 接近上緣 (壓力)"
    
    if ch in ['1', '2']:
        # 1. 獲取參數
        limit, min_vol = get_user_scan_params()

        print_flush(f"\n正在掃描 {title}...")
        res = scan_vp(GLOBAL_INDICATOR_CACHE["data"], mode, min_volume=min_vol)
        
        print_flush(f"\n{title}: 找到 {len(res)} 檔符合條件的股票 (顯示前{limit}檔)")
        print_flush("=" * 80)
        for i, (code, pct, ind) in enumerate(res[:limit]):
            print_flush(f"{i+1}. {format_scan_result(code, ind['name'], ind)}")
        print_flush("=" * 80)
        print_flush(f"[顯示檔數: {min(limit, len(res))}/{len(res)}]")

def mfi_scan_submenu():
    """MFI掃描子選單"""
    print_flush("\n【MFI掃描】")
    print_flush(f"[已載入指標: {len(GLOBAL_INDICATOR_CACHE['data'])} 檔]")
    print_flush("[1] MFI由小→大 (資金流入開始)")
    print_flush("[2] MFI由大→小 (資金流出結束)")
    print_flush("[0] 返回")
    
    ch = read_single_key()
    if ch == '0': return
    
    if ch in ['1', '2']:
        # 1. 獲取參數
        limit, min_vol = get_user_scan_params()

        order = 'asc' if ch == '1' else 'desc'
        title = "MFI由小→大 (資金流入開始)" if order == 'asc' else "MFI由大→小 (資金流出結束)"
        
        print_flush(f"\n正在掃描 {title}...")
        results = scan_mfi_mode(GLOBAL_INDICATOR_CACHE["data"], order=order, min_volume=min_vol)
        
        print_flush(f"\n{title}: 找到 {len(results)} 檔符合條件的股票 (顯示前{limit}檔)")
        print_flush("=" * 80)
        
        for i, (code, mfi, indicators) in enumerate(results[:limit], 1):
            name = get_correct_stock_name(code, indicators.get('name', code))
            print_flush(f"\n{i}. {format_scan_result(code, name, indicators)}")
        
        print_flush("=" * 80)
        print_flush(f"[顯示檔數: {min(limit, len(results))}/{len(results)}]")

def ma_scan_submenu():
    """均線掃描子選單"""
    print_flush("\n【均線掃描】")
    print_flush(f"[已載入指標: {len(GLOBAL_INDICATOR_CACHE['data'])} 檔]")
    print_flush("[1] 低於MA200 -0%~-10%")
    print_flush("[2] 低於MA20 -0%~-10%")
    print_flush("[0] 返回")
    
    ch = read_single_key()
    if ch == '0': return
    
    if ch in ['1', '2']:
        # 1. 獲取參數
        limit, min_vol = get_user_scan_params()

        ma_type = 'MA200' if ch == '1' else 'MA20'
        title = f"低於{ma_type} -0%~-10%"
        
        print_flush(f"\n正在掃描 {title}...")
        results = scan_ma_mode(GLOBAL_INDICATOR_CACHE["data"], ma_type=ma_type, min_volume=min_vol)
        
        print_flush(f"\n{title}: 找到 {len(results)} 檔符合條件的股票 (顯示前{limit}檔)")
        print_flush("=" * 80)
        
        for i, (code, pct, indicators) in enumerate(results[:limit], 1):
            name = get_correct_stock_name(code, indicators.get('name', code))
            print_flush(f"\n{i}. {format_scan_result(code, name, indicators)}")
        
        print_flush("=" * 80)
        print_flush(f"[顯示檔數: {min(limit, len(results))}/{len(results)}]")

def data_management_menu():
    """資料管理子選單"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("【資料管理與更新】")
        print_flush("="*60)
        print_flush("[1] 步驟1: 更新上市櫃清單")
        print_flush("[2] 步驟2: 下載 TPEx (上櫃)")
        print_flush("[3] 步驟3: 下載 TWSE (上市)")
        print_flush("[4] 步驟4: 檢查數據缺失")
        print_flush("[5] 步驟5: 清理下市股票")
        print_flush("[6] 步驟6: 驗證一致性並補漏 (斷點續抓)")
        print_flush("[7] 步驟7: 計算技術指標")
        print_flush("-" * 60)
        print_flush("[8] 一鍵執行每日更新 (Steps 1->2->3->4->5->6->7)")
        print_flush("[9] 快速更新（僅 2->3->7，跳過補漏）")
        print_flush("[0] 返回主選單")

        ch = read_single_key()

        if ch == '1': step1_fetch_stock_list()
        elif ch == '2': step2_download_tpex_daily()
        elif ch == '3': step3_download_twse_daily()
        elif ch == '4': step4_check_data_gaps()
        elif ch == '5': step5_clean_delisted()
        elif ch == '6': step6_verify_and_backfill(resume=True)
        elif ch == '7': 
            step7_calc_indicators()
            GLOBAL_INDICATOR_CACHE["data"] = {}
            print_flush("✓ 系統快取已清除")
        elif ch == '8':
            step1_fetch_stock_list()  # 先更新清單
            updated_codes = set()
            
            s2 = step2_download_tpex_daily()
            if isinstance(s2, set): updated_codes.update(s2)
            
            s3 = step3_download_twse_daily()
            if isinstance(s3, set): updated_codes.update(s3)
            
            step5_clean_delisted()  # 有清單才能正確清理
            step4_check_data_gaps() # 新增: 檢查數據缺失
            data = step4_load_data()
            
            s6 = step6_verify_and_backfill(data, resume=True) # 修正: 啟用斷點續傳
            if isinstance(s6, set): updated_codes.update(s6)
            
            # [修正] 不使用 target_codes，強制 Step 7 掃描所有股票
            # Step 7 內已有嚴格的 SQL 檢查，若指標已存在會自動跳過，不會浪費時間
            # 這樣能確保即使 Step 6 沒更新 (例如只有價格但缺指標)，Step 7 也能補算
            step7_calc_indicators(data)
            
            # [新增] 更新後清除快取，確保掃描功能讀到最新數據
            GLOBAL_INDICATOR_CACHE["data"] = {}
            print_flush("✓ 系統快取已清除，下次掃描將重新載入最新數據")
        elif ch == '9':
            step2_download_tpex_daily()
            step3_download_twse_daily()
            step7_calc_indicators()
            GLOBAL_INDICATOR_CACHE["data"] = {}
            print_flush("✓ 系統快取已清除")
        elif ch == '0': break

def step6_verify_and_backfill(data=None, resume=False):
    """驗證資料完整性與回補 - 支援斷點續抓"""
    print_flush("\n[Step 6] 驗證資料完整性與回補...")
    
    if data is None:
        data = step4_load_data()
    
    # 收集需要回補的股票
    tasks = []
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        for code, info in data.items():
            cur.execute("SELECT COUNT(*) FROM stock_data WHERE code=?", (code,))
            count = cur.fetchone()[0]
            if count < MIN_DATA_COUNT:
                # [優化] 加入最新日期以供比對
                tasks.append((code, info['name'], count, info['date']))
    
    if not tasks:
        print_flush(f"✓ 所有股票資料完整 (皆 >= {MIN_DATA_COUNT} 筆)")
        return set()

    # 讀取進度
    progress = load_progress()
    start_idx = progress.get("last_code_index", 0) if resume else 0
    
    # 重置進度（如果不是 resume）
    if not resume:
        save_progress(last_idx=0)
        start_idx = 0
    
    # [修正] 檢查進度索引是否有效
    if start_idx >= len(tasks):
        print_flush(f"⚠ 進度紀錄 ({start_idx}) 超出當前任務範圍 ({len(tasks)})，重置進度從頭開始...")
        start_idx = 0
        save_progress(last_idx=0)
    
    print_flush(f"⚠ 發現 {len(tasks)} 檔股票資料不足，開始回補...")
    if start_idx > 0:
        print_flush(f"📍 從第 {start_idx+1} 檔繼續（已完成 {start_idx} 檔）")
    
    # 使用DataSourceManager進行回補
    data_source_manager = DataSourceManager(silent=True)
    
    tracker = ProgressTracker(total_lines=3)
    
    success_count = 0
    fail_count = 0
    skip_count = start_idx
    
    updated_codes = set()
    
    with tracker:
        # 優化: 移出迴圈，避免重複請求
        latest_date = get_latest_market_date()
        end_date = latest_date
        start_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=1095)).strftime("%Y-%m-%d")
        
        for i in range(start_idx, len(tasks)):
            code, name, count, last_date = tasks[i]
            
            # [省錢優化] 如果資料庫最新日期等於市場最新日期，代表今日已更新過，跳過 API
            if last_date == latest_date:
                tracker.update_lines(
                    f"正在回補: {code} {name}",
                    f"目前筆數: {count} -> 目標: {MIN_DATA_COUNT}",
                    f"狀態: 今日已更新 (跳過 API 請求)"
                )
                success_count += 1
                updated_codes.add(code) # 仍加入更新列表以觸發計算
                save_progress(last_idx=i)
                continue

            tracker.update_lines(
                f"正在回補: {code} {name}",
                f"目前筆數: {count} -> 目標: {MIN_DATA_COUNT}",
                f"進度: {i+1}/{len(tasks)} | 成功: {success_count} | 失敗: {fail_count}"
            )
            
            # Debug info
            # print_flush(f"  -> Fetching {code} ({start_date} ~ {end_date})...")
            
            df = data_source_manager.fetch_history(code, start_date, end_date)
            
            if df is not None and not df.empty:
                try:
                    with db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        inserted = 0
                        for _, row in df.iterrows():
                            cur.execute("""
                                INSERT OR IGNORE INTO stock_data 
                                (code, name, date, open, high, low, close, volume)
                                VALUES (?,?,?,?,?,?,?,?)
                            """, (code, name, row['date'], row.get('open'), row.get('high'), 
                                  row.get('low'), row.get('close'), row.get('volume')))
                            if cur.rowcount > 0:
                                inserted += 1
                        conn.commit()
                    if inserted > 0:
                        success_count += 1
                        updated_codes.add(code)
                        # print_flush(f"  -> Inserted {inserted} rows")
                    else:
                        # [修正] 如果有抓到資料但沒新增，代表已是最新 (可能是新股)
                        # 視為成功，並加入更新列表以觸發 Step 7 (回應使用者期待)
                        success_count += 1
                        updated_codes.add(code)
                        tracker.update_lines(
                            f"正在回補: {code} {name}",
                            f"目前筆數: {count} -> 目標: {MIN_DATA_COUNT}",
                            f"狀態: 資料已是最新 (可能是新上市股)"
                        )
                        # fail_count += 1
                        # print_flush(f"  -> No new rows inserted (Data overlap)")
                except Exception as e:
                    fail_count += 1
                    logging.error(f"DB Write Error {code}: {e}")
            else:
                # 真的抓不到資料才算失敗
                fail_count += 1
                # print_flush(f"  -> Fetch failed or empty")
            
            # 每完成一檔就保存進度
            save_progress(last_idx=i)
                            pending_updates.append((
                                ind.get('MA3'), ind.get('MA20'), ind.get('MA60'), ind.get('MA120'), ind.get('MA200'),
                                ind.get('WMA3'), ind.get('WMA20'), ind.get('WMA60'), ind.get('WMA120'), ind.get('WMA200'),
                                ind.get('MFI'), ind.get('VWAP'), ind.get('CHG14'), ind.get('RSI'), ind.get('MACD'), ind.get('SIGNAL'),
                                ind.get('POC'), ind.get('VP_upper'), ind.get('VP_lower'),
                                ind.get('Month_K'), ind.get('Month_D'),
                                ind.get('Daily_K'), ind.get('Daily_D'),
                                ind.get('Week_K'), ind.get('Week_D'),
                                ind.get('MA3_prev'), ind.get('MA20_prev'), ind.get('MA60_prev'), ind.get('MA120_prev'), ind.get('MA200_prev'),
                                ind.get('WMA3_prev'), ind.get('WMA20_prev'), ind.get('WMA60_prev'), ind.get('WMA120_prev'), ind.get('WMA200_prev'),
                                ind.get('MFI_prev'), ind.get('VWAP_prev'), ind.get('CHG14_prev'),
                                ind.get('Month_K_prev'), ind.get('Month_D_prev'),
                                ind.get('Daily_K_prev'), ind.get('Daily_D_prev'),
                                ind.get('Week_K_prev'), ind.get('Week_D_prev'),
                                ind.get('close_prev'), ind.get('vol_prev'),
                                code, ind.get('date'),
                                ind.get('Smart_Score'), ind.get('SMI_Signal'), ind.get('NVI_Signal'), ind.get('VSA_Signal'), ind.get('SVI_Signal')
                            ))
                    
                    status_msg = "計算完成" if indicators_list else "無數據/失敗"
                    
                except Exception as e:
                    status_msg = f"錯誤: {e}"
                    # print_flush(f"Worker Error {code}: {e}") # 避免刷屏
                
                # 更新進度顯示
                tracker.update_lines(
                    f'正在計算: {code} {name}',
                    f'進度: {i+1}/{total} (Buffer: {len(pending_updates)})',
                    f'狀態: {status_msg}'
                )
                
                # 批量寫入資料庫 (主執行緒執行，避免鎖死)
                if len(pending_updates) >= batch_size or (i == total - 1 and pending_updates):
                    try:
                        with db_manager.get_connection() as conn:
                            cur = conn.cursor()
                            cur.executemany("""
                                UPDATE stock_data SET
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
                                    smart_score=?, smi_signal=?, nvi_signal=?, vsa_signal=?, svi_signal=?
                                WHERE code=? AND date=?
                            """, pending_updates)
                            conn.commit()
                    except Exception as e:
                        # 顯示錯誤但不中斷，並確保 buffer 被清空
                        tracker.update_lines(
                            f'正在計算: {code} {name}',
                            f'進度: {i+1}/{total} (Buffer: {len(pending_updates)})',
                            f'狀態: 寫入失敗! {e}'
                        )
                        print_flush(f"\n❌ 批量寫入錯誤: {e}")
                    finally:
                        # 無論成功或失敗，都清空緩衝區，避免無限重試導致卡死
                        pending_updates.clear()
            
    print_flush(f"✓ 已完成 {total} 檔股票的指標計算與寫入")
    return step4_load_data()

def maintenance_menu():
    """系統維護選單"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("【系統維護】")
        print_flush("="*60)
        print_flush("[1] 資料庫備份與還原")
        print_flush("[2] 刪除指定日期資料")
        print_flush("[3] 檢查 API 連線狀態")
        print_flush("[4] 檢查資料庫空值率")
        print_flush("[5] 強制重算所有指標 (修復數據用)")
        print_flush("[0] 返回主選單")
        
        choice = read_single_key("請選擇: ")
        
        if choice == '1': backup_menu()
        elif choice == '2': delete_data_by_date()
        elif choice == '3': check_api_status()
        elif choice == '4': check_db_nulls()
        elif choice == '5':
            print_flush("\n⚠ 警告: 此操作將重新計算所有股票的指標，可能需要較長時間 (約 10-20 分鐘)。")
            confirm = input("確定要執行嗎? (y/n) [預設y]: ").strip().lower()
            if not confirm or confirm == 'y':
                step7_calc_indicators(force=True)
            else:
                print_flush("已取消")
        elif choice == '0': break

def integrated_quick_integrity_check():
    print_flush("\n[快速檢查] 執行中...")
    step4_check_data_gaps()

def integrated_full_integrity_check(days=250):
    print_flush(f"\n[全面檢查] 檢查最近 {days} 天數據...")
    step4_check_data_gaps()

def integrated_architecture_diagnosis():
    print_flush("\n[架構診斷] 檢查資料庫結構...")
    try:
        ensure_db()
        print_flush("✓ 資料庫結構正常")
    except Exception as e:
        print_flush(f"❌ 架構異常: {e}")

def integrated_fix_missing_dates():
    print_flush("\n[修復] 功能尚未實作")

def check_db_nulls():
    """檢查資料庫空值率"""
    print_flush("\n[檢查] 資料庫空值率分析...")
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all columns
            cursor.execute("PRAGMA table_info(stock_data)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Get latest date
            cursor.execute("SELECT MAX(date) FROM stock_data")
            latest_date = cursor.fetchone()[0]
            
            # Count total rows and latest rows
            cursor.execute("SELECT COUNT(*) FROM stock_data")
            total_rows = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM stock_data WHERE date=?", (latest_date,))
            latest_rows = cursor.fetchone()[0]
            
            if total_rows == 0:
                print_flush("❌ 資料庫無數據")
                return

            print_flush(f"分析範圍: 全歷史 ({total_rows} 筆) vs 最新日期 {latest_date} ({latest_rows} 筆)")
            print_flush("-" * 75)
            print_flush(f"{'欄位名稱':<20} | {'全歷史空值%':<12} | {'最新日空值%':<12} | {'狀態':<10}")
            print_flush("-" * 75)
            
            # Check nulls for each column
            for col in columns:
                # 隱藏廢棄欄位
                if col in ['kd_k', 'kd_d', 'kd_golden_cross']:
                    continue
                    
                # 全歷史空值
                cursor.execute(f"SELECT COUNT(*) FROM stock_data WHERE {col} IS NULL")
                null_count = cursor.fetchone()[0]
                null_pct = (null_count / total_rows) * 100
                
                # 最新日空值
                cursor.execute(f"SELECT COUNT(*) FROM stock_data WHERE date=? AND {col} IS NULL", (latest_date,))
                latest_null_count = cursor.fetchone()[0]
                latest_null_pct = (latest_null_count / latest_rows) * 100
                
                # 狀態判斷
                status = "OK"
                if latest_null_pct > 10:
                    status = "缺資料 (!)"
                elif latest_null_pct > 0:
                    status = "部分缺"
                
                # 格式化輸出
                print_flush(f"{col:<20} | {null_pct:<10.2f}% | {latest_null_pct:<10.2f}% | {status}")
                
            print_flush("-" * 75)
            print_flush("說明:")
            print_flush("1. [全歷史空值%] 高 (如 MA200 約 40%) 是正常的，代表早期資料不足無法計算。")
            print_flush("2. [最新日空值%] 應接近 0%。若 MA200 在最新日仍有空值，代表該股上市未滿 200 天。")
            
            print_flush("\n" + "="*50)
            print_flush("是否立即執行 [1]~[7] 完整更新以修復缺失數據？輸入y/n (預設 y):")
            ans = input("如果輸入y就執行，輸入n就返回選單: ").strip().lower()
            if not ans or ans == 'y':
                # 呼叫一鍵更新邏輯 (Option 8 的邏輯)
                step1_fetch_stock_list()
                updated_codes = set()
                s2 = step2_download_tpex_daily()
                if isinstance(s2, set): updated_codes.update(s2)
                s3 = step3_download_twse_daily()
                if isinstance(s3, set): updated_codes.update(s3)
                step5_clean_delisted()
                step4_check_data_gaps()
                data = step4_load_data()
                step6_verify_and_backfill(data, resume=True)
                step7_calc_indicators(data)
                GLOBAL_INDICATOR_CACHE["data"] = {}
                print_flush("✓ 系統快取已清除，更新完成")
            
    except Exception as e:
        print_flush(f"❌ 檢查失敗: {e}")

def diagnostic_menu():
    """診斷與修復選單"""
    while True:
        print_flush("\n" + "="*60)
        print_flush("🔧 診斷與修復")
        print_flush("="*60)
        print_flush("[1] 快速完整性檢查 (最近30天)")
        print_flush("[2] 全面完整性檢查與修復 (最近{MIN_DATA_COUNT}天)")
        print_flush("[3] 架構診斷與異常檢測")
        print_flush("[4] 修復指定股票缺失日期")
        print_flush("[0] 返回主選單")
        
        choice = read_single_key("請選擇: ")
        
        if choice == '1':
            integrated_quick_integrity_check()
        elif choice == '2':
            integrated_full_integrity_check(days=MIN_DATA_COUNT)
        elif choice == '3':
            integrated_architecture_diagnosis()
        elif choice == '4':
            integrated_fix_missing_dates()
        elif choice == '0':
            break

def main_menu():
    # 初始化資料庫
    try:
        ensure_db()
    except Exception as e: print(f"DB Init Error: {e}")

    first_run = True
    while True:
        if first_run:
            display_system_status() # 恢復顯示，但已優化不檢查 API
            first_run = False

        print_flush("\n" + "="*80)
        print_flush("台灣股票分析系統 v40 (Fast Timeout)")
        print_flush("="*80)
        print_flush("[1] 資料管理 (更新/補漏/計算)")
        print_flush("[2] 市場掃描 (策略/篩選)")
        print_flush("[3] 系統維護與診斷 (備份/檢查)")
        print_flush("[0] 離開")
        print_flush("-" * 80)
        print_flush("提示: 直接輸入股號 (如 2330) 可查詢個股")
        
        # 標準輸入邏輯 (需按 Enter)
        try:
            choice = input("請選擇: ").strip()
        except EOFError:
            break
        
        if choice == '1': data_management_menu()
        elif choice == '2': market_scan_menu()
        elif choice == '3': maintenance_menu()
        elif choice == '0': sys.exit(0)
        elif len(choice) == 4 and choice.isdigit():
            name = get_correct_stock_name(choice)
            
            # 恢復詢問顯示天數
            print("顯示天數(預設30天): ", end='', flush=True)
            days_input = ""
            start_time = time.time()
            while True:
                if msvcrt.kbhit():
                    ch = msvcrt.getch()
                    try:
                        decoded = ch.decode('utf-8')
                        if decoded == '\r': # Enter
                            print()
                            break
                        if decoded.isdigit():
                            print(decoded, end='', flush=True)
                            days_input += decoded
                    except: pass
                if time.time() - start_time > 5 and not days_input: # 5秒無輸入自動預設
                    print()
                    break
                time.sleep(0.05)
            
            days = int(days_input) if days_input else 30
            
            res = calculate_stock_history_indicators(choice, display_days=days)
            if res:
                print_flush(f"\n【{choice} {name}】近期走勢:")
                print_flush(format_scan_result_list(choice, name, res))
            else:
                print_flush("❌ 查無資料")
                time.sleep(1)
        elif len(choice) > 1:
             print_flush("❌ 輸入無效")
             time.sleep(0.5)

def load_indicators_cache():
    """載入指標快取 (只讀取 DB，不強制重算)"""
    print_flush("正在載入指標數據...")
    data = step4_load_data()
    if not data:
        return {}
    return data

if __name__ == "__main__":
    main_menu()