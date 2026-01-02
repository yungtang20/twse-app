# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 核心配置模組
"""
import os
import sys

# ==============================
# 環境自動偵測
# ==============================
def _detect_android():
    """偵測是否為 Android 環境 (Termux)"""
    # 方法 1: 檢查 Termux 路徑
    if os.path.exists('/data/data/com.termux'):
        return True
    # 方法 2: 檢查環境變數
    if 'ANDROID_ROOT' in os.environ:
        return True
    # 方法 3: 檢查 sys.platform
    if hasattr(sys, 'getandroidapilevel'):
        return True
    return False


class Config:
    """系統全域配置，消除魔術數字"""
    
    # 環境偵測
    IS_ANDROID = _detect_android()
    LIGHTWEIGHT_MODE = IS_ANDROID  # Android 自動啟用輕量模式
    
    # 根據環境調整參數
    if LIGHTWEIGHT_MODE:
        MAX_WORKERS = 2
        BATCH_SIZE = 50
        CACHE_SIZE = 30
    else:
        MAX_WORKERS = 6
        BATCH_SIZE = 200
        CACHE_SIZE = 100
    
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

