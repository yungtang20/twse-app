#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
寶塔線專用掃描程式 (獨立版)
功能: 掃描資料庫中符合寶塔線轉折點條件的股票
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
import time
import math
from datetime import datetime, timedelta  # 修正：加入 timedelta 的 import
import colorama
from colorama import Fore, Style

# 初始化顏色
colorama.init()

# 資料庫路徑 (請根據您的環境調整)
DB_FILE = "taiwan_stock.db"

def calculate_tower_line(df, window=3):
    """
    計算寶塔線指標 (TOWER)
    df: DataFrame 包含 'close' 欄位
    window: 計算窗口，預設3天
    回傳: 帶有 'tower' 欄位的 DataFrame (1=紅/漲, -1=黑/跌, 0=無信號)
    """
    if len(df) < window + 1:
        return df
    
    df = df.copy()
    df['tower'] = 0
    
    # 計算前n天的最高價和最低價
    df['prev_high'] = df['close'].rolling(window=window).max().shift(1)
    df['prev_low'] = df['close'].rolling(window=window).min().shift(1)
    
    # 填充第一行
    if pd.isna(df['prev_high'].iloc[window-1]):
        first_window = df.iloc[:window]
        df.loc[df.index[window-1], 'prev_high'] = first_window['close'].max()
        df.loc[df.index[window-1], 'prev_low'] = first_window['close'].min()
    
    # 計算寶塔線
    for i in range(window, len(df)):
        current_close = df['close'].iloc[i]
        prev_high = df['prev_high'].iloc[i]
        prev_low = df['prev_low'].iloc[i]
        
        if pd.isna(prev_high) or pd.isna(prev_low):
            continue
            
        if current_close > prev_high:
            df.loc[df.index[i], 'tower'] = 1  # 翻紅（買進）
        elif current_close < prev_low:
            df.loc[df.index[i], 'tower'] = -1  # 翻黑（賣出）
        else:
            # 延續前一狀態
            df.loc[df.index[i], 'tower'] = df['tower'].iloc[i-1]
    
    return df

def scan_tower_turnaround():
    """掃描全資料庫符合寶塔線轉折點條件的個股"""
    # 設定顯示參數
    limit = 30  # 顯示前30筆
    min_vol = 500  # 最小成交量500張
    
    print(f"\n{Fore.CYAN}正在執行全資料庫寶塔線轉折點掃描 (成交量 > {min_vol} 張)...{Style.RESET_ALL}")
    print(f"篩選條件: 1.寶塔線由黑轉紅 2.轉折前連續3天下跌 3.成交量放大1.5倍 4.價格突破5日均線")
    
    # 連接資料庫
    try:
        conn = sqlite3.connect(DB_FILE)
    except Exception as e:
        print(f"{Fore.RED}❌ 資料庫連接失敗: {e}{Style.RESET_ALL}")
        return
    
    # 獲取所有股票代碼
    try:
        cur = conn.cursor()
        cur.execute("SELECT code, name FROM stock_snapshot")
        stocks = cur.fetchall()
    except Exception as e:
        print(f"{Fore.RED}❌ 無法獲取股票清單: {e}{Style.RESET_ALL}")
        conn.close()
        return
    
    # 批次載入歷史資料
    print(f"{Fore.CYAN}正在載入歷史資料...{Style.RESET_ALL}")
    codes = [s[0] for s in stocks]
    history_map = {}
    
    # 設定日期範圍 (過去2年)
    cutoff_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")  # 修正：timedelta 現在已定義
    cutoff_int = int(cutoff_date)
    
    # 執行SQL查詢
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
    
    try:
        df_all = pd.read_sql_query(query, conn, params=params)
        if not df_all.empty:
            df_all['date'] = pd.to_datetime(df_all['date'])
            groups = list(df_all.groupby('code'))
            for i, (code, group) in enumerate(groups):
                history_map[code] = group.reset_index(drop=True)
    except Exception as e:
        print(f"{Fore.RED}❌ 批次載入失敗: {e}{Style.RESET_ALL}")
        conn.close()
        return
    
    results = []
    # 計數器
    count_total = len(stocks)
    count_data = 0
    count_vol = 0
    count_turn = 0
    count_ma = 0
    
    # 掃描每支股票
    for code, name in stocks:
        df = history_map.get(code)
        if df is None or len(df) < 15:  # 需要足夠天數
            continue
        
        count_data += 1
        try:
            # 1. 成交量篩選
            if df['volume'].iloc[-1] < min_vol * 1000:  # 轉換為股
                continue
            count_vol += 1
            
            # 2. 計算寶塔線
            df = calculate_tower_line(df)
            
            # 3. 檢查轉折點
            if len(df) < 5:
                continue
                
            # 取最後5天資料
            recent_df = df.iloc[-5:]
            
            # 檢查寶塔線是否由黑轉紅
            tower_values = recent_df['tower'].values
            has_turnaround = False
            turnaround_idx = -1
            
            for i in range(1, len(tower_values)):
                if tower_values[i-1] == -1 and tower_values[i] == 1:
                    has_turnaround = True
                    turnaround_idx = i - 1  # 轉折點索引
                    break
            
            if not has_turnaround:
                continue
            count_turn += 1
            
            # 4. 檢查轉折前連續下跌
            if turnaround_idx >= 3:  # 確保有足夠前序數據
                prev_tower = df['tower'].iloc[-(turnaround_idx+4):- (turnaround_idx+1)].values
                if not np.all(prev_tower == -1):
                    continue
            else:
                continue
            
            # 5. 檢查成交量放大
            turnaround_date_idx = - (len(tower_values) - turnaround_idx)
            if turnaround_date_idx < -1:  # 確保有前一天
                turnaround_vol = df['volume'].iloc[turnaround_date_idx]
                prev_5_days_vol = df['volume'].iloc[turnaround_date_idx-5:turnaround_date_idx]
                avg_vol = prev_5_days_vol.mean()
                
                if avg_vol > 0 and turnaround_vol < avg_vol * 1.5:
                    continue
            
            # 6. 檢查價格突破5日均線
            df['ma5'] = df['close'].rolling(window=5).mean()
            turnaround_close = df['close'].iloc[turnaround_date_idx]
            turnaround_ma5 = df['ma5'].iloc[turnaround_date_idx]
            
            if turnaround_ma5 > 0 and turnaround_close <= turnaround_ma5:
                continue
            count_ma += 1
            
            # 7. 準備結果
            current = df.iloc[-1]
            prev_close = df['close'].iloc[-2]
            change_pct = (current['close'] - prev_close) / prev_close * 100
            
            vol_ratio = current['volume'] / df['volume'].iloc[-6:-1].mean() if len(df) >= 6 else 1
            
            results.append({
                'code': code,
                'name': name,
                'close': current['close'],
                'close_prev': prev_close,
                'change_pct': change_pct,
                'volume': current['volume'] / 1000,  # 轉換為張
                'vol_ratio': vol_ratio,
                'turnaround_date': df.index[turnaround_date_idx].strftime('%Y-%m-%d')
            })
            
        except Exception as e:
            continue
    
    # 顯示掃描結果
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}[篩選過程] 寶塔線轉折點掃描{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"總股數: {count_total}")
    print(f"{'─'*60}")
    print(f"✓ 資料充足 (>15日)        → {count_data} 檔")
    print(f"✓ 成交量 >= {min_vol}張       → {count_vol} 檔")
    print(f"✓ 寶塔線由黑轉紅          → {count_turn} 檔")
    print(f"✓ 轉折前連續3天下跌        → 自動檢查")
    print(f"✓ 成交量放大1.5倍         → 自動檢查")
    print(f"✓ 價格突破5日均線         → {count_ma} 檔 (最終選出)")
    print(f"{'─'*60}")
    
    if not results:
        print(f"\n{Fore.YELLOW}沒有符合條件的股票。{Style.RESET_ALL}")
        conn.close()
        return
    
    # 依成交量比值排序
    results.sort(key=lambda x: x['vol_ratio'], reverse=True)
    
    print(f"\n{Fore.GREEN}【寶塔線轉折點 TOP】 (前 {limit} 筆){Style.RESET_ALL}")
    header = f"{'代號':<6} {'名稱':<8} {'轉折日':<12} {'收盤':<10} {'漲跌幅%':<10} {'成交量(量比)':<16}"
    print(header)
    print("-" * 80)
    
    for res in results[:limit]:
        # 收盤價顏色
        c_price = Fore.RED if res['close'] > res['close_prev'] else Fore.GREEN
        price_str = f"{c_price}{res['close']:.2f}{Style.RESET_ALL}"
        
        # 漲跌幅顏色
        c_change = Fore.RED if res['change_pct'] > 0 else Fore.GREEN
        change_str = f"{c_change}{res['change_pct']:+.2f}{Style.RESET_ALL}"
        
        # 量比顏色
        c_vol = Fore.MAGENTA if res['vol_ratio'] >= 2.0 else (Fore.RED if res['vol_ratio'] >= 1.5 else Fore.GREEN)
        vol_str = f"{c_vol}{int(res['volume'])}張({res['vol_ratio']:.1f}倍){Style.RESET_ALL}"
        
        print(f"{res['code']:<6} {res['name']:<8} {res['turnaround_date']:<12} {price_str:<19} {change_str:<19} {vol_str:<25}")
    
    conn.close()
    return [r['code'] for r in results[:limit]]

def main():
    """主程式"""
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}【寶塔線轉折點掃描程式】{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    
    # 檢查資料庫是否存在
    if not os.path.exists(DB_FILE):
        print(f"{Fore.RED}❌ 資料庫 {DB_FILE} 不存在，請確認路徑{Style.RESET_ALL}")
        print(f"預設資料庫路徑: {os.path.abspath(DB_FILE)}")
        return
    
    # 執行寶塔線掃描
    scan_tower_turnaround()

if __name__ == "__main__":
    main()