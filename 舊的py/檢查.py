#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台灣股票分析系統 - 資料庫健康檢查工具
檔名: check_db_status.py
功能: 
1. 檢查各類指標 (基本面、技術面、籌碼面) 的欄位完成率。
2. 檢查歷史 K 線的資料長度分佈 (是否滿足 450 天)。
3. 檢查三大法人與融資券的資料覆蓋率。
"""

import sqlite3
import os
import pandas as pd
import sys

# 設定顯示顏色
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

DB_FILE = "taiwan_stock.db"

def get_db():
    if not os.path.exists(DB_FILE):
        print(f"{Colors.FAIL}❌ 找不到資料庫檔案: {DB_FILE}{Colors.ENDC}")
        sys.exit(1)
    return sqlite3.connect(DB_FILE)

def print_bar(percent, width=20):
    """繪製進度條"""
    fill = int(width * percent / 100)
    bar = '█' * fill + '░' * (width - fill)
    color = Colors.GREEN if percent > 95 else (Colors.WARNING if percent > 80 else Colors.FAIL)
    return f"{color}[{bar}]{Colors.ENDC}"

def check_snapshot_completeness():
    """檢查快照表 (stock_snapshot) 各指標完成率"""
    conn = get_db()
    
    try:
        # 讀取總股數 (母體)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_meta")
        total_listed = cursor.fetchone()[0]
        
        print(f"\n{Colors.HEADER}=== 1. 指標完成率檢查 (Stock Snapshot) ==={Colors.ENDC}")
        print(f"應有總股數 (名冊): {total_listed}")
        
        # 讀取快照表
        df = pd.read_sql("SELECT * FROM stock_snapshot", conn)
        snapshot_count = len(df)
        print(f"實際快照數 (已建檔): {snapshot_count} ({snapshot_count/total_listed*100:.1f}%)")
        print("-" * 60)
        print(f"{'指標名稱':<15} | {'完成數':<8} | {'完成率':<8} | {'狀態'}")
        print("-" * 60)
        
        # 定義要檢查的指標群組
        check_groups = {
            "【基本面】": ['close', 'pe', 'pb', 'yield'],
            "【均線系統】": ['ma20', 'ma60', 'ma200'],
            "【技術指標】": ['rsi', 'macd', 'mfi14', 'kd_k' if 'kd_k' in df.columns else 'month_k'], 
            "【籌碼數據】": ['foreign_buy', 'trust_buy', 'margin_balance'],
            "【聰明錢】": ['smart_score', 'nvi', 'vp_poc']
        }
        
        for group_name, cols in check_groups.items():
            print(f"{Colors.BLUE}{group_name}{Colors.ENDC}")
            for col in cols:
                # 相容性檢查 (避免舊 DB 缺欄位報錯)
                if col not in df.columns:
                    # 嘗試找別名
                    if col == 'kd_k' and 'month_k' in df.columns: col = 'month_k'
                    else:
                        print(f"  {col:<13} | {Colors.FAIL}欄位缺失{Colors.ENDC}")
                        continue
                
                # 計算非空值數量 (排除 None, NaN, 0)
                # 注意: 有些指標 0 是有意義的，但大部情況下 0 代表沒計算到
                if col in ['foreign_buy', 'trust_buy']:
                    # 籌碼可以是 0 (沒買賣)，所以只檢查 Not Null
                    valid_count = df[col].notnull().sum()
                else:
                    # 技術指標通常不會剛好是 0 (除了信號類)
                    valid_count = df[col].apply(lambda x: x is not None and x != 0).sum()
                
                pct = (valid_count / total_listed) * 100
                bar = print_bar(pct)
                print(f"  {col:<13} | {valid_count:<8} | {pct:>6.1f}%  | {bar}")
            print("")
            
    except Exception as e:
        print(f"檢查失敗: {e}")
    finally:
        conn.close()

def check_history_depth():
    """檢查歷史資料 (stock_history) 的長度分佈"""
    conn = get_db()
    print(f"{Colors.HEADER}=== 2. K線資料長度檢查 (Stock History) ==={Colors.ENDC}")
    
    try:
        # 統計每檔股票的 K 線筆數
        df = pd.read_sql("SELECT code, COUNT(*) as days FROM stock_history GROUP BY code", conn)
        
        if df.empty:
            print(f"{Colors.FAIL}❌ 歷史資料表為空！{Colors.ENDC}")
            return

        # 分級統計
        bins = [0, 100, 250, 449, 99999]
        labels = ['極短 (<100)', '不足一年 (100-250)', '不足兩年 (250-449)', '充足 (>=450)']
        df['status'] = pd.cut(df['days'], bins=bins, labels=labels)
        
        counts = df['status'].value_counts().sort_index()
        total = len(df)
        
        print(f"有 K 線資料的股票數: {total}")
        print("-" * 60)
        
        for label in labels:
            count = counts.get(label, 0)
            pct = (count / total) * 100 if total > 0 else 0
            
            # 設定顏色：>=450 為綠色，其他為黃色或紅色
            color = Colors.GREEN if '>=450' in label else (Colors.WARNING if '250' in label else Colors.FAIL)
            
            print(f"{label:<20} | {count:<6} 檔 | {pct:>5.1f}% | {color}{'█'*int(pct/5)}{Colors.ENDC}")
            
        print("-" * 60)
        
        # 檢查最近更新日期
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date_int) FROM stock_history")
        last_date = cursor.fetchone()[0]
        print(f"📅 資料庫最新交易日: {Colors.BOLD}{last_date}{Colors.ENDC}")
        
    except Exception as e:
        print(f"檢查失敗: {e}")
    finally:
        conn.close()

def check_other_tables():
    """檢查其他表格狀況"""
    conn = get_db()
    cursor = conn.cursor()
    print(f"\n{Colors.HEADER}=== 3. 關聯資料表檢查 ==={Colors.ENDC}")
    
    tables = {
        'institutional_investors': '三大法人',
        'margin_data': '融資融券',
        'market_index': '大盤指數'
    }
    
    print(f"{'資料表':<25} | {'總筆數':<10} | {'涵蓋日期數'}")
    print("-" * 60)
    
    for table, name in tables.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(DISTINCT date_int) FROM {table}")
            days = cursor.fetchone()[0]
            
            status = Colors.GREEN if count > 0 else Colors.FAIL
            print(f"{name:<25} | {status}{count:<10}{Colors.ENDC} | {days} 天")
        except:
            print(f"{name:<25} | {Colors.FAIL}未建立{Colors.ENDC} | -")
            
    conn.close()

def main():
    os.system('cls' if os.name == 'nt' else 'clear') # 清畫面
    print("="*60)
    print(" 🏥 資料庫健康診斷報告 (Database Health Check)")
    print("="*60)
    
    check_snapshot_completeness()
    check_history_depth()
    check_other_tables()
    
    print("\n" + "="*60)
    print("診斷建議：")
    print(f"1. 若 {Colors.BOLD}【均線系統】{Colors.ENDC} 完成率低 → 請執行 `python 最終修正.py` -> [7] 計算指標。")
    print(f"2. 若 {Colors.BOLD}K線資料 >=450{Colors.ENDC} 比例低 → 請執行 `python patch_update_final.py` 回補。")
    print(f"3. 若 {Colors.BOLD}【基本面】{Colors.ENDC} 缺漏 → 請執行 `python patch_update_full.py`。")
    print("="*60)

if __name__ == "__main__":
    main()
