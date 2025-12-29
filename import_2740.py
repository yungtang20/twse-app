"""
匯入 2740 天蔥歷史資料 (從 Goodinfo 下載的 HTML 檔)
"""
import os
import pandas as pd
import sqlite3
import glob
from datetime import datetime

DATA_DIR = r'd:\twse\2740'
DB_PATH = r'd:\twse\taiwan_stock.db'
STOCK_CODE = '2740'

def parse_date(date_str):
    """解析 Goodinfo 日期格式"""
    try:
        # 移除可能的引號和空白
        s = str(date_str).replace("'", "").replace('"', '').strip()
        
        if '/' in s:
            parts = s.split('/')
            y = int(parts[0])
            m = int(parts[1])
            d = int(parts[2])
            
            if y >= 1900:
                return y * 10000 + m * 100 + d
            elif y >= 100:
                # ROC (e.g. 112 -> 2023)
                return (1911 + y) * 10000 + m * 100 + d
            else:
                # 2 digits (e.g. 01 -> 2001, 99 -> 1999)
                # 假設 2740 上市於 2015
                if y <= 70:
                    return (2000 + y) * 10000 + m * 100 + d
                else:
                    return (1900 + y) * 10000 + m * 100 + d
        return None
    except:
        return None

def clean_number(val):
    """清除數值中的逗號等非數字字符"""
    if pd.isna(val) or str(val).strip() == '-':
        return None
    try:
        if isinstance(val, str):
            val = val.replace(',', '')
        return float(val)
    except:
        return None

def main():
    print("="*60)
    print(f"開始匯入 {STOCK_CODE} 歷史資料 (目錄: {DATA_DIR})")
    print("="*60)

    files = glob.glob(os.path.join(DATA_DIR, "*.html"))
    print(f"找到 {len(files)} 個檔案")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    total_inserted = 0
    
    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"處理 {filename}...", end="")
        
        try:
            dfs = pd.read_html(file_path, encoding='utf-8')
            
            target_df = None
            for df in dfs:
                if '交易日期' in str(df.columns) or '日期' in str(df.columns):
                    target_df = df
                    break
            
            if target_df is None:
                print(" ⚠ 找不到資料表格")
                continue

            # 統一欄位名稱
            if isinstance(target_df.columns, pd.MultiIndex):
                target_df.columns = target_df.columns.get_level_values(-1)
            
            # 尋找對應欄位
            col_map = {}
            for col in target_df.columns:
                if '日期' in col: col_map['date'] = col
                elif '開盤' in col: col_map['open'] = col
                elif '最高' in col: col_map['high'] = col
                elif '最低' in col: col_map['low'] = col
                elif '收盤' in col: col_map['close'] = col
                elif '張數' in col: col_map['volume'] = col
            
            if 'date' not in col_map or 'close' not in col_map:
                print(" ⚠ 欄位不完整")
                continue

            count = 0
            for _, row in target_df.iterrows():
                date_str = str(row[col_map['date']])
                date_int = parse_date(date_str)
                
                if not date_int:
                    continue
                
                open_price = clean_number(row.get(col_map.get('open')))
                high_price = clean_number(row.get(col_map.get('high')))
                low_price = clean_number(row.get(col_map.get('low')))
                close_price = clean_number(row.get(col_map.get('close')))
                
                # 成交張數 -> 股數 (*1000)
                vol_sheets = clean_number(row.get(col_map.get('volume')))
                volume = int(vol_sheets * 1000) if vol_sheets is not None else 0
                
                if close_price is None:
                    continue

                cur.execute("""
                    INSERT OR REPLACE INTO stock_history 
                    (code, date_int, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (STOCK_CODE, date_int, open_price, high_price, low_price, close_price, volume))
                count += 1
            
            conn.commit()
            print(f" ✓ 匯入 {count} 筆")
            total_inserted += count
            
        except Exception as e:
            print(f" ❌ 錯誤: {e}")

    print("\n" + "="*60)
    print(f"匯入完成！總計匯入 {total_inserted} 筆資料")
    
    # 驗證
    cur.execute("SELECT COUNT(*), MIN(date_int), MAX(date_int) FROM stock_history WHERE code = ?", (STOCK_CODE,))
    row = cur.fetchone()
    print(f"資料庫現有 {STOCK_CODE} 資料: {row[0]} 筆 ({row[1]} ~ {row[2]})")
    
    conn.close()

if __name__ == "__main__":
    main()
