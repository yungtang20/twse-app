"""
從 TDCC 開放資料抓取集保資料並寫入資料庫
URL: https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5
"""

import requests
import sqlite3
import pandas as pd
from io import StringIO
import urllib3
from datetime import datetime

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TDCC_CSV_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"

def fetch_tdcc_data():
    """抓取 TDCC 集保資料"""
    print("正在從 TDCC 抓取集保資料...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/csv,application/csv,*/*'
    }
    
    try:
        resp = requests.get(TDCC_CSV_URL, headers=headers, timeout=60, verify=False)
        resp.raise_for_status()
        
        # 嘗試解析為 CSV
        content = resp.text
        print(f"Response length: {len(content)}")
        
        # 檢查是否為 CSV 格式
        if '證券代號' in content:
            df = pd.read_csv(StringIO(content))
            print(f"成功解析 CSV，共 {len(df)} 筆資料")
            print(f"欄位: {list(df.columns)}")
            return df
        else:
            print(f"回應內容不是預期的 CSV 格式")
            print(f"前 500 字元: {content[:500]}")
            return None
            
    except Exception as e:
        print(f"抓取失敗: {e}")
        return None

def process_and_insert(df, conn):
    """處理並寫入資料庫"""
    if df is None or df.empty:
        print("無資料可處理")
        return 0
    
    # 確認必要欄位
    required_cols = ['證券代號', '持股分級', '人數', '股數']
    for col in required_cols:
        if col not in df.columns:
            print(f"缺少必要欄位: {col}")
            print(f"現有欄位: {list(df.columns)}")
            return 0
    
    # 找到比例欄位
    pct_col = None
    for col in ['占集保庫存數比例%', '占集保庫存數比例', '比例']:
        if col in df.columns:
            pct_col = col
            break
    
    if not pct_col:
        print("找不到比例欄位")
        pct_col = 'proportion'  # 設定預設值
        df[pct_col] = 0
    
    # 找到日期欄位
    date_col = None
    for col in ['資料日期', '日期', 'date']:
        if col in df.columns:
            date_col = col
            break
    
    if date_col:
        # 解析日期 (可能是民國年格式 114/12/19)
        sample_date = str(df[date_col].iloc[0])
        if '/' in sample_date:
            parts = sample_date.split('/')
            if len(parts) == 3:
                year = int(parts[0])
                if year < 2000:  # 民國年
                    year += 1911
                month = int(parts[1])
                day = int(parts[2])
                date_int = year * 10000 + month * 100 + day
            else:
                date_int = int(datetime.now().strftime('%Y%m%d'))
        else:
            date_int = int(sample_date.replace('-', ''))
    else:
        date_int = int(datetime.now().strftime('%Y%m%d'))
    
    print(f"資料日期: {date_int}")
    
    # 只處理 4 位數代碼的股票
    df['證券代號'] = df['證券代號'].astype(str).str.strip()
    df = df[df['證券代號'].str.match(r'^\d{4}$')]
    print(f"篩選後共 {len(df)} 筆 (只保留4位數代碼)")
    
    # 寫入資料庫
    count = 0
    for _, row in df.iterrows():
        try:
            code = str(row['證券代號'])
            level = int(row['持股分級'])
            holders = int(row['人數'])
            shares = int(row['股數'])
            proportion = float(row[pct_col]) if pct_col in row.index else 0
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_shareholding_all 
                (code, date_int, level, holders, shares, proportion)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (code, date_int, level, holders, shares, proportion))
            count += 1
        except Exception as e:
            continue
    
    conn.commit()
    return count

def check_result(conn):
    """檢查結果"""
    result = conn.execute("""
        SELECT COUNT(DISTINCT date_int) as dates, 
               COUNT(DISTINCT code) as stocks,
               COUNT(*) as total
        FROM stock_shareholding_all
    """).fetchone()
    
    print(f"\n=== 資料庫統計 ===")
    print(f"不同日期數: {result[0]}")
    print(f"不同股票數: {result[1]}")
    print(f"總筆數: {result[2]}")
    
    # 顯示最近的日期
    dates = conn.execute("""
        SELECT DISTINCT date_int FROM stock_shareholding_all 
        ORDER BY date_int DESC LIMIT 5
    """).fetchall()
    print(f"最近5個日期: {[d[0] for d in dates]}")
    
    # 顯示 2330 的資料
    sample = conn.execute("""
        SELECT date_int, level, holders, proportion 
        FROM stock_shareholding_all 
        WHERE code = '2330' 
        ORDER BY date_int DESC, level
        LIMIT 17
    """).fetchall()
    
    if sample:
        print(f"\n=== 2330 台積電 (最新) ===")
        total_holders = sum(s[2] for s in sample)
        large_pct = sum(s[3] for s in sample if s[1] >= 15)
        print(f"總人數: {total_holders:,}")
        print(f"千張以上持股比例: {large_pct:.2f}%")

if __name__ == "__main__":
    print("=== TDCC 集保資料補齊工具 ===\n")
    
    # 抓取資料
    df = fetch_tdcc_data()
    
    if df is not None:
        # 寫入資料庫
        conn = sqlite3.connect("taiwan_stock.db")
        count = process_and_insert(df, conn)
        print(f"\n成功寫入 {count} 筆資料")
        
        # 檢查結果
        check_result(conn)
        conn.close()
    else:
        print("\n無法取得資料，請檢查網路連線")
