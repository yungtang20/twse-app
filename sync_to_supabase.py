"""
Supabase 滾動視窗同步腳本
從本地 SQLite 同步最近 450 天的資料到 Supabase
"""

import sqlite3
import requests
import json
from datetime import datetime, timedelta
from typing import Optional

# ============================================
# 配置
# ============================================
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_secret_XSeaHx_76CRxA6j8nZ3qDg_nzgFgTAN"  # 使用 Secret Key
LOCAL_DB = "d:/twse/taiwan_stock.db"
RETENTION_DAYS = 450  # 保留天數 (足夠計算 MA200)
BATCH_SIZE = 500  # 每批上傳筆數

# Supabase schema 中存在的欄位 (避免上傳不存在的欄位)
SUPABASE_COLUMNS = {
    'code', 'name', 'date', 'open', 'high', 'low', 'close', 'volume',
    'close_prev', 'vol_prev',
    'ma3', 'ma20', 'ma60', 'ma120', 'ma200',
    'wma3', 'wma20', 'wma60', 'wma120', 'wma200',
    'mfi14', 'vwap20', 'chg14_pct', 'rsi', 'macd', 'signal',
    'vp_poc', 'vp_upper', 'vp_lower',
    'month_k', 'month_d', 'daily_k', 'daily_d', 'week_k', 'week_d',
    'smi', 'smi_signal', 'svi', 'svi_signal', 'nvi', 'nvi_signal',
    'vsa_signal', 'smart_score', 'clv', 'pvi',
    'ma3_prev', 'ma20_prev', 'ma60_prev', 'ma120_prev', 'ma200_prev',
    'wma3_prev', 'wma20_prev', 'wma60_prev', 'wma120_prev', 'wma200_prev',
    'mfi14_prev', 'vwap20_prev', 'chg14_pct_prev',
    'month_k_prev', 'month_d_prev', 'daily_k_prev', 'daily_d_prev',
    'week_k_prev', 'week_d_prev',
    'smi_prev', 'svi_prev', 'nvi_prev', 'smart_score_prev'
}

# ============================================
# Supabase API 客戶端
# ============================================
class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.url = url
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"  # Upsert 模式
        }
    
    def upsert(self, table: str, data: list) -> dict:
        """Upsert 資料到 Supabase"""
        endpoint = f"{self.url}/rest/v1/{table}"
        response = requests.post(endpoint, headers=self.headers, json=data)
        return {"status": response.status_code, "data": response.text}
    
    def delete_old_data(self, table: str, days: int) -> dict:
        """刪除超過指定天數的舊資料"""
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        endpoint = f"{self.url}/rest/v1/{table}?date=lt.{cutoff_date}"
        response = requests.delete(endpoint, headers=self.headers)
        return {"status": response.status_code, "data": response.text}
    
    def get_row_count(self, table: str) -> int:
        """取得資料表行數"""
        endpoint = f"{self.url}/rest/v1/{table}?select=id"
        headers = {**self.headers, "Prefer": "count=exact"}
        response = requests.get(endpoint, headers=headers)
        count = response.headers.get("content-range", "0/0").split("/")[-1]
        return int(count) if count != "*" else 0

# ============================================
# 同步邏輯
# ============================================
def get_columns_from_db(conn) -> list:
    """從 SQLite 取得欄位名稱"""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(stock_data)")
    return [row[1] for row in cursor.fetchall()]

def sync_to_supabase(full_sync: bool = False):
    """
    同步本地資料到 Supabase
    
    Args:
        full_sync: True = 同步全部 450 天資料, False = 只同步最新一天
    """
    print("=" * 50)
    print("📤 Supabase 同步開始")
    print("=" * 50)
    
    # 初始化客戶端
    client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    
    # 計算日期範圍
    cutoff_date = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    
    # 連接本地資料庫
    conn = sqlite3.connect(LOCAL_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 取得欄位名稱
    columns = get_columns_from_db(conn)
    print(f"📊 欄位數量: {len(columns)}")
    
    # 查詢資料
    if full_sync:
        print(f"🔄 完整同步模式: 抓取 {cutoff_date} 之後的資料")
        query = f"SELECT * FROM stock_data WHERE date >= ? ORDER BY date, code"
        cursor.execute(query, (cutoff_date,))
    else:
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"⚡ 增量同步模式: 只抓取 {today} 的資料")
        query = f"SELECT * FROM stock_data WHERE date = ? ORDER BY code"
        cursor.execute(query, (today,))
    
    rows = cursor.fetchall()
    total_rows = len(rows)
    print(f"📝 待同步筆數: {total_rows:,}")
    
    if total_rows == 0:
        print("⚠️ 沒有資料需要同步")
        conn.close()
        return
    
    # 分批上傳
    success_count = 0
    error_count = 0
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        batch_data = []
        
        for row in batch:
            record = {}
            for col in columns:
                # 只同步 Supabase schema 中存在的欄位
                if col not in SUPABASE_COLUMNS:
                    continue
                value = row[col]
                # 處理 None 和特殊類型
                if value is None:
                    record[col] = None
                elif isinstance(value, bytes):
                    record[col] = int.from_bytes(value, 'little') if len(value) <= 8 else None
                elif isinstance(value, float):
                    # 處理 inf 和 nan (JSON 不支援)
                    import math
                    record[col] = value if math.isfinite(value) else None
                else:
                    record[col] = value
            batch_data.append(record)
        
        # 上傳批次
        result = client.upsert("stock_data", batch_data)
        
        if result["status"] in [200, 201]:
            success_count += len(batch)
        else:
            error_count += len(batch)
            print(f"❌ 批次 {i//BATCH_SIZE + 1} 失敗: {result['data'][:200]}")
        
        # 進度顯示
        progress = (i + len(batch)) / total_rows * 100
        print(f"📈 進度: {progress:.1f}% ({success_count:,}/{total_rows:,})", end="\r")
    
    print()  # 換行
    
    # 清理舊資料
    if full_sync:
        print(f"🧹 清理 {RETENTION_DAYS} 天前的舊資料...")
        result = client.delete_old_data("stock_data", RETENTION_DAYS)
        if result["status"] == 200:
            print("✅ 清理完成")
        else:
            print(f"⚠️ 清理失敗: {result['data'][:200]}")
    
    # 統計
    print("=" * 50)
    print(f"✅ 同步完成: {success_count:,} 筆成功")
    if error_count > 0:
        print(f"❌ 錯誤: {error_count:,} 筆失敗")
    
    # 查詢 Supabase 資料量
    total_in_supabase = client.get_row_count("stock_data")
    print(f"📊 Supabase 總資料量: {total_in_supabase:,} 筆")
    
    conn.close()
    print("=" * 50)

# ============================================
# CLI 入口
# ============================================
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        print("🚀 執行完整同步 (最近 450 天)")
        sync_to_supabase(full_sync=True)
    else:
        print("⚡ 執行增量同步 (僅今日)")
        sync_to_supabase(full_sync=False)
