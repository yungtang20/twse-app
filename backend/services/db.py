"""
台灣股市分析系統 - 資料庫服務
整合 SQLite (本地資料) 與 Supabase (雲端資料)
"""
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import os
from supabase import create_client

# 資料庫路徑
DB_PATH = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) / "taiwan_stock.db"

# Supabase 設定
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_secret_XSeaHx_76CRxA6j8nZ3qDg_nzgFgTAN"

class DBManager:
    """資料庫管理器"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.supabase = None
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("✅ Supabase 連線初始化成功")
        except Exception as e:
            print(f"⚠️ Supabase 連線初始化失敗: {e}")
    
    @contextmanager
    def get_connection(self, timeout: int = 30):
        """取得 SQLite 連線"""
        conn = sqlite3.connect(str(self.db_path), timeout=timeout)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """執行 SQLite 查詢"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def execute_single(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """執行 SQLite 單一查詢"""
        results = self.execute_query(query, params)
        return results[0] if results else None

# 全域資料庫管理器實例
db_manager = DBManager()

# ========================================
# 資料存取函數
# ========================================

def get_all_stocks() -> List[Dict]:
    """取得所有股票清單 (SQLite)"""
    query = """
        SELECT code, name, market, industry
        FROM stock_meta
        WHERE code GLOB '[0-9][0-9][0-9][0-9]'
        ORDER BY code
    """
    return db_manager.execute_query(query)

def get_stock_by_code(code: str) -> Optional[Dict]:
    """取得單一股票資料 (SQLite)"""
    query = """
        SELECT m.code, m.name, m.market, m.industry,
               s.close, s.change_pct, s.volume, s.amount,
               s.ma5, s.ma20, s.ma60, s.ma120, s.ma200,
               s.rsi, s.mfi, s.k, s.d,
               s.vp_poc, s.vp_high, s.vp_low,
               s.foreign_buy, s.trust_buy, s.dealer_buy
        FROM stock_meta m
        LEFT JOIN stock_snapshot s ON m.code = s.code
        WHERE m.code = ?
    """
    return db_manager.execute_single(query, (code,))

def get_stock_history(code: str, limit: int = 60) -> List[Dict]:
    """取得股票歷史 K 線 (SQLite)"""
    query = """
        SELECT date_int, open, high, low, close, volume, amount
        FROM stock_history
        WHERE code = ?
        ORDER BY date_int DESC
        LIMIT ?
    """
    results = db_manager.execute_query(query, (code, limit))
    return list(reversed(results))

def get_stock_indicators(code: str) -> Optional[Dict]:
    """取得股票技術指標 (SQLite)"""
    query = """
        SELECT *
        FROM stock_snapshot
        WHERE code = ?
    """
    return db_manager.execute_single(query, (code,))

def get_institutional_data(code: str, limit: int = 30) -> List[Dict]:
    """取得法人買賣超資料 (Supabase)"""
    if not db_manager.supabase:
        return []
    try:
        res = db_manager.supabase.table("institutional_investors") \
            .select("*") \
            .eq("code", code) \
            .order("date_int", desc=True) \
            .limit(limit) \
            .execute()
        return list(reversed(res.data))
    except Exception as e:
        print(f"Error fetching institutional data: {e}")
        return []

def get_system_status() -> Dict:
    """取得系統狀態"""
    stock_count = db_manager.execute_single("SELECT COUNT(*) as cnt FROM stock_meta")
    latest_date = db_manager.execute_single("SELECT MAX(date_int) as dt FROM stock_history")
    
    return {
        "db_path": str(DB_PATH),
        "stock_count": stock_count["cnt"] if stock_count else 0,
        "latest_date": latest_date["dt"] if latest_date else None,
        "db_exists": DB_PATH.exists(),
        "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0,
        "supabase_connected": db_manager.supabase is not None
    }
