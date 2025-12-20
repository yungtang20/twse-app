"""
台灣股市分析系統 - 資料庫服務
沿用原 SQLite 資料庫，提供連線管理
"""
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import os

# 資料庫路徑
DB_PATH = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) / "taiwan_stock.db"

class DBManager:
    """資料庫管理器 (簡化版)"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
    
    @contextmanager
    def get_connection(self, timeout: int = 30):
        """取得資料庫連線 (with context manager)"""
        conn = sqlite3.connect(str(self.db_path), timeout=timeout)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """執行查詢並返回結果"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def execute_single(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """執行查詢並返回單一結果"""
        results = self.execute_query(query, params)
        return results[0] if results else None
    
    def shutdown(self):
        """關閉資料庫管理器"""
        pass  # 每次查詢都會自動關閉連線，無需額外處理


# 全域資料庫管理器實例
db_manager = DBManager()


# ========================================
# 資料存取函數
# ========================================

def get_all_stocks() -> List[Dict]:
    """取得所有股票清單"""
    query = """
        SELECT code, name, market, industry
        FROM stock_meta
        WHERE code GLOB '[0-9][0-9][0-9][0-9]'
        ORDER BY code
    """
    return db_manager.execute_query(query)


def get_stock_by_code(code: str) -> Optional[Dict]:
    """取得單一股票資料"""
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
    """取得股票歷史 K 線"""
    query = """
        SELECT date_int, open, high, low, close, volume, amount
        FROM stock_history
        WHERE code = ?
        ORDER BY date_int DESC
        LIMIT ?
    """
    results = db_manager.execute_query(query, (code, limit))
    return list(reversed(results))  # 按日期從舊到新排序


def get_stock_indicators(code: str) -> Optional[Dict]:
    """取得股票技術指標"""
    query = """
        SELECT *
        FROM stock_snapshot
        WHERE code = ?
    """
    return db_manager.execute_single(query, (code,))


def get_system_status() -> Dict:
    """取得系統狀態"""
    stock_count = db_manager.execute_single("SELECT COUNT(*) as cnt FROM stock_meta")
    latest_date = db_manager.execute_single("SELECT MAX(date_int) as dt FROM stock_history")
    
    return {
        "db_path": str(DB_PATH),
        "stock_count": stock_count["cnt"] if stock_count else 0,
        "latest_date": latest_date["dt"] if latest_date else None,
        "db_exists": DB_PATH.exists(),
        "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0
    }
