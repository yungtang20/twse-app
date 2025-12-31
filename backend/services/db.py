"""
台灣股市分析系統 - 資料庫服務
整合 SQLite (本地資料) 與 Supabase (雲端資料)
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import os
from supabase import create_client

# 預設資料庫路徑
DEFAULT_DB_PATH = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) / "taiwan_stock.db"

def get_configured_db_path() -> Path:
    """從 config.json 讀取資料庫路徑，若無則使用預設值"""
    import json
    config_path = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) / "config.json"
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if config.get("db_path"):
                    return Path(config["db_path"])
        except Exception:
            pass
    return DEFAULT_DB_PATH

DB_PATH = get_configured_db_path()

# Supabase 設定 (優先使用環境變數)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://bshxromrtsetlfjdeggv.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Njk5NzI1NywiZXhwIjoyMDgyNTczMjU3fQ.8i4GD8rOQtpISgEd2ZX-wzR4xq2FCuKC99NyKqjmHi0")

# 自動偵測雲端模式: 如果 SQLite 檔案不存在，就是雲端模式
IS_CLOUD_MODE = not DB_PATH.exists()

class DBManager:
    """資料庫管理器"""
    
    def __init__(self, db_path: Path = None):
        self.db_path = db_path or get_configured_db_path()
        self._supabase = None
        self._supabase_initialized = False
        self.is_cloud_mode = IS_CLOUD_MODE
        
        # 雲端模式: 自動連線 Supabase
        if IS_CLOUD_MODE:
            print("☁️ 偵測到雲端模式 (SQLite 不存在)，自動連線 Supabase...")
            self.connect_supabase()
    
    def set_db_path(self, new_path: str) -> bool:
        """動態切換資料庫路徑"""
        new_path = Path(new_path)
        if not new_path.exists():
            return False
        self.db_path = new_path
        return True

    @property
    def supabase(self):
        """Get Supabase client (returns None if not connected)"""
        return self._supabase

    def connect_supabase(self):
        """Explicitly connect to Supabase"""
        try:
            print(f"🔄 初始化 Supabase 連線... URL: {SUPABASE_URL[:20]}...")
            self._supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            self._supabase_initialized = True
            print("✅ Supabase 連線初始化成功")
            return True
        except Exception as e:
            import traceback
            print(f"⚠️ Supabase 連線初始化失敗: {e}")
            traceback.print_exc()
            self._supabase = None
            self._supabase_initialized = False
            return False
    
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

    def execute_update(self, query: str, params: tuple = ()) -> None:
        """執行 SQLite 更新/刪除/DDL"""
        with self.get_connection() as conn:
            conn.execute(query, params)
            conn.commit()

# 全域資料庫管理器實例
db_manager = DBManager()

# ========================================
# 資料存取函數
# ========================================

def get_all_stocks() -> List[Dict]:
    """取得所有股票清單 (支援雲端模式)"""
    # 雲端模式: 從 Supabase 讀取
    if db_manager.is_cloud_mode and db_manager.supabase:
        try:
            response = db_manager.supabase.table('stock_meta').select('code, name, market_type').execute()
            if response.data:
                return [{'code': r['code'], 'name': r['name'], 'market': r.get('market_type', '')} for r in response.data]
        except Exception as e:
            print(f"⚠️ 雲端讀取股票清單失敗: {e}")
            return []
    
    # 本地模式: SQLite
    if db_manager.is_cloud_mode:
        return []  # 雲端模式下沒有 SQLite
        
    query = """
        SELECT code, name, market_type as market
        FROM stock_meta
        WHERE code GLOB '[0-9][0-9][0-9][0-9]'
        ORDER BY code
    """
    return db_manager.execute_query(query)

def get_stock_by_code(code: str) -> Optional[Dict]:
    """取得單一股票資料 (SQLite)"""
    query = """
        SELECT m.code, m.name, m.market_type as market,
               s.close, 
               CASE WHEN s.close_prev > 0 THEN (s.close - s.close_prev) / s.close_prev * 100 ELSE 0 END as change_pct,
               s.volume, s.amount,
               s.ma5, s.ma20, s.ma60, s.ma120, s.ma200,
               s.rsi, s.mfi14 as mfi, s.daily_k as k, s.daily_d as d,
               s.vp_poc, s.vp_high, s.vp_low,
               s.foreign_buy, s.trust_buy, s.dealer_buy
        FROM stock_meta m
        LEFT JOIN stock_snapshot s ON m.code = s.code
        WHERE m.code = ?
    """
    return db_manager.execute_single(query, (code,))

def get_stock_history(code: str, limit: int = 60) -> List[Dict]:
    """取得股票歷史 K 線 (支援本地/雲端切換)"""
    # 1. 雲端模式優先 (自動偵測或設定檔指定)
    read_source = "cloud" if db_manager.is_cloud_mode else "local"
    
    # 檢查 config.json 是否有覆蓋設定
    try:
        import json
        config_path = Path("config.json")
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                read_source = config.get("read_source", read_source)
    except:
        pass

    # 2. 如果是雲端模式，且 Supabase 已連線
    if read_source == "cloud" and db_manager.supabase:
        try:
            return get_stock_history_from_cloud(code, limit)
        except Exception as e:
            print(f"⚠️ 雲端讀取失敗: {e}")
            if db_manager.is_cloud_mode:
                # 雲端模式下無法降級，返回空資料
                return []
            # 否則降級回本地

    # 3. 本地讀取 (SQLite)
    if db_manager.is_cloud_mode:
        # 雲端模式下沒有 SQLite，返回空資料
        return []
    
    query = """
        SELECT date_int, open, high, low, close, volume, amount,
               foreign_buy, trust_buy, dealer_buy,
               tdcc_count, large_shareholder_pct
        FROM stock_history
        WHERE code = ?
        ORDER BY date_int DESC
        LIMIT ?
    """
    results = db_manager.execute_query(query, (code, limit))
    return list(reversed(results))

def get_stock_history_from_cloud(code: str, limit: int = 60) -> List[Dict]:
    """從 Supabase 取得股票歷史"""
    if not db_manager.supabase:
        return []
    
    # 查詢 stock_history
    res = db_manager.supabase.table("stock_history") \
        .select("*") \
        .eq("code", code) \
        .order("date_int", desc=True) \
        .limit(limit) \
        .execute()
    
    data = res.data
    if not data:
        return []
        
    # 轉換格式以符合前端需求
    formatted = []
    for row in data:
        # Supabase 欄位可能略有不同，確保對應
        item = {
            "date_int": row.get("date_int"),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "amount": row.get("amount", 0),
            "foreign_buy": row.get("foreign_buy", 0),
            "trust_buy": row.get("trust_buy", 0),
            "dealer_buy": row.get("dealer_buy", 0),
            "tdcc_count": row.get("tdcc_count", 0),
            "large_shareholder_pct": row.get("large_shareholder_pct", 0)
        }
        formatted.append(item)
        
    return list(reversed(formatted))

def get_stock_shareholding_history(code: str, min_level: int = 15) -> List[Dict]:
    """獲取股票分級持股歷史 (大戶持股)"""
    # 雲端模式: 返回空資料 (股東持股資料可能沒有同步到雲端)
    if db_manager.is_cloud_mode:
        return []
    
    query = """
        SELECT date_int, SUM(holders) as holders, SUM(proportion) as proportion
        FROM stock_shareholding_all
        WHERE code = ? AND level >= ? AND level <= 15
        GROUP BY date_int
        ORDER BY date_int ASC
    """
    return db_manager.execute_query(query, (code, min_level))

def get_tdcc_total_holders(code: str) -> List[Dict]:
    """獲取股票集保總人數 (所有分級的人數合計)"""
    # 雲端模式: 返回空資料
    if db_manager.is_cloud_mode:
        return []
    
    query = """
        SELECT date_int, SUM(holders) as total_holders
        FROM stock_shareholding_all
        WHERE code = ?
        GROUP BY date_int
        ORDER BY date_int ASC
    """
    return db_manager.execute_query(query, (code,))

def get_stock_indicators(code: str) -> Optional[Dict]:
    """取得股票技術指標"""
    # 雲端模式: 返回空資料
    if db_manager.is_cloud_mode:
        return None
    
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
    """取得系統狀態 (支援雲端模式)"""
    # 雲端模式: 從 Supabase 取得狀態
    if db_manager.is_cloud_mode:
        if db_manager.supabase:
            try:
                # 取得股票數量
                meta_res = db_manager.supabase.table('stock_meta').select('code', count='exact').limit(1).execute()
                stock_count = meta_res.count if meta_res else 0
                
                # 取得最新日期
                hist_res = db_manager.supabase.table('stock_history').select('date_int').order('date_int', desc=True).limit(1).execute()
                latest_date = hist_res.data[0]['date_int'] if hist_res.data else None
                
                return {
                    "db_path": "Supabase (Cloud)",
                    "stock_count": stock_count,
                    "latest_date": latest_date,
                    "institutional_date": None,
                    "db_exists": True,
                    "db_size_mb": 0,
                    "last_modified": None,
                    "supabase_connected": True,
                    "is_cloud_mode": True
                }
            except Exception as e:
                print(f"⚠️ 雲端狀態讀取錯誤: {e}")
                return {
                    "db_path": "Supabase (Cloud - Error)",
                    "stock_count": 0,
                    "latest_date": None,
                    "supabase_connected": db_manager.supabase is not None,
                    "is_cloud_mode": True,
                    "error": str(e)
                }
        else:
            return {
                "db_path": "Cloud mode but Supabase not connected",
                "stock_count": 0,
                "latest_date": None,
                "supabase_connected": False,
                "is_cloud_mode": True
            }
    
    # 本地模式: SQLite
    stock_count = db_manager.execute_single("SELECT COUNT(*) as cnt FROM stock_meta")
    latest_date = db_manager.execute_single("SELECT MAX(date_int) as dt FROM stock_history")
    inst_date = db_manager.execute_single("SELECT MAX(date_int) as dt FROM institutional_investors")
    
    return {
        "db_path": str(DB_PATH),
        "stock_count": stock_count["cnt"] if stock_count else 0,
        "latest_date": latest_date["dt"] if latest_date else None,
        "institutional_date": inst_date["dt"] if inst_date else None,
        "db_exists": DB_PATH.exists(),
        "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0,
        "last_modified": datetime.fromtimestamp(DB_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if DB_PATH.exists() else None,
        "supabase_connected": db_manager.supabase is not None,
        "is_cloud_mode": False
    }

def get_cloud_status() -> Dict:
    """取得雲端資料狀態"""
    if not db_manager.supabase:
        return {"connected": False, "error": "Supabase client not initialized"}
    
    try:
        # 查詢雲端最新日期
        res_date = db_manager.supabase.table("stock_history") \
            .select("date_int") \
            .order("date_int", desc=True) \
            .limit(1) \
            .execute()
            
        latest_date = res_date.data[0]["date_int"] if res_date.data else None
        
        # 查詢雲端股票數量 (概估，因為 count(*) 在 Supabase API 可能較慢，這裡改用 stock_data 查詢)
        res_count = db_manager.supabase.table("stock_data") \
            .select("code", count="exact") \
            .execute()
            
        stock_count = res_count.count
        
        return {
            "connected": True,
            "latest_date": latest_date,
            "stock_count": stock_count
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}
