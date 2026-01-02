# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 資料庫管理模組
"""
import sqlite3
import threading
import queue
from pathlib import Path
from contextlib import contextmanager

from .config import Config

# 全域變數
IS_ANDROID = Config.IS_ANDROID
WORK_DIR = Path(__file__).parent.parent  # d:\twse
DB_FILE = WORK_DIR / Config.DB_PATH


class DatabaseManager:
    """資料庫連線池管理器 (Singleton)"""
    _instance = None
    _pool_size = 5
    _connection_pool = None
    _pool_lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._init_pool()
            cls._instance._remove_stale_locks()
        return cls._instance
    
    def _init_pool(self):
        """初始化連線池 (Lazy Initialization)"""
        with self._pool_lock:
            if self._connection_pool is None:
                self._connection_pool = queue.Queue(maxsize=self._pool_size)
                for _ in range(self._pool_size):
                    conn = self._create_connection()
                    self._connection_pool.put(conn)
    
    def _create_connection(self):
        """建立單一連線 (DRY Principle)"""
        conn = sqlite3.connect(DB_FILE, timeout=60, check_same_thread=False)
        if not IS_ANDROID:
            conn.execute("PRAGMA journal_mode=WAL;")
        else:
            conn.execute("PRAGMA journal_mode=DELETE;")
            conn.execute("PRAGMA busy_timeout=60000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _remove_stale_locks(self):
        """移除 SQLite 殘留的鎖定檔案"""
        lock_files = [
            DB_FILE.with_suffix(DB_FILE.suffix + '-journal'),
            DB_FILE.with_suffix(DB_FILE.suffix + '-wal'),
            DB_FILE.with_suffix(DB_FILE.suffix + '-shm')
        ]
        
        for lock_file in lock_files:
            if lock_file.exists():
                try:
                    lock_file.unlink()
                except:
                    pass
    
    @contextmanager
    def get_connection(self, timeout=30):
        """從連線池取得連線 (Thread-Safe)"""
        conn = None
        try:
            conn = self._connection_pool.get(timeout=timeout)
            yield conn
        except queue.Empty:
            raise sqlite3.OperationalError("連線池已滿，請稍後重試")
        finally:
            if conn:
                try:
                    self._connection_pool.put_nowait(conn)
                except queue.Full:
                    conn.close()


# 全域資料庫管理器實例
db_manager = DatabaseManager()
