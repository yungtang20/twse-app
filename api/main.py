# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - FastAPI 後端
手機與網頁共用 API
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import sys
from pathlib import Path

# 確保可以 import 專案模組
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import Config, db_manager

app = FastAPI(
    title="台灣股市分析系統 API",
    description="提供股票資料與技術指標計算",
    version="1.0.0"
)

# CORS 設定 (允許手機 APP 存取)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """API 根路徑"""
    return {"status": "ok", "message": "台灣股市分析系統 API"}


@app.get("/api/stocks")
def get_stock_list(limit: int = Query(100, ge=1, le=2000)):
    """取得股票列表"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT code, name FROM stock_meta 
                ORDER BY code LIMIT ?
            """, (limit,))
            rows = cur.fetchall()
        return [{"code": r[0], "name": r[1]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{code}")
def get_stock_detail(code: str):
    """取得個股詳細資料"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT code, name, list_date FROM stock_meta WHERE code = ?
            """, (code,))
            meta = cur.fetchone()
            
            if not meta:
                raise HTTPException(status_code=404, detail=f"Stock {code} not found")
            
            # 取得最新指標
            cur.execute("""
                SELECT * FROM stock_snapshot WHERE code = ?
            """, (code,))
            snapshot = cur.fetchone()
            
        return {
            "code": meta[0],
            "name": meta[1],
            "list_date": meta[2],
            "snapshot": dict(zip([d[0] for d in cur.description], snapshot)) if snapshot else None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{code}/history")
def get_stock_history(
    code: str, 
    days: int = Query(30, ge=1, le=450)
):
    """取得個股歷史資料"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT date_int, open, high, low, close, volume 
                FROM stock_history 
                WHERE code = ? 
                ORDER BY date_int DESC 
                LIMIT ?
            """, (code, days))
            rows = cur.fetchall()
            
        return [{
            "date": str(r[0]),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5]
        } for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
