"""
台灣股市分析系統 - FastAPI 後端主程式
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import sys
import os

# 將父目錄加入路徑，以便引用原始 Python 程式的模塊
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.routers import stocks, scan, ranking, admin, rankings
from backend.services.db import db_manager
from backend.scheduler import start_scheduler

@asynccontextmanager
async def lifespan(app: FastAPI):
    """應用程式生命週期管理"""
    # 啟動時
    print("📈 台灣股市分析系統 API 啟動中...")
    start_scheduler()
    yield
    # 關閉時
    print("👋 API 關閉中...")
    db_manager.shutdown()

app = FastAPI(
    title="台灣股市分析系統 API",
    description="提供台灣股市分析功能的 RESTful API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS 設定 (允許前端跨域請求)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 註冊路由
app.include_router(stocks.router, prefix="/api", tags=["股票"])
app.include_router(scan.router, prefix="/api", tags=["掃描"])
app.include_router(ranking.router, prefix="/api", tags=["排行"])
app.include_router(rankings.router) # No prefix needed as it's defined in the router
app.include_router(admin.router, prefix="/api", tags=["管理"])

@app.get("/")
async def root():
    """根路徑"""
    return {
        "message": "台灣股市分析系統 API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """健康檢查端點"""
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
