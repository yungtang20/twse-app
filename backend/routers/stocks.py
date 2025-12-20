"""
台灣股市分析系統 - 股票 API 路由
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel

from services.db import (
    get_all_stocks,
    get_stock_by_code,
    get_stock_history,
    get_stock_indicators,
    get_system_status
)

router = APIRouter()


# ========================================
# 資料模型
# ========================================

class StockBase(BaseModel):
    """股票基本資料"""
    code: str
    name: str
    market: Optional[str] = None
    industry: Optional[str] = None


class StockDetail(StockBase):
    """股票詳細資料"""
    close: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    amount: Optional[float] = None
    ma5: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    ma120: Optional[float] = None
    ma200: Optional[float] = None
    rsi: Optional[float] = None
    mfi: Optional[float] = None
    k: Optional[float] = None
    d: Optional[float] = None
    vp_poc: Optional[float] = None
    vp_high: Optional[float] = None
    vp_low: Optional[float] = None
    foreign_buy: Optional[float] = None
    trust_buy: Optional[float] = None
    dealer_buy: Optional[float] = None


class HistoryItem(BaseModel):
    """歷史 K 線資料"""
    date_int: int
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    amount: Optional[float] = None


class APIResponse(BaseModel):
    """標準 API 回應"""
    success: bool
    data: Optional[dict | list] = None
    message: Optional[str] = None


# ========================================
# API 端點
# ========================================

@router.get("/stocks", response_model=APIResponse)
async def list_stocks(
    market: Optional[str] = Query(None, description="市場 (twse/tpex)"),
    limit: int = Query(100, ge=1, le=2000, description="回傳筆數"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    """
    取得股票清單
    - 支援分頁與市場篩選
    """
    try:
        stocks = get_all_stocks()
        
        # 市場篩選
        if market:
            stocks = [s for s in stocks if s.get("market", "").lower() == market.lower()]
        
        # 分頁
        total = len(stocks)
        stocks = stocks[offset:offset + limit]
        
        return {
            "success": True,
            "data": {
                "stocks": stocks,
                "total": total,
                "limit": limit,
                "offset": offset
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stocks/{code}", response_model=APIResponse)
async def get_stock(code: str):
    """
    取得單一股票詳細資料
    """
    try:
        stock = get_stock_by_code(code)
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"股票 {code} 不存在")
        
        return {
            "success": True,
            "data": stock
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stocks/{code}/history", response_model=APIResponse)
async def get_history(
    code: str,
    limit: int = Query(60, ge=1, le=500, description="回傳筆數")
):
    """
    取得股票歷史 K 線資料
    """
    try:
        history = get_stock_history(code, limit)
        
        if not history:
            raise HTTPException(status_code=404, detail=f"股票 {code} 無歷史資料")
        
        return {
            "success": True,
            "data": {
                "code": code,
                "history": history,
                "count": len(history)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stocks/{code}/indicators", response_model=APIResponse)
async def get_indicators(code: str):
    """
    取得股票技術指標
    """
    try:
        indicators = get_stock_indicators(code)
        
        if not indicators:
            raise HTTPException(status_code=404, detail=f"股票 {code} 無指標資料")
        
        return {
            "success": True,
            "data": indicators
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=APIResponse)
async def system_status():
    """
    取得系統狀態
    """
    try:
        status = get_system_status()
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
