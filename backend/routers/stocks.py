"""
台灣股市分析系統 - 股票 API 路由
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from backend.services.db import (
    get_all_stocks,
    get_stock_by_code,
    get_stock_history,
    get_stock_shareholding_history,
    get_tdcc_total_holders,
    get_stock_indicators,
    get_institutional_data,
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
    tdcc_count: Optional[int] = None
    large_shareholder_pct: Optional[float] = None

class ShareholdingItem(BaseModel):
    """分級持股資料"""
    date_int: int
    holders: int
    proportion: float

class APIResponse(BaseModel):
    """標準 API 回應"""
    success: bool
    data: Optional[Any] = None
    message: Optional[str] = None


# ========================================
# API 端點
# ========================================

@router.get("/stocks", response_model=APIResponse)
async def list_stocks(
    market: Optional[str] = Query(None, description="市場 (twse/tpex)"),
    limit: int = Query(100, ge=1, le=5000, description="回傳筆數"),
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
    limit: int = Query(30, ge=1, le=2000, description="回傳筆數")
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

@router.get("/stocks/{code}/shareholding", response_model=APIResponse)
async def get_shareholding(
    code: str,
    threshold: int = Query(1000, description="持股門檻 (1000, 800, 600, 400, 200, 100, 50, 10)")
):
    """
    取得股票分級持股歷史
    - total_holders: 集保總人數 (所有分級的人數合計)
    - large_holders: 大戶持股資料 (根據門檻篩選)
    """
    try:
        # 映射 threshold 到 min_level
        mapping = {
            1000: 15,
            800: 14,
            600: 13,
            400: 12,
            200: 11,
            100: 10,
            50: 9,
            10: 4
        }
        min_level = mapping.get(threshold, 15)
        
        # 取得集保總人數 (不分級)
        total_holders_history = get_tdcc_total_holders(code)
        
        # 取得大戶持股 (依門檻篩選)
        large_holders_history = get_stock_shareholding_history(code, min_level)
        
        return {
            "success": True,
            "data": {
                "code": code,
                "threshold": threshold,
                "total_holders": total_holders_history,  # 集保總人數
                "large_holders": large_holders_history   # 大戶持股
            }
        }
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


@router.get("/stocks/{code}/institutional", response_model=APIResponse)
async def get_institutional(
    code: str,
    limit: int = Query(30, ge=1, le=100, description="回傳筆數")
):
    """
    取得法人買賣超資料 (從 Supabase)
    """
    try:
        data = get_institutional_data(code, limit)
        
        return {
            "success": True,
            "data": data
        }
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
