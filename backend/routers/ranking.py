"""
台灣股市分析系統 - 法人排行 API 路由
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict
from pydantic import BaseModel

from services.db import db_manager

router = APIRouter()


# ========================================
# 排行結果模型
# ========================================

class RankingItem(BaseModel):
    """排行項目"""
    rank: int
    code: str
    name: str
    close: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    buy_sell: Optional[float] = None
    amount: Optional[float] = None
    consecutive_days: Optional[int] = None


class RankingResponse(BaseModel):
    """排行回應"""
    success: bool
    data: Optional[dict] = None
    message: Optional[str] = None


# ========================================
# 查詢函數
# ========================================

def get_institutional_ranking(
    entity: str,  # foreign, trust, dealer
    direction: str,  # buy, sell
    limit: int = 30,
    min_days: int = 1
) -> List[Dict]:
    """取得法人排行"""
    
    # 欄位映射
    column_map = {
        "foreign": "foreign_buy",
        "trust": "trust_buy",
        "dealer": "dealer_buy"
    }
    
    column = column_map.get(entity, "foreign_buy")
    
    if direction == "buy":
        condition = f"s.{column} > 0"
        order = f"s.{column} DESC"
    else:
        condition = f"s.{column} < 0"
        order = f"s.{column} ASC"
    
    query = f"""
        SELECT 
            m.code, m.name, m.market,
            s.close, s.change_pct, s.volume, s.amount,
            s.{column} as buy_sell
        FROM stock_meta m
        JOIN stock_snapshot s ON m.code = s.code
        WHERE m.code GLOB '[0-9][0-9][0-9][0-9]'
        AND s.volume >= 500
        AND {condition}
        ORDER BY {order}
        LIMIT ?
    """
    
    results = db_manager.execute_query(query, (limit,))
    
    # 加入排名
    for i, item in enumerate(results):
        item["rank"] = i + 1
    
    return results


# ========================================
# API 端點
# ========================================

@router.get("/ranking/foreign-buy", response_model=RankingResponse)
async def foreign_buy_ranking(
    limit: int = Query(30, ge=1, le=100),
    min_days: int = Query(1, ge=1, le=30, description="連續買入天數")
):
    """外資買超排行"""
    try:
        results = get_institutional_ranking("foreign", "buy", limit, min_days)
        
        return {
            "success": True,
            "data": {
                "entity": "foreign",
                "direction": "buy",
                "title": "外資買超排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/foreign-sell", response_model=RankingResponse)
async def foreign_sell_ranking(
    limit: int = Query(30, ge=1, le=100)
):
    """外資賣超排行"""
    try:
        results = get_institutional_ranking("foreign", "sell", limit)
        
        return {
            "success": True,
            "data": {
                "entity": "foreign",
                "direction": "sell",
                "title": "外資賣超排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/trust-buy", response_model=RankingResponse)
async def trust_buy_ranking(
    limit: int = Query(30, ge=1, le=100)
):
    """投信買超排行"""
    try:
        results = get_institutional_ranking("trust", "buy", limit)
        
        return {
            "success": True,
            "data": {
                "entity": "trust",
                "direction": "buy",
                "title": "投信買超排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/trust-sell", response_model=RankingResponse)
async def trust_sell_ranking(
    limit: int = Query(30, ge=1, le=100)
):
    """投信賣超排行"""
    try:
        results = get_institutional_ranking("trust", "sell", limit)
        
        return {
            "success": True,
            "data": {
                "entity": "trust",
                "direction": "sell",
                "title": "投信賣超排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/dealer-buy", response_model=RankingResponse)
async def dealer_buy_ranking(
    limit: int = Query(30, ge=1, le=100)
):
    """自營商買超排行"""
    try:
        results = get_institutional_ranking("dealer", "buy", limit)
        
        return {
            "success": True,
            "data": {
                "entity": "dealer",
                "direction": "buy",
                "title": "自營商買超排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/dealer-sell", response_model=RankingResponse)
async def dealer_sell_ranking(
    limit: int = Query(30, ge=1, le=100)
):
    """自營商賣超排行"""
    try:
        results = get_institutional_ranking("dealer", "sell", limit)
        
        return {
            "success": True,
            "data": {
                "entity": "dealer",
                "direction": "sell",
                "title": "自營商賣超排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/{entity}-{direction}", response_model=RankingResponse)
async def generic_ranking(
    entity: str,
    direction: str,
    limit: int = Query(30, ge=1, le=100)
):
    """
    通用排行端點
    - entity: foreign, trust, dealer
    - direction: buy, sell
    """
    if entity not in ["foreign", "trust", "dealer"]:
        raise HTTPException(status_code=400, detail="無效的法人類型")
    
    if direction not in ["buy", "sell"]:
        raise HTTPException(status_code=400, detail="無效的買賣方向")
    
    try:
        results = get_institutional_ranking(entity, direction, limit)
        
        entity_names = {
            "foreign": "外資",
            "trust": "投信",
            "dealer": "自營商"
        }
        
        direction_names = {
            "buy": "買超",
            "sell": "賣超"
        }
        
        return {
            "success": True,
            "data": {
                "entity": entity,
                "direction": direction,
                "title": f"{entity_names[entity]}{direction_names[direction]}排行",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
