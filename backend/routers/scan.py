"""
台灣股市分析系統 - 市場掃描 API 路由
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from services.db import db_manager

router = APIRouter()


# ========================================
# 掃描結果模型
# ========================================

class ScanResult(BaseModel):
    """掃描結果項目"""
    code: str
    name: str
    close: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    volume_ratio: Optional[float] = None
    score: Optional[float] = None
    signals: Optional[List[str]] = None


class ScanResponse(BaseModel):
    """掃描回應"""
    success: bool
    data: Optional[dict] = None
    message: Optional[str] = None


# ========================================
# 掃描查詢函數
# ========================================

def execute_scan_query(
    conditions: str,
    order_by: str = "s.close DESC",
    limit: int = 30
) -> List[Dict]:
    """執行掃描查詢"""
    query = f"""
        SELECT 
            m.code, m.name, m.market,
            s.close, s.change_pct, s.volume, s.amount,
            s.ma5, s.ma20, s.ma60, s.ma120, s.ma200,
            s.rsi, s.mfi, s.k, s.d,
            s.vp_poc, s.vp_high, s.vp_low,
            s.vol_ma60
        FROM stock_meta m
        JOIN stock_snapshot s ON m.code = s.code
        WHERE m.code GLOB '[0-9][0-9][0-9][0-9]'
        AND s.volume >= 500
        {conditions}
        ORDER BY {order_by}
        LIMIT ?
    """
    return db_manager.execute_query(query, (limit,))


# ========================================
# API 端點
# ========================================

@router.get("/scan/vp", response_model=ScanResponse)
async def scan_vp(
    direction: str = Query("support", description="support=支撐區, resistance=壓力區"),
    tolerance: float = Query(0.02, ge=0, le=0.1, description="容忍度 (%)"),
    limit: int = Query(30, ge=1, le=100)
):
    """
    VP 掃描 (箱型壓力/支撐)
    - support: 接近下緣支撐
    - resistance: 接近上緣壓力
    """
    try:
        if direction == "support":
            conditions = """
                AND s.vp_low IS NOT NULL
                AND s.close BETWEEN s.vp_low * 0.98 AND s.vp_low * 1.02
            """
            order_by = "ABS(s.close - s.vp_low) / s.vp_low ASC"
        else:
            conditions = """
                AND s.vp_high IS NOT NULL
                AND s.close BETWEEN s.vp_high * 0.98 AND s.vp_high * 1.02
            """
            order_by = "ABS(s.close - s.vp_high) / s.vp_high ASC"
        
        results = execute_scan_query(conditions, order_by, limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "vp",
                "direction": direction,
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/mfi", response_model=ScanResponse)
async def scan_mfi(
    condition: str = Query("oversold", description="oversold=超賣, overbought=超買"),
    limit: int = Query(30, ge=1, le=100)
):
    """
    MFI 掃描 (資金流向)
    - oversold: MFI < 20 (超賣)
    - overbought: MFI > 80 (超買)
    """
    try:
        if condition == "oversold":
            conditions = "AND s.mfi IS NOT NULL AND s.mfi < 20"
            order_by = "s.mfi ASC"
        else:
            conditions = "AND s.mfi IS NOT NULL AND s.mfi > 80"
            order_by = "s.mfi DESC"
        
        results = execute_scan_query(conditions, order_by, limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "mfi",
                "condition": condition,
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/ma", response_model=ScanResponse)
async def scan_ma(
    pattern: str = Query("bull", description="bull=多頭排列, below_ma20=低於MA20"),
    limit: int = Query(30, ge=1, le=100)
):
    """
    均線掃描
    - bull: 多頭排列 (收盤價在 MA5 > MA20 > MA60)
    - below_ma20: 低於 MA20 在 0-10% 之間
    """
    try:
        if pattern == "bull":
            conditions = """
                AND s.ma5 IS NOT NULL AND s.ma20 IS NOT NULL AND s.ma60 IS NOT NULL
                AND s.close > s.ma5
                AND s.ma5 > s.ma20
                AND s.ma20 > s.ma60
            """
            order_by = "s.change_pct DESC"
        else:  # below_ma20
            conditions = """
                AND s.ma20 IS NOT NULL
                AND s.close < s.ma20
                AND s.close >= s.ma20 * 0.9
            """
            order_by = "(s.close - s.ma20) / s.ma20 DESC"
        
        results = execute_scan_query(conditions, order_by, limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "ma",
                "pattern": pattern,
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/kd-cross", response_model=ScanResponse)
async def scan_kd_cross(
    signal: str = Query("golden", description="golden=金叉, death=死叉"),
    limit: int = Query(30, ge=1, le=100)
):
    """
    KD 交叉訊號掃描
    - golden: K > D (金叉)
    - death: K < D (死叉)
    """
    try:
        if signal == "golden":
            conditions = """
                AND s.k IS NOT NULL AND s.d IS NOT NULL
                AND s.k > s.d
                AND s.k < 50
            """
            order_by = "(s.k - s.d) DESC"
        else:
            conditions = """
                AND s.k IS NOT NULL AND s.d IS NOT NULL
                AND s.k < s.d
                AND s.k > 50
            """
            order_by = "(s.d - s.k) DESC"
        
        results = execute_scan_query(conditions, order_by, limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "kd-cross",
                "signal": signal,
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/vsbc", response_model=ScanResponse)
async def scan_vsbc(
    style: str = Query("steady", description="steady=穩健型, burst=爆發型, trend=趨勢型"),
    limit: int = Query(30, ge=1, le=100)
):
    """
    VSBC 籌碼策略掃描
    - steady: 穩健型
    - burst: 爆發型
    - trend: 趨勢型
    """
    try:
        # 基礎條件：60日均量 > 1000，量能比 > 1.3
        base_conditions = """
            AND s.vol_ma60 IS NOT NULL AND s.vol_ma60 > 1000
            AND s.volume / NULLIF(s.vol_ma60, 0) > 1.3
            AND s.ma20 IS NOT NULL
            AND ABS(s.close - s.ma20) / NULLIF(s.ma20, 0) < 0.12
        """
        
        if style == "steady":
            conditions = base_conditions + " AND s.change_pct BETWEEN -2 AND 2"
        elif style == "burst":
            conditions = base_conditions + " AND s.change_pct > 3"
        else:  # trend
            conditions = base_conditions + " AND s.close > s.ma20 AND s.ma20 > s.ma60"
        
        results = execute_scan_query(conditions, "s.volume DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "vsbc",
                "style": style,
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/smart-money", response_model=ScanResponse)
async def scan_smart_money(limit: int = Query(30, ge=1, le=100)):
    """
    聰明錢掃描 (NVI 主力籌碼)
    """
    try:
        # NVI 相關條件 (如果有 NVI 欄位)
        conditions = """
            AND s.volume IS NOT NULL
            AND s.change_pct > 0
            AND s.volume < s.vol_ma60
        """
        
        results = execute_scan_query(conditions, "s.change_pct DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "smart-money",
                "description": "縮量上漲 (主力控盤訊號)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/list", response_model=ScanResponse)
async def list_scan_types():
    """
    取得所有可用的掃描類型
    """
    scan_types = [
        {"id": "vp", "name": "VP掃描", "description": "箱型壓力/支撐"},
        {"id": "mfi", "name": "MFI掃描", "description": "資金流向指標"},
        {"id": "ma", "name": "均線掃描", "description": "多頭/空頭排列"},
        {"id": "kd-cross", "name": "KD交叉", "description": "金叉/死叉訊號"},
        {"id": "vsbc", "name": "VSBC策略", "description": "量價/箱型/籌碼"},
        {"id": "smart-money", "name": "聰明錢", "description": "NVI主力籌碼"},
    ]
    
    return {
        "success": True,
        "data": {
            "scan_types": scan_types
        }
    }
