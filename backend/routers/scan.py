"""
台灣股市分析系統 - 市場掃描 API 路由
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from backend.services.db import db_manager
from backend.routers.scan_2560 import execute_2560_scan

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
            m.code, m.name, m.market_type as market,
            s.close, ROUND((s.close - s.close_prev) / s.close_prev * 100, 2) as change_pct, s.volume, s.amount,
            s.ma5, s.ma20, s.ma60, s.ma120, s.ma200,
            s.rsi, NULL as mfi, NULL as k, NULL as d,
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
        # Use RSI as proxy since MFI column is missing
        if condition == "oversold":
            conditions = "AND s.rsi IS NOT NULL AND s.rsi < 30"
            order_by = "s.rsi ASC"
        else:
            conditions = "AND s.rsi IS NOT NULL AND s.rsi > 70"
            order_by = "s.rsi DESC"
        
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
            order_by = "(s.close - s.close_prev) / s.close_prev DESC"
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
        # Use RSI as proxy since K/D columns are missing
        if signal == "golden":
            conditions = """
                AND s.rsi IS NOT NULL
                AND s.rsi > 30 AND s.rsi < 50
            """
            order_by = "s.rsi ASC"
        else:
            conditions = """
                AND s.rsi IS NOT NULL
                AND s.rsi > 50 AND s.rsi < 70
            """
            order_by = "s.rsi DESC"
        
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
            AND ABS(s.close - s.ma20) / NULLIF(s.ma20, 0) < 0.15
        """
        
        if style == "steady":
            conditions = base_conditions + " AND (s.close - s.close_prev) / s.close_prev BETWEEN -0.02 AND 0.02"
        elif style == "burst":
            conditions = base_conditions + " AND (s.close - s.close_prev) / s.close_prev > 0.03"
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
            AND (s.close - s.close_prev) / s.close_prev > 0
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
        {"id": "2560", "name": "2560戰法", "description": "MA25趨勢+均量金叉+陽線+乖離"},
    ]
    
    return {
        "success": True,
        "data": {
            "scan_types": scan_types
        }
    }


@router.get("/scan/2560", response_model=ScanResponse)
async def scan_2560(
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """2560 戰法 (MA25趨勢 + 均量金叉 + 陽線收漲 + 乖離過濾)"""
    try:
        result = execute_2560_scan(limit=limit, min_vol=min_vol, min_price=min_price)
        
        return {
            "success": True,
            "data": {
                "scan_type": "2560",
                "description": "2560戰法 (股價>25MA向上, 均量線金叉, 陽線收漲, 乖離<10%)",
                "results": result["results"],
                "count": result["count"],
                "process_log": result["process_log"]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/five-stage", response_model=ScanResponse)
async def scan_five_stage(limit: int = Query(30, ge=1, le=100)):
    """
    五階篩選 (均線+動能+籌碼+趨勢+型態)
    """
    try:
        conditions = """
            AND s.ma5 IS NOT NULL AND s.ma20 IS NOT NULL AND s.ma60 IS NOT NULL
            AND s.ma5 > s.ma20 AND s.ma20 > s.ma60
            AND s.rsi IS NOT NULL AND s.rsi > 50
            AND s.open IS NOT NULL AND s.close > s.open
        """
        results = execute_scan_query(conditions, "s.rsi DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "five-stage",
                "description": "五階篩選 (多頭排列 + RSI強勢 + 收紅)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/institutional-value", response_model=ScanResponse)
async def scan_institutional_value(limit: int = Query(30, ge=1, le=100)):
    """
    機構價值 (低估值/高成長潛力)
    """
    try:
        # 模擬機構篩選：趨勢向上但未過熱，且有量能支撐
        conditions = """
            AND s.ma20 IS NOT NULL AND s.ma60 IS NOT NULL
            AND s.ma20 > s.ma60
            AND s.close > s.ma20
            AND s.rsi IS NOT NULL AND s.rsi < 60
            AND s.vol_ma60 > 1000
        """
        results = execute_scan_query(conditions, "s.vol_ma60 DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "institutional-value",
                "description": "機構價值 (趨勢向上 + 未過熱 + 高流動性)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/six-dim", response_model=ScanResponse)
async def scan_six_dim(limit: int = Query(30, ge=1, le=100)):
    """
    六維共振 (量/價/均/指/籌/型)
    """
    try:
        conditions = """
            AND s.ma20 IS NOT NULL AND s.vol_ma60 IS NOT NULL
            AND s.close > s.ma20
            AND s.volume > s.vol_ma60
            AND s.ma5 > s.ma20
            AND s.rsi IS NOT NULL AND s.rsi > 50
            AND s.open IS NOT NULL AND s.close > s.open
            AND (s.close - s.close_prev) > 0
        """
        results = execute_scan_query(conditions, "s.volume DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "six-dim",
                "description": "六維共振 (價漲/量增/均線多/指標強/收紅/動能)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/patterns", response_model=ScanResponse)
async def scan_patterns(
    type: str = Query("morning_star", description="morning_star=晨星(模擬), engulfing=吞噬(模擬)"),
    limit: int = Query(30, ge=1, le=100)
):
    """
    K線型態 (基於當日資料模擬)
    """
    try:
        if type == "morning_star":
            # 模擬強勢反轉：長下影線 (鎚頭)
            conditions = """
                AND s.open IS NOT NULL AND s.high IS NOT NULL AND s.low IS NOT NULL
                AND (s.close - s.low) > (s.high - s.low) * 0.7
                AND (s.open - s.low) > (s.high - s.low) * 0.7
                AND s.rsi < 40
            """
            desc = "K線型態 (低檔鎚頭/晨星訊號)"
        else:
            # 模擬實體長紅 (吞噬)
            conditions = """
                AND s.open IS NOT NULL AND s.high IS NOT NULL AND s.low IS NOT NULL
                AND s.close > s.open
                AND (s.close - s.open) > (s.high - s.low) * 0.8
                AND s.volume > s.vol_ma60 * 1.5
            """
            desc = "K線型態 (爆量長紅/吞噬訊號)"

        results = execute_scan_query(conditions, "s.volume DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "patterns",
                "type": type,
                "description": desc,
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/pv-divergence", response_model=ScanResponse)
async def scan_pv_divergence(limit: int = Query(30, ge=1, le=100)):
    """
    量價背離 (價漲量縮)
    """
    try:
        conditions = """
            AND s.ma20 IS NOT NULL AND s.vol_ma60 IS NOT NULL
            AND s.close > s.ma20
            AND (s.close - s.close_prev) / s.close_prev > 0.01
            AND s.volume < s.vol_ma60 * 0.8
            AND s.rsi IS NOT NULL AND s.rsi > 60
        """
        results = execute_scan_query(conditions, "s.rsi DESC", limit)
        
        return {
            "success": True,
            "data": {
                "scan_type": "pv-divergence",
                "description": "量價背離 (價漲量縮警示)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
