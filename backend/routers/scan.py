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
    amount: Optional[float] = None
    volume_ratio: Optional[float] = None
    score: Optional[float] = None
    signals: Optional[List[str]] = None
    
    # Technicals
    ma5: Optional[float] = None
    ma20: Optional[float] = None
    ma25: Optional[float] = None
    ma60: Optional[float] = None
    ma120: Optional[float] = None
    ma200: Optional[float] = None
    vol_ma5: Optional[float] = None
    vol_ma60: Optional[float] = None
    rsi: Optional[float] = None
    mfi: Optional[float] = None
    
    # VP & VWAP
    vp_poc: Optional[float] = None
    vp_high: Optional[float] = None
    vp_low: Optional[float] = None
    vwap: Optional[float] = None
    
    # Chips
    foreign: Optional[int] = None
    trust: Optional[int] = None
    dealer: Optional[int] = None
    big_trader: Optional[float] = None
    concentration: Optional[int] = None


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
    limit: int = 30,
    min_vol: int = 500,
    min_price: Optional[float] = None,
    scan_type: str = "default",
    **kwargs
) -> List[Dict]:
    """執行掃描查詢 (支援本地/雲端)"""
    
    # 檢查讀取來源設定
    import json
    from pathlib import Path
    read_source = "local"
    try:
        config_path = Path("config.json")
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                read_source = config.get("read_source", "local")
    except:
        pass
    
    # 雲端模式: 使用 Supabase (傳入 scan_type 以支援不同篩選)
    if read_source == "cloud" or db_manager.is_cloud_mode:
        return execute_scan_query_cloud(scan_type, limit, min_vol, min_price, **kwargs)
    
    extra_conditions = f"AND s.volume >= {min_vol}"
    if min_price:
        extra_conditions += f" AND s.close >= {min_price}"

    query = f"""
        SELECT 
            m.code, m.name, m.market_type as market,
            s.close, ROUND((s.close - s.close_prev) / s.close_prev * 100, 2) as change_pct, s.volume, s.amount,
            s.ma5, s.ma20, s.ma25, s.ma60, s.ma120, s.ma200,
            s.rsi, s.rsi as mfi, NULL as k, NULL as d,
            s.vp_poc, s.vp_high, s.vp_low,
            s.vol_ma5, s.vol_ma60,
            s.foreign_buy as "foreign", s.trust_buy as "trust", s.dealer_buy as "dealer",
            s.major_holders_pct as big_trader, s.total_shareholders as concentration,
            ROUND(s.amount / NULLIF(s.volume, 0), 2) as vwap
        FROM stock_meta m
        JOIN stock_snapshot s ON m.code = s.code
        WHERE m.code GLOB '[0-9][0-9][0-9][0-9]'
        {extra_conditions}
        {conditions}
        ORDER BY {order_by}
        LIMIT ?
    """
    return db_manager.execute_query(query, (limit,))


def execute_scan_query_cloud(
    scan_type: str = "default",
    limit: int = 30,
    min_vol: int = 500,
    min_price: Optional[float] = None,
    **kwargs
) -> List[Dict]:
    """雲端掃描查詢 (使用 Supabase) - 支援不同掃描類型"""
    if not db_manager.supabase:
        return []
    
    try:
        # 從 stock_snapshot 讀取資料 (包含六維共振所需的所有指標)
        query = db_manager.supabase.table('stock_snapshot').select(
            'code, name, close, close_prev, volume, amount, '
            'ma5, ma20, ma60, ma120, ma200, rsi, mfi14, '
            'vp_poc, vp_high, vp_low, foreign_buy, trust_buy, dealer_buy, '
            'macd, signal, daily_k, daily_d, lwr, bbi, mtm'
        )
        
        # 基本篩選條件
        if min_vol > 0:
            query = query.gte('volume', min_vol)
        if min_price:
            query = query.gte('close', min_price)
        
        # 根據 scan_type 套用特定篩選
        if scan_type == "vp_support":
            query = query.not_.is_('vp_low', 'null')
        elif scan_type == "vp_resistance":
            query = query.not_.is_('vp_high', 'null')
        elif scan_type == "mfi_oversold":
            query = query.not_.is_('mfi14', 'null').lt('mfi14', 30)
        elif scan_type == "mfi_overbought":
            query = query.not_.is_('mfi14', 'null').gt('mfi14', 70)
        elif scan_type.startswith("ma_"):
            query = query.not_.is_('ma20', 'null').not_.is_('ma60', 'null')
        elif scan_type == "institutional":
            query = query.not_.is_('ma20', 'null').not_.is_('ma60', 'null').not_.is_('rsi', 'null').lt('rsi', 60)
        elif scan_type == "six_dim":
            query = query.not_.is_('rsi', 'null')
        elif scan_type == "kd_month":
            # 月KD金叉需要 month_k, month_d
            query = query.not_.is_('rsi', 'null')  # 使用 RSI 作為基本篩選
        elif scan_type == "vsbc":
            # VSBC 需要 vp_poc, ma20, ma60
            query = query.not_.is_('vp_poc', 'null').not_.is_('ma20', 'null').not_.is_('ma60', 'null')
        elif scan_type == "smart_money":
            # 聰明錢：價格 > MA200, MFI < 80
            query = query.not_.is_('ma200', 'null').not_.is_('mfi14', 'null').lt('mfi14', 80)
        elif scan_type == "2560":
            # 2560 戰法：需要 ma20, ma60
            query = query.not_.is_('ma20', 'null').not_.is_('ma60', 'null')
        elif scan_type == "five_stage":
            # 五階篩選：RSI > 50
            query = query.not_.is_('rsi', 'null').gt('rsi', 50)
        elif scan_type == "patterns_morning_star":
            # K線型態 - 晨星: 收盤價 > 開盤價 (反轉向上)
            query = query.not_.is_('close', 'null').not_.is_('close_prev', 'null')
        elif scan_type == "patterns_evening_star":
            # K線型態 - 黃昏之星: 收盤價 < 開盤價 (反轉向下)
            query = query.not_.is_('close', 'null').not_.is_('close_prev', 'null')
        elif scan_type == "pv_div":
            # 量價背離: 找價格和成交量走勢不一致的股票
            query = query.not_.is_('rsi', 'null')
        
        # 排序與限制 (加大 limit 以便後續 Python 過濾)
        fetch_limit = limit * 5 if scan_type not in ["default"] else limit
        query = query.order('volume', desc=True).limit(fetch_limit)
        
        response = query.execute()
        
        if not response.data:
            return []
        
        # 轉換並進行細部篩選
        results = []
        for row in response.data:
            # 計算漲跌幅
            change_pct = 0.0
            if row.get('close') and row.get('close_prev'):
                try:
                    change_pct = round((row['close'] - row['close_prev']) / row['close_prev'] * 100, 2)
                except:
                    pass
            
            item = {
                'code': row.get('code'),
                'name': row.get('name'),
                'close': row.get('close'),
                'change_pct': change_pct,
                'volume': row.get('volume'),
                'amount': row.get('amount'),
                'ma5': row.get('ma5'),
                'ma20': row.get('ma20'),
                'ma60': row.get('ma60'),
                'ma120': row.get('ma120'),
                'ma200': row.get('ma200'),
                'rsi': row.get('rsi'),
                'mfi': row.get('mfi14'),
                'vp_poc': row.get('vp_poc'),
                'vp_high': row.get('vp_high'),
                'vp_low': row.get('vp_low'),
                'foreign': row.get('foreign_buy'),
                'trust': row.get('trust_buy'),
                'dealer': row.get('dealer_buy'),
                'vwap': round(row.get('amount', 0) / max(row.get('volume', 1), 1), 2) if row.get('volume') else None,
                # 六維共振指標
                'macd': row.get('macd'),
                'signal': row.get('signal'),
                'daily_k': row.get('daily_k'),
                'daily_d': row.get('daily_d'),
                'lwr': row.get('lwr'),
                'bbi': row.get('bbi'),
                'mtm': row.get('mtm')
            }
            
            # Python 層級細部過濾
            close = item.get('close') or 0
            vp_low = item.get('vp_low') or 0
            vp_high = item.get('vp_high') or 0
            ma20 = item.get('ma20') or 0
            ma60 = item.get('ma60') or 0
            rsi = item.get('rsi') or 0
            
            passed = True
            tolerance = kwargs.get('tolerance', 0.02)
            
            if scan_type == "vp_support":
                if vp_low and close:
                    dist = abs(close - vp_low) / close
                    item['_distance'] = dist  # 保存距離供排序
                    passed = dist < tolerance
                else:
                    passed = False
            elif scan_type == "vp_resistance":
                if vp_high and close:
                    dist = abs(close - vp_high) / close
                    item['_distance'] = dist  # 保存距離供排序
                    passed = dist < tolerance
                else:
                    passed = False
            elif scan_type == "ma_bull":
                # MA 多頭排列: close > ma5 > ma20 > ma60
                ma5 = item.get('ma5') or 0
                if not (close > ma5 > ma20 > ma60 and ma60 > 0):
                    passed = False
            elif scan_type == "institutional":
                # 機構價值: MA20 > MA60, Close > MA20, RSI < 60
                if not (ma20 > ma60 and close > ma20 and rsi < 60 and rsi > 0):
                    passed = False
            elif scan_type == "six_dim":
                # 六維共振: MACD/KDJ/RSI/LWR/BBI/MTM 至少5項符合
                macd_val = row.get('macd') or 0
                signal_val = row.get('signal') or 0
                daily_k = row.get('daily_k') or 0
                daily_d = row.get('daily_d') or 0
                lwr_val = row.get('lwr') or -100  # 預設為最低值
                bbi_val = row.get('bbi') or 0
                mtm_val = row.get('mtm') or 0
                
                # 計算六維分數
                score = 0
                if macd_val > signal_val: score += 1  # MACD 多頭
                if daily_k > daily_d: score += 1       # KDJ 多頭
                if rsi > 50: score += 1                # RSI 強勢
                if lwr_val > -50: score += 1           # LWR 非超賣
                if close > bbi_val and bbi_val > 0: score += 1  # 價格 > BBI
                if mtm_val > 0: score += 1             # MTM 動能向上
                
                # 保存分數供排序使用
                item['six_dim_score'] = score
                
                # 至少5項符合
                if score < 5:
                    passed = False
            elif scan_type == "kd_month":
                # 月KD金叉: K > D, K < 80, RSI > 50
                # 由於雲端沒有 month_k/month_d，使用 daily_k/daily_d 作為替代
                daily_k = row.get('daily_k') or 0
                daily_d = row.get('daily_d') or 0
                if not (daily_k > daily_d and daily_k < 80 and rsi > 50):
                    passed = False
                item['_kd_score'] = daily_k - daily_d  # 金叉強度供排序
            elif scan_type == "vsbc":
                # VSBC: 站上 POC, MA20 > MA60, Close > MA20
                vp_poc = item.get('vp_poc') or 0
                if not (close >= vp_poc and ma20 > ma60 and close > ma20 and vp_poc > 0):
                    passed = False
                item['_vsbc_score'] = (close - vp_poc) / vp_poc if vp_poc else 0  # 站上 POC 幅度
            elif scan_type == "smart_money":
                # 聰明錢: Close > MA200, MFI < 80, RSI > 50
                ma200 = item.get('ma200') or 0
                mfi = item.get('mfi') or 0
                if not (close > ma200 and mfi < 80 and rsi > 50 and ma200 > 0):
                    passed = False
                item['_smart_score'] = (close - ma200) / ma200 if ma200 else 0  # 距離 MA200 幅度
            elif scan_type == "2560":
                # 2560 戰法: Close > MA20 > MA60, 乖離 < 10%
                if ma20 <= 0:
                    passed = False
                else:
                    bias = (close - ma20) / ma20
                    if not (close > ma20 > ma60 and 0 < bias < 0.1):
                        passed = False
                    item['_bias'] = bias  # 乖離率供排序
            elif scan_type == "five_stage":
                # 五階篩選: RSI > 50, MA20 > MA60, Close > MA20
                ma5 = item.get('ma5') or 0
                if not (rsi > 50 and ma5 > ma20 > ma60 and close > ma5):
                    passed = False
                item['_five_score'] = rsi  # RSI 分數供排序
            elif scan_type == "patterns_morning_star":
                # 晨星: 收盤價 > 昨收 (反轉向上), RSI < 50 (從低位反彈更有意義)
                close_prev = item.get('close') and row.get('close_prev') or 0
                change_pct = item.get('change_pct') or 0
                if not (change_pct > 0 and rsi < 50):
                    passed = False
                item['_pattern_score'] = change_pct  # 漲幅供排序
            elif scan_type == "patterns_evening_star":
                # 黃昏之星: 收盤價 < 昨收 (反轉向下), RSI > 50 (從高位回落更有意義)
                change_pct = item.get('change_pct') or 0
                if not (change_pct < 0 and rsi > 50):
                    passed = False
                item['_pattern_score'] = abs(change_pct)  # 跌幅供排序
            elif scan_type == "pv_div":
                # 量價背離: 價格上漲但 RSI 下降，或價格下跌但 RSI 上升
                change_pct = item.get('change_pct') or 0
                # 簡化版: 價格創新高但 RSI 未過熱, 或價格下跌但 RSI 較強
                if change_pct > 0 and rsi < 70:
                    item['_div_type'] = 'bull_div'  # 正向背離
                    item['_div_score'] = (70 - rsi)  # RSI 越低越有背離意義
                elif change_pct < 0 and rsi > 30:
                    item['_div_type'] = 'bear_div'  # 負向背離
                    item['_div_score'] = (rsi - 30)  # RSI 越高越有背離意義
                else:
                    passed = False
            
            if passed:
                results.append(item)
        
        # 根據 scan_type 進行特定排序
        if scan_type == "six_dim":
            # 六維共振：按分數降序，分數相同則按 RSI 降序
            results.sort(key=lambda x: (x.get('six_dim_score', 0), x.get('rsi', 0)), reverse=True)
        elif scan_type == "institutional":
            # 機構價值：按 RSI 升序（未過熱優先）
            results.sort(key=lambda x: x.get('rsi', 100))
        elif scan_type.startswith("vp_"):
            # VP 掃描：按距離升序（越接近越優先）
            results.sort(key=lambda x: x.get('_distance', 999))
        elif scan_type.startswith("mfi_"):
            # MFI 掃描：oversold 按 MFI 升序，overbought 按 MFI 降序
            if "oversold" in scan_type:
                results.sort(key=lambda x: x.get('mfi', 100))
            else:
                results.sort(key=lambda x: x.get('mfi', 0), reverse=True)
        elif scan_type.startswith("ma_"):
            # 均線掃描：按乖離率升序（貼近均線優先）
            results.sort(key=lambda x: abs((x.get('close', 0) - x.get('ma20', 1)) / max(x.get('ma20', 1), 1)))
        elif scan_type == "kd_month":
            # 月KD：按金叉強度降序 (K-D 越大越強)
            results.sort(key=lambda x: x.get('_kd_score', 0), reverse=True)
        elif scan_type == "vsbc":
            # VSBC：按站上 POC 幅度降序
            results.sort(key=lambda x: x.get('_vsbc_score', 0), reverse=True)
        elif scan_type == "smart_money":
            # 聰明錢：按距離 MA200 幅度降序（越強勢越優先）
            results.sort(key=lambda x: x.get('_smart_score', 0), reverse=True)
        elif scan_type == "2560":
            # 2560：按乖離率升序（貼近 MA20 優先）
            results.sort(key=lambda x: x.get('_bias', 999))
        elif scan_type == "five_stage":
            # 五階：按 RSI 降序（越強勢越優先）
            results.sort(key=lambda x: x.get('_five_score', 0), reverse=True)
        elif scan_type.startswith("patterns_"):
            # K線型態：按反轉幅度排序
            results.sort(key=lambda x: x.get('_pattern_score', 0), reverse=True)
        elif scan_type == "pv_div":
            # 量價背離：按背離強度排序
            results.sort(key=lambda x: x.get('_div_score', 0), reverse=True)
        
        return results[:limit]
    except Exception as e:
        print(f"⚠️ 雲端掃描查詢錯誤: {e}")
        return []


# ========================================
# API 端點
# ========================================

@router.get("/scan/vp", response_model=ScanResponse)
async def scan_vp(
    direction: str = Query("support", description="support=支撐區, resistance=壓力區"),
    tolerance: float = Query(0.02, ge=0, le=0.1, description="容忍度 (%)"),
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    VP 掃描 (箱型壓力/支撐)
    - support: 接近下緣支撐 (VP Lower)
    - resistance: 接近上緣壓力 (VP Upper)
    """
    try:
        if direction == "support":
            conditions = f"""
                AND s.vp_low IS NOT NULL
                AND ABS(s.close - s.vp_low) / s.close < {tolerance}
            """
            order_by = "ABS(s.close - s.vp_low) / s.close ASC"
        else:
            conditions = f"""
                AND s.vp_high IS NOT NULL
                AND ABS(s.close - s.vp_high) / s.close < {tolerance}
            """
            order_by = "ABS(s.close - s.vp_high) / s.close ASC"
        
        results = execute_scan_query(conditions, order_by, limit, min_vol, min_price,
                                     scan_type=f"vp_{direction}", tolerance=tolerance)
        
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
    condition: str = Query("oversold", description="oversold=資金流入開始(由小→大), overbought=資金流出結束(由大→小)"),
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    MFI 掃描 (資金流向) - 對齊 Python 版本
    - oversold: MFI 由小→大 (mfi > mfi_prev AND mfi < 30)
    - overbought: MFI 由大→小 (mfi < mfi_prev AND mfi > 70)
    """
    try:
        if condition == "oversold":
            # Python: mfi > mfi_prev AND mfi < 30 (資金開始流入)
            conditions = """
                AND s.mfi14 IS NOT NULL AND s.mfi14_prev IS NOT NULL
                AND s.mfi14 > s.mfi14_prev
                AND s.mfi14 < 30
            """
            order_by = "s.mfi14 ASC"
        else:
            # Python: mfi < mfi_prev AND mfi > 70 (資金開始流出)
            conditions = """
                AND s.mfi14 IS NOT NULL AND s.mfi14_prev IS NOT NULL
                AND s.mfi14 < s.mfi14_prev
                AND s.mfi14 > 70
            """
            order_by = "s.mfi14 DESC"
        
        results = execute_scan_query(conditions, order_by, limit, min_vol, min_price,
                                     scan_type=f"mfi_{condition}")
        
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
    pattern: str = Query("bull", description="bull=多頭排列, below_ma20=低於MA20, below_ma200=低於MA200"),
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    均線掃描
    - bull: 多頭排列 (收盤價在 MA5 > MA20 > MA60 > MA120, 乖離 < 10%)
    - below_ma20: 低於 MA20 在 0-10% 之間
    - below_ma200: 低於 MA200 在 0-10% 之間
    """
    try:
        if pattern == "bull":
            conditions = """
                AND s.ma5 IS NOT NULL AND s.ma20 IS NOT NULL AND s.ma60 IS NOT NULL AND s.ma120 IS NOT NULL
                AND s.close > s.ma5
                AND s.ma5 > s.ma20
                AND s.ma20 > s.ma60
                AND s.ma60 > s.ma120
                AND (s.close - s.ma20) / s.ma20 * 100 > 0
                AND (s.close - s.ma20) / s.ma20 * 100 < 10
            """
            order_by = "(s.close - s.ma20) / s.ma20 ASC"
        elif pattern == "below_ma20":
            conditions = """
                AND s.ma20 IS NOT NULL
                AND s.close < s.ma20
                AND s.close >= s.ma20 * 0.9
            """
            order_by = "(s.close - s.ma20) / s.ma20 DESC"
        else:  # below_ma200
            conditions = """
                AND s.ma200 IS NOT NULL
                AND s.close < s.ma200
                AND s.close >= s.ma200 * 0.9
            """
            order_by = "(s.close - s.ma200) / s.ma200 DESC"
        
        results = execute_scan_query(conditions, order_by, limit, min_vol, min_price, 
                                       scan_type=f"ma_{pattern}")
        
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
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    KD 交叉訊號掃描 (月KD + NVI)
    - golden: 月K > 月D (金叉) 且 K < 80 且 NVI > PVI
    - death: 月K < 月D (死叉)
    """
    try:
        if signal == "golden":
            # Python Logic: K_prev <= D_prev AND K > D AND K < 80 AND NVI > PVI
            conditions = """
                AND s.month_k IS NOT NULL AND s.month_d IS NOT NULL 
                AND s.month_k_prev IS NOT NULL AND s.month_d_prev IS NOT NULL
                AND s.nvi IS NOT NULL AND s.pvi IS NOT NULL
                AND s.month_k_prev <= s.month_d_prev
                AND s.month_k > s.month_d
                AND s.month_k < 80
                AND s.nvi > s.pvi
            """
            order_by = "s.month_k ASC"
        else:
            # Death Cross (Reverse Logic)
            conditions = """
                AND s.month_k IS NOT NULL AND s.month_d IS NOT NULL
                AND s.month_k_prev IS NOT NULL AND s.month_d_prev IS NOT NULL
                AND s.month_k_prev >= s.month_d_prev
                AND s.month_k < s.month_d
            """
            order_by = "s.month_k DESC"
        
        results = execute_scan_query(conditions, order_by, limit, min_vol, min_price,
                                     scan_type="kd_month")
        
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
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    VSBC 籌碼策略掃描 (多方行為) - 對齊 Python 版本
    條件: VSBC PR>=99, VSBC上升, 站上POC, 均線多頭(MA20>MA60), 站上MA20
    """
    try:
        # Python Logic from scan_vsbc_strategy():
        # 1. vsbc_pct >= 99 (VSBC 百分位前1%)
        # 2. vsbc > vsbc_prev (VSBC 數值上升)
        # 3. close >= vp_poc (站上 POC)
        # 4. ma20 > ma60 (均線多頭)
        # 5. close > ma20 (站上月線)
        
        conditions = """
            AND s.vsbc_pct IS NOT NULL AND s.vsbc IS NOT NULL AND s.vsbc_prev IS NOT NULL
            AND s.vsbc_pct >= 99
            AND s.vsbc > s.vsbc_prev
            AND s.vp_poc IS NOT NULL AND s.close >= s.vp_poc
            AND s.ma20 IS NOT NULL AND s.ma60 IS NOT NULL AND s.ma20 > s.ma60
            AND s.close > s.ma20
        """
        
        # Style variations
        if style == "burst":
            conditions += " AND (s.close - s.close_prev) / s.close_prev > 0.03"
        
        results = execute_scan_query(conditions, "s.vsbc_pct DESC, s.volume DESC", limit, min_vol, min_price,
                                     scan_type="vsbc")
        
        return {
            "success": True,
            "data": {
                "scan_type": "vsbc",
                "style": style,
                "description": "VSBC 多方行為 (PR>=99 + 上升 + 站上POC + MA20>MA60 + 站上MA20)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/smart-money", response_model=ScanResponse)
async def scan_smart_money(
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    聰明錢掃描 (NVI主力籌碼) - 對齊 Python 版本
    條件: 成交量 > 昨日x1.1, 價格 > MA200, MFI < 80, smart_score >= 4
    """
    try:
        # Python Logic from scan_smart_money_strategy():
        # 1. volume > vol_prev * 1.1 (量增)
        # 2. close > MA200 (趨勢向上)
        # 3. mfi < 80 (未過熱)
        # 4. smart_score >= 4 (籌碼評分高)
        
        conditions = """
            AND s.vol_prev IS NOT NULL AND s.volume > s.vol_prev * 1.1
            AND s.ma200 IS NOT NULL AND s.close > s.ma200
            AND s.mfi14 IS NOT NULL AND s.mfi14 < 80
            AND s.smart_score IS NOT NULL AND s.smart_score >= 4
        """
        
        results = execute_scan_query(conditions, "s.smart_score DESC", limit, min_vol, min_price,
                                     scan_type="smart_money")
        
        return {
            "success": True,
            "data": {
                "scan_type": "smart-money",
                "description": "聰明錢指標 (量增>1.1x + 價>MA200 + MFI<80 + 籌碼評分>=4)",
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
        # Python Logic:
        # 1. Trend: close > ma25 AND ma25_slope > 0
        # 2. Vol Cross: vol_ma5 > vol_ma60 AND vol_ma5_prev <= vol_ma60_prev (Crossover)
        # 3. Validation: close > open AND close > close_prev
        # 4. Proximity: close < ma25 * 1.10
        
        conditions = """
            AND s.ma25 IS NOT NULL AND s.ma25_slope IS NOT NULL
            AND s.vol_ma5 IS NOT NULL AND s.vol_ma60 IS NOT NULL
            AND s.close > s.ma25
            AND s.ma25_slope > 0
            AND s.vol_ma5 > s.vol_ma60
            AND s.close > s.open
            AND s.close > s.close_prev
            AND s.close < s.ma25 * 1.10
        """
        # Note: Previous day vol cross check is hard in simple snapshot SQL without joining history or having 'vol_cross' flag.
        # Assuming 'vol_ma5 > vol_ma60' is the main state we want to catch (active golden cross state).
        # If strict crossover is needed, we need 'vol_ma5_prev' and 'vol_ma60_prev' which might not be in snapshot or need join.
        # Schema has vol_prev, but maybe not vol_ma5_prev.
        # Let's check schema for prev MAs. Schema has 'ma20_prev', 'ma60_prev'. Does it have 'vol_ma5_prev'?
        # Schema: 'vol_ma5' is there. 'vol_ma5_prev' is NOT in the list I saw (only price MAs).
        # So I'll stick to state check: vol_ma5 > vol_ma60.
        
        results = execute_scan_query(conditions, "s.volume DESC", limit, min_vol, min_price,
                                     scan_type="2560")
        
        return {
            "success": True,
            "data": {
                "scan_type": "2560",
                "description": "2560戰法 (股價>25MA向上, 均量線多頭, 陽線收漲, 乖離<10%)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/five-stage", response_model=ScanResponse)
async def scan_five_stage(
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    五階篩選 (均線+動能+籌碼+趨勢+型態)
    """
    try:
        # Python Logic approximation:
        # 1. RS (Mansfield RS > 0)
        # 2. Strength (MA5>20>60)
        # 3. Smart Money (NVI > PVI)
        # 4. Value (RSI > 50)
        # 5. Trigger (Close > Open)
        
        conditions = """
            AND s.mansfield_rs IS NOT NULL AND s.mansfield_rs > 0
            AND s.ma5 > s.ma20 AND s.ma20 > s.ma60
            AND s.nvi > s.pvi
            AND s.rsi > 50
            AND s.close > s.open
        """
        results = execute_scan_query(conditions, "s.mansfield_rs DESC", limit, min_vol, min_price,
                                     scan_type="five_stage")
        
        return {
            "success": True,
            "data": {
                "scan_type": "five-stage",
                "description": "五階篩選 (RS強勢 + 多頭排列 + NVI籌碼 + RSI強勢 + 收紅)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/institutional-value", response_model=ScanResponse)
async def scan_institutional_value(
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
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
        results = execute_scan_query(conditions, "s.vol_ma60 DESC", limit, min_vol, min_price, 
                                       scan_type="institutional")
        
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
async def scan_six_dim(
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    六維共振 (量/價/均/指/籌/型)
    """
    try:
        # Python Logic:
        # 1. MACD > Signal
        # 2. K > D
        # 3. RSI > 50
        # 4. LWR > -50
        # 5. Price > BBI
        # 6. MTM > 0
        # Score >= 5
        
        conditions = """
            AND (
                (CASE WHEN s.macd > s.signal THEN 1 ELSE 0 END) +
                (CASE WHEN s.daily_k > s.daily_d THEN 1 ELSE 0 END) +
                (CASE WHEN s.rsi > 50 THEN 1 ELSE 0 END) +
                (CASE WHEN s.lwr > -50 THEN 1 ELSE 0 END) +
                (CASE WHEN s.close > s.bbi THEN 1 ELSE 0 END) +
                (CASE WHEN s.mtm > 0 THEN 1 ELSE 0 END)
            ) >= 5
        """
        # Order by Score (approximated by sum) then Volume
        order_by = """
            (
                (CASE WHEN s.macd > s.signal THEN 1 ELSE 0 END) +
                (CASE WHEN s.daily_k > s.daily_d THEN 1 ELSE 0 END) +
                (CASE WHEN s.rsi > 50 THEN 1 ELSE 0 END) +
                (CASE WHEN s.lwr > -50 THEN 1 ELSE 0 END) +
                (CASE WHEN s.close > s.bbi THEN 1 ELSE 0 END) +
                (CASE WHEN s.mtm > 0 THEN 1 ELSE 0 END)
            ) DESC, s.volume DESC
        """
        
        results = execute_scan_query(conditions, order_by, limit, min_vol, min_price,
                                     scan_type="six_dim")
        
        return {
            "success": True,
            "data": {
                "scan_type": "six-dim",
                "description": "六維共振 (MACD/KDJ/RSI/LWR/BBI/MTM 至少5項符合)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/patterns", response_model=ScanResponse)
async def scan_patterns(
    type: str = Query("morning_star", description="morning_star=晨星, engulfing=吞噬(暫無)"),
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    K線型態 (晨星/夜星)
    """
    try:
        if type == "morning_star":
            conditions = "AND s.pattern_morning_star = 1"
            desc = "K線型態 (早晨之星)"
        else:
            # Assuming evening star for 'engulfing' or other type for now, or just evening star
            conditions = "AND s.pattern_evening_star = 1"
            desc = "K線型態 (黃昏之星)"

        results = execute_scan_query(conditions, "s.volume DESC", limit, min_vol, min_price,
                                     scan_type=f"patterns_{type}")
        
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
async def scan_pv_divergence(
    limit: int = Query(30, ge=1, le=100),
    min_vol: int = Query(500, ge=0, description="最小成交量"),
    min_price: float = Query(None, gt=0, description="最低股價")
):
    """
    量價背離 (3日背離)
    """
    try:
        # Use pre-calculated divergence columns
        conditions = "AND (s.div_3day_bull = 1 OR s.div_3day_bear = 1)"
        
        results = execute_scan_query(conditions, "s.rsi DESC", limit, min_vol, min_price,
                                     scan_type="pv_div")
        
        return {
            "success": True,
            "data": {
                "scan_type": "pv-divergence",
                "description": "量價背離 (3日價漲量縮/價跌量增)",
                "results": results,
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
