"""
2560 戰法掃描邏輯 - 快速 SQL 版本 (使用 stock_snapshot 現有欄位)

注意: 此版本無法檢查「昨日均量交叉」條件 (需歷史資料)
使用簡化版條件: MA25趨勢 + 均量5>60 + 乖離<10%
"""
from typing import Dict, Any, Optional
from backend.services.db import db_manager


def execute_2560_scan(limit: int = 30, min_vol: int = 500, min_price: Optional[float] = None) -> Dict[str, Any]:
    """
    2560 戰法掃描 (快速 SQL 版本)
    
    使用 stock_snapshot 現有欄位進行快速篩選
    條件: MA25趨勢向上 + 均量5>60 + 乖離<10%
    """
    process_log = []
    
    # Base query parts
    base_where = "WHERE m.code GLOB '[0-9][0-9][0-9][0-9]'"
    
    # 1. Total count
    count_query = f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where}"
    res = db_manager.execute_query(count_query)
    total_count = res[0]['count'] if res else 0
    process_log.append({"step": "總股數", "count": total_count, "desc": "資料庫中所有股票"})
    
    # 2. Volume filter
    vol_cond = f"AND s.volume >= {min_vol * 1000}"
    count_query = f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond}"
    res = db_manager.execute_query(count_query)
    process_log.append({"step": f"成交量 >= {min_vol}張", "count": res[0]['count'] if res else 0, "desc": "過濾低流動性個股"})
    
    # 3. Price filter (if specified)
    price_cond = ""
    if min_price:
        price_cond = f"AND s.close >= {min_price}"
        count_query = f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {price_cond}"
        res = db_manager.execute_query(count_query)
        process_log.append({"step": f"股價 >= {min_price}元", "count": res[0]['count'] if res else 0, "desc": "過濾低價股"})
    
    # 4. Trend condition: close > ma25 AND ma25_slope > 0
    trend_cond = "AND s.ma25 IS NOT NULL AND s.ma25_slope IS NOT NULL AND s.close > s.ma25 AND s.ma25_slope > 0"
    count_query = f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {price_cond} {trend_cond}"
    res = db_manager.execute_query(count_query)
    process_log.append({"step": "趨勢條件 (股價>25MA向上)", "count": res[0]['count'] if res else 0, "desc": "收盤>25MA 且斜率>0"})
    
    # 5. Volume crossover (simplified): vol_ma5 > vol_ma60
    vol_cross_cond = "AND s.vol_ma5 IS NOT NULL AND s.vol_ma60 IS NOT NULL AND s.vol_ma5 > s.vol_ma60"
    count_query = f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {price_cond} {trend_cond} {vol_cross_cond}"
    res = db_manager.execute_query(count_query)
    process_log.append({"step": "均量條件 (5>60)", "count": res[0]['count'] if res else 0, "desc": "5日均量 > 60日均量"})
    
    # 6. Proximity: close < ma25 * 1.10
    prox_cond = "AND s.close < s.ma25 * 1.10"
    count_query = f"SELECT COUNT(*) as count FROM stock_meta m JOIN stock_snapshot s ON m.code = s.code {base_where} {vol_cond} {price_cond} {trend_cond} {vol_cross_cond} {prox_cond}"
    res = db_manager.execute_query(count_query)
    process_log.append({"step": "乖離過濾 (<10%)", "count": res[0]['count'] if res else 0, "desc": "距25MA不超過10%"})
    
    # Final query with all conditions
    all_conditions = f"{vol_cond} {price_cond} {trend_cond} {vol_cross_cond} {prox_cond}"
    
    final_query = f"""
        SELECT 
            m.code, m.name,
            s.close, 
            ROUND((s.close - s.close_prev) / s.close_prev * 100, 2) as change_pct,
            s.volume,
            s.ma25,
            s.vol_ma5 / 1000.0 as vol_ma5,
            s.vol_ma60 / 1000.0 as vol_ma60,
            CASE WHEN s.vol_ma60 > 0 THEN ROUND(s.volume * 1.0 / s.vol_ma60, 2) ELSE 0 END as vol_ratio
        FROM stock_meta m
        JOIN stock_snapshot s ON m.code = s.code
        {base_where}
        {all_conditions}
        ORDER BY vol_ratio DESC
        LIMIT ?
    """
    
    results = db_manager.execute_query(final_query, (limit,))
    
    # Format results
    formatted_results = []
    for r in results:
        formatted_results.append({
            "code": r['code'],
            "name": r['name'],
            "close": r['close'],
            "change_pct": r['change_pct'],
            "volume": r['volume'],
            "ma25": round(r['ma25'], 2) if r['ma25'] else None,
            "vol_ma5": round(r['vol_ma5'], 1) if r['vol_ma5'] else None,
            "vol_ma60": round(r['vol_ma60'], 1) if r['vol_ma60'] else None,
            "vol_ratio": r['vol_ratio']
        })
    
    return {
        "results": formatted_results,
        "count": len(formatted_results),
        "process_log": process_log
    }
