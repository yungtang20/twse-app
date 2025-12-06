"""
掃描策略實作
移植自原始程式碼的掃描邏輯
"""

def scan_smart_money(indicators_data, min_volume=500000, limit=20):
    """
    聰明錢掃描 (Smart Score >= 3)
    
    Args:
        indicators_data: {code: indicator_dict}
        min_volume: 最小成交量
        limit: 顯示筆數
    
    Returns:
        list: [(code, score, indicators)]
    """
    results = []
    
    for code, ind in indicators_data.items():
        try:
            vol = ind.get('volume', 0) or 0
            if vol < min_volume:
                continue
            
            score = ind.get('smart_score') or 0
            
            if score >= 3:
                results.append((code, score, ind))
        except:
            continue
    
    # 按 Score 降序排序
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results[:limit]


def scan_triple_filter(indicators_data, min_volume=500000, limit=20):
    """
    三重篩選 (WMA↑ + MFI↑ + VWAP↑)
    
    Args:
        indicators_data: {code: indicator_dict}
        min_volume: 最小成交量
        limit: 顯示筆數
    
    Returns:
        list: [(code, indicators)]
    """
    results = []
    
    for code, ind in indicators_data.items():
        try:
            vol = ind.get('volume', 0) or 0
            if vol < min_volume:
                continue
            
            # WMA20 上升
            wma20 = ind.get('wma20')
            wma20_prev = ind.get('wma20_prev')
            wma_rising = wma20 and wma20_prev and wma20 > wma20_prev
            
            # MFI 上升
            mfi = ind.get('mfi14')
            mfi_prev = ind.get('mfi14_prev')
            mfi_rising = mfi and mfi_prev and mfi > mfi_prev
            
            # VWAP 上升
            vwap = ind.get('vwap20')
            vwap_prev = ind.get('vwap20_prev')
            vwap_rising = vwap and vwap_prev and vwap > vwap_prev
            
            if wma_rising and mfi_rising and vwap_rising:
                results.append((code, ind))
        except:
            continue
    
    return results[:limit]


def scan_ma_alignment(indicators_data, min_volume=500000, limit=20):
    """
    均線多頭排列 (MA3 > MA20 > MA60 > MA120 > MA200)
    
    Args:
        indicators_data: {code: indicator_dict}
        min_volume: 最小成交量
        limit: 顯示筆數
    
    Returns:
        list: [(code, indicators)]
    """
    results = []
    
    for code, ind in indicators_data.items():
        try:
            vol = ind.get('volume', 0) or 0
            if vol < min_volume:
                continue
            
            ma3 = ind.get('ma3')
            ma20 = ind.get('ma20')
            ma60 = ind.get('ma60')
            ma120 = ind.get('ma120')
            ma200 = ind.get('ma200')
            close = ind.get('close')
            
            if not all([ma3, ma20, ma60, ma120, ma200, close]):
                continue
            
            # 均線多頭排列
            if close > ma3 > ma20 > ma60 > ma120 > ma200:
                results.append((code, ind))
        except:
            continue
    
    return results[:limit]


def scan_vp_breakout(indicators_data, mode='upper', min_volume=500000, limit=20):
    """
    VP 突破掃描
    
    Args:
        indicators_data: {code: indicator_dict}
        mode: 'upper' (上緣) 或 'lower' (下緣)
        min_volume: 最小成交量
        limit: 顯示筆數
    
    Returns:
        list: [(code, indicators)]
    """
    results = []
    
    for code, ind in indicators_data.items():
        try:
            vol = ind.get('volume', 0) or 0
            if vol < min_volume:
                continue
            
            close = ind.get('close')
            vp_upper = ind.get('vp_upper')
            vp_lower = ind.get('vp_lower')
            
            if not all([close, vp_upper, vp_lower]):
                continue
            
            if mode == 'upper' and close > vp_upper:
                results.append((code, ind))
            elif mode == 'lower' and close < vp_lower:
                results.append((code, ind))
        except:
            continue
    
    return results[:limit]


def scan_mfi_high(indicators_data, min_volume=500000, limit=20):
    """
    MFI 高值排序
    
    Args:
        indicators_data: {code: indicator_dict}
        min_volume: 最小成交量
        limit: 顯示筆數
    
    Returns:
        list: [(code, mfi, indicators)]
    """
    results = []
    
    for code, ind in indicators_data.items():
        try:
            vol = ind.get('volume', 0) or 0
            if vol < min_volume:
                continue
            
            mfi = ind.get('mfi14')
            if mfi:
                results.append((code, mfi, ind))
        except:
            continue
    
    # 按 MFI 降序排序
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results[:limit]


def scan_kd_golden_cross(indicators_data, min_volume=500000, limit=20):
    """
    月KD 黃金交叉 (K > D 且兩者同時上升)
    
    Args:
        indicators_data: {code: indicator_dict}
        min_volume: 最小成交量
        limit: 顯示筆數
    
    Returns:
        list: [(code, indicators)]
    """
    results = []
    
    for code, ind in indicators_data.items():
        try:
            vol = ind.get('volume', 0) or 0
            if vol < min_volume:
                continue
            
            k = ind.get('month_k')
            d = ind.get('month_d')
            k_prev = ind.get('month_k_prev')
            d_prev = ind.get('month_d_prev')
            
            if not all([k, d, k_prev, d_prev]):
                continue
            
            # K > D 且兩者同時上升
            if k > d and k > k_prev and d > d_prev:
                results.append((code, ind))
        except:
            continue
    
    return results[:limit]
