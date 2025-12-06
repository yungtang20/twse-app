"""
Plugin Helpers - 插件安全輔助函數庫
提供給插件使用的安全工具函數
"""
from typing import Dict, List, Tuple, Any, Optional


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全地將值轉換為浮點數
    
    Args:
        value: 要轉換的值
        default: 轉換失敗時的預設值
    
    Returns:
        浮點數結果
    """
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """
    安全地將值轉換為整數
    
    Args:
        value: 要轉換的值
        default: 轉換失敗時的預設值
    
    Returns:
        整數結果
    """
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def filter_by_volume(data: Dict, min_volume: int) -> Dict:
    """
    依成交量過濾股票
    
    Args:
        data: 股票指標數據 {code: indicators}
        min_volume: 最小成交量
    
    Returns:
        過濾後的數據
    """
    result = {}
    for code, ind in data.items():
        vol = safe_int(ind.get('volume', 0))
        if vol >= min_volume:
            result[code] = ind
    return result


def filter_by_indicator(data: Dict, key: str, 
                        min_val: float = None, 
                        max_val: float = None) -> Dict:
    """
    依指標值過濾股票
    
    Args:
        data: 股票指標數據
        key: 指標欄位名稱
        min_val: 最小值 (可選)
        max_val: 最大值 (可選)
    
    Returns:
        過濾後的數據
    """
    result = {}
    for code, ind in data.items():
        val = safe_float(ind.get(key))
        
        if min_val is not None and val < min_val:
            continue
        if max_val is not None and val > max_val:
            continue
        
        result[code] = ind
    return result


def sort_by_indicator(data: Dict, key: str, descending: bool = True) -> List[Tuple[str, Dict]]:
    """
    依指標值排序股票
    
    Args:
        data: 股票指標數據
        key: 排序用的指標欄位
        descending: 是否降序 (預設 True)
    
    Returns:
        排序後的 (code, indicators) 列表
    """
    items = list(data.items())
    items.sort(key=lambda x: safe_float(x[1].get(key, 0)), reverse=descending)
    return items


def compare_values(val1: float, val2: float, tolerance: float = 0.0001) -> int:
    """
    比較兩個數值
    
    Returns:
        1 if val1 > val2, -1 if val1 < val2, 0 if equal
    """
    if abs(val1 - val2) < tolerance:
        return 0
    return 1 if val1 > val2 else -1


def is_crossing_up(current: float, previous: float, 
                   current_ref: float, previous_ref: float) -> bool:
    """
    判斷是否向上穿越
    
    Args:
        current: 當前值
        previous: 前一期值
        current_ref: 當前參考值
        previous_ref: 前一期參考值
    
    Returns:
        是否發生向上穿越
    """
    return previous <= previous_ref and current > current_ref


def is_crossing_down(current: float, previous: float, 
                     current_ref: float, previous_ref: float) -> bool:
    """
    判斷是否向下穿越
    """
    return previous >= previous_ref and current < current_ref


def is_rising(current: float, previous: float) -> bool:
    """判斷是否上升"""
    return current > previous


def is_falling(current: float, previous: float) -> bool:
    """判斷是否下降"""
    return current < previous


def percent_change(current: float, previous: float) -> float:
    """
    計算百分比變化
    
    Returns:
        變化百分比
    """
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100


def in_range(value: float, lower: float, upper: float) -> bool:
    """
    判斷值是否在範圍內
    """
    return lower <= value <= upper


def get_indicator(indicators: Dict, key: str, default: Any = None) -> Any:
    """
    安全取得指標值
    """
    return indicators.get(key, default)


def get_prev_indicator(indicators: Dict, key: str, default: Any = None) -> Any:
    """
    取得前一期指標值 (自動加 _prev 後綴)
    """
    prev_key = f"{key}_prev"
    return indicators.get(prev_key, default)


# 建立輔助函數字典 (供 SafeExecutor 使用)
PLUGIN_HELPERS = {
    'safe_float': safe_float,
    'safe_int': safe_int,
    'filter_by_volume': filter_by_volume,
    'filter_by_indicator': filter_by_indicator,
    'sort_by_indicator': sort_by_indicator,
    'compare_values': compare_values,
    'is_crossing_up': is_crossing_up,
    'is_crossing_down': is_crossing_down,
    'is_rising': is_rising,
    'is_falling': is_falling,
    'percent_change': percent_change,
    'in_range': in_range,
    'get_indicator': get_indicator,
    'get_prev_indicator': get_prev_indicator,
}
