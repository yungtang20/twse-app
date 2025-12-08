"""
基本掃描模板

使用方式:
1. 複製此檔案到 plugins/ 目錄
2. 修改 scan() 函數邏輯
3. 在 user_plugins.json 中註冊
"""

def scan(data, params):
    """
    基本掃描模板
    
    Args:
        data: dict - {code: indicators_dict} 全市場指標數據
        params: dict - 使用者參數 (如 min_volume, threshold 等)
    
    Returns:
        list: [(code, sort_value, indicators), ...]
    """
    results = []
    min_vol = params.get('min_volume', 100000)
    
    for code, ind in data.items():
        # 1. 成交量過濾
        vol = ind.get('volume', 0) or 0
        if vol < min_vol:
            continue
        
        # 2. ★ 在這裡加入你的篩選條件
        # 範例: MFI > 50 且 收盤價上漲
        mfi = ind.get('mfi14') or ind.get('MFI') or 0
        close = ind.get('close') or 0
        close_prev = ind.get('close_prev') or 0
        
        if mfi > 50 and close > close_prev:
            # 3. 加入結果 (code, 排序值, 指標)
            sort_value = mfi  # 用 MFI 排序
            results.append((code, sort_value, ind))
    
    # 4. 排序 (高到低)
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results


# 插件 metadata (用於 JSON 註冊)
PLUGIN_INFO = {
    "id": "my_custom_scan",
    "name": "我的自訂掃描",
    "description": "MFI > 50 且收盤價上漲",
    "version": "1.0.0",
    "author": "user",
    "params": {
        "min_volume": {"type": "int", "default": 100000, "label": "最小成交量(股)"}
    }
}
