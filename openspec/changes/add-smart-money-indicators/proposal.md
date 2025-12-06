# Change: Add Smart Money Indicators

## Why
使用者希望能透過「聰明錢」指標 (Smart Money Indicators) 來分析主力動向，包含 SMI, SVI, NVI 與 VSA 訊號，以提升選股準確度。

## What Changes
- **Database**: 新增 `smi`, `svi`, `nvi`, `vsa_signal`, `smart_score` 等 17 個欄位到 `stock_data` 表。
- **Logic**: 在 `IndicatorCalculator` 中新增 `calculate_smart_money_indicators` 方法，實作 CLV, SVI, NVI, VSA 算法。
- **Process**: 修改 `calculate_stock_history_indicators` 與 `step7_calc_indicators` 以支援新指標的計算與儲存。
- **UI/Feature**: 新增 `scan_smart_money_strategy` 掃描功能與 `format_smart_money_result` 顯示格式，並更新主選單。

## Impact
- Affected specs: `smart-money-analysis` (New)
- Affected code: `最終完全版.py`
