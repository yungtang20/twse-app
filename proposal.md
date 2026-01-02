# 專案執行計畫書 (Proposal)

## 1. 執行目標
- 修復 `最終修正.py` 中，查詢個股 (如 2330) 時顯示「無歷史數據」的問題。
- 確保歷史數據能正確從資料庫或 API 取得並顯示。

## 2. 修改內容

### 2.1 根本原因
- `IndicatorCalculator` 類別中的多個 `@staticmethod` 方法使用了 `pd.` 和 `np.` 但沒有在方法內部 import。
- 由於 `calculate_stock_history_indicators` 使用 try-except 並吞掉所有例外回傳 `None`，導致錯誤訊息被隱藏。

### 2.2 修復項目

| 行號 | 方法名稱 | 問題 | 狀態 |
|------|---------|------|------|
| 4765 | calculate_rsi_series | 缺少 pd/np import | ✓ 已修復 |
| 4819 | calculate_mfi | 缺少 pd/np import | ✓ 已修復 |
| 4852 | calculate_vwap_series | 缺少 pd/np import | ✓ 已修復 |
| 4868 | calculate_chg14_series | 缺少 pd/np import | ✓ 已修復 |
| 4882 | calculate_monthly_kd_series | 缺少 pd/np import | ✓ 已修復 |
| 4913 | calculate_smart_score_series | 缺少 pd import | ✓ 已修復 |
| 4935 | calculate_smi_series | 缺少 pd/np import | ✓ 已修復 |
| 4948 | calculate_nvi_series | 缺少 pd import | ✓ 已修復 |
| 4968 | calculate_adl_series | 缺少 pd import | ✓ 已修復 |
| 4983 | calculate_rs_series | 缺少 pd import | ✓ 已修復 |
| 5003 | calculate_pvi_series | 缺少 pd import | ✓ 已修復 |
| 5021 | calculate_clv_series | 缺少 pd import | ✓ 已修復 |
| 5033 | calculate_3day_divergence_series | 缺少 pd import | ✓ 已修復 |
| 5129 | calculate_vsbc_bands | 缺少 pd/np import | ✓ 已修復 |
| 5162 | calculate_pattern_morning_star | 缺少 pd import | ✓ 已修復 |
| 5202 | calculate_pattern_evening_star | 缺少 pd import | ✓ 已修復 |
| 5786 | get_indicator_color | 函數不存在 | ✓ 已新增 |

### 2.3 新增函數

```python
def get_indicator_color(val):
    """指標顏色：正值=紅色、負值=綠色（用於法人買賣超等）"""
    if val is None: return Colors.RESET
    if val > 0: return Colors.RED
    elif val < 0: return Colors.GREEN
    return Colors.RESET
```

## 3. 修改原因
- 使用者回報查詢 2330 時，雖然有即時股價，但歷史走勢顯示無數據。
- 資料庫有 735 筆 2330 歷史資料，但 `calculate_stock_history_indicators` 回傳 `None`。
- 根本原因是技術指標計算過程中的 NameError 被 catch 後隱藏。

## 4. 修改進度
- [2026-01-03 02:48] 開始分析問題，建立計畫書。
- [2026-01-03 02:50] 確認資料庫有資料 (735 筆)，問題在程式邏輯。
- [2026-01-03 02:52] 發現 `calculate_stock_history_indicators` 的 try-except 吞掉了 NameError。
- [2026-01-03 02:54] 逐步追蹤定位到 `calculate_mfi` 缺少 import。
- [2026-01-03 02:56] 批量修復 14 個缺少 import 的方法。
- [2026-01-03 02:58] 新增遺失的 `get_indicator_color` 函數。
- [2026-01-03 03:00] 驗證成功，歷史數據可正確顯示。
