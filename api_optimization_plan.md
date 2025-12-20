# API 優化方案

## 測試結果總結

### TWSE (證交所)
| 資料類型 | 建議來源 | 日期 | 原因 |
|---------|---------|------|-----|
| 每日行情 | **MI_INDEX 網頁版** | 20251218 | OpenAPI 延遲1天 |
| PE/PB殖利率 | **BWIBBU_ALL 網頁版** | 20251219 | 最新 |
| 法人買賣超 | **T86 網頁版** | 20251218 | OpenAPI 凌晨失敗 |
| 融資融券 | **MI_MARGN 網頁版** | 20251218 | 較即時 |

### TPEx (櫃買中心)
| 資料類型 | 建議來源 | 日期 | 原因 |
|---------|---------|------|-----|
| 每日行情 | **OpenAPI** | 1141218 | 網頁版需cookie |
| 法人買賣超 | **OpenAPI** | 1141218 | 穩定 |
| 融資融券 | **OpenAPI** | 1141218 | 穩定 |

## 需修改的函數

1. `_fetch_twse_data()` - ✅ 已改用 MI_INDEX
2. `step3_download_twse_daily()` - 需確認使用新 API
3. `PePbDataAPI.fetch_twse_pepb()` - 改用 BWIBBU_ALL 網頁版
4. `InstitutionalInvestorAPI.fetch_twse_openapi()` - ✅ 已有 T86 網頁版備援
5. `MarginDataAPI.fetch_twse_margin()` - 改用 MI_MARGN 網頁版
6. TPEx 相關 - 維持 OpenAPI (已是最佳選擇)
