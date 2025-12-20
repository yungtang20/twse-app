import requests
import warnings
warnings.filterwarnings("ignore")

# TPEx 櫃買中心開放資料 API 列表
apis = [
    # 行情相關
    ("櫃買行情 (stk_quote)", "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk43_result.php?l=zh-tw&d=114/12/18&o=json"),
    ("櫃買指數報價", "https://www.tpex.org.tw/web/stock/aftertrading/otc_index_summary/OTC_index_summary_result.php?l=zh-tw&d=114/12/18&o=json"),
    
    # 法人相關
    ("法人買賣超", "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d=114/12/18&t=D&o=json"),
    
    # 融資融券
    ("融資融券 (margin)", "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&d=114/12/18&o=json"),
    
    # PE/PB (本益比)
    ("本益比/殖利率", "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&d=114/12/18&o=json"),
    
    # OpenAPI 對照
    ("OpenAPI 行情", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"),
    ("OpenAPI 法人", "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"),
    ("OpenAPI 融資", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"),
]

print("=" * 70)
print("櫃買中心 (TPEx) 開放資料 API 資料日期測試")
print("=" * 70)
print(f"{'API 名稱':<30} {'日期':<15} {'筆數':<8} {'狀態'}")
print("-" * 70)

for name, url in apis:
    try:
        r = requests.get(url, timeout=15, verify=False)
        data = r.json()
        
        # 網頁版 API
        if isinstance(data, dict):
            if 'aaData' in data:
                count = len(data['aaData'])
                date = data.get('reportTitle', '-')[:15] if data.get('reportTitle') else data.get('date', '-')
                print(f"{name:<30} {date:<15} {count:<8} ✓")
            elif 'iTotalRecords' in data:
                count = data.get('iTotalRecords', 0)
                date = '-'
                print(f"{name:<30} {date:<15} {count:<8} ✓")
            else:
                print(f"{name:<30} {'-':<15} {0:<8} ⚠")
        
        # OpenAPI (list 格式)
        elif isinstance(data, list) and data:
            # 嘗試找日期欄位
            date = data[0].get('Date', data[0].get('日期', 'N/A'))
            count = len(data)
            print(f"{name:<30} {date:<15} {count:<8} ✓")
        else:
            print(f"{name:<30} {'-':<15} {0:<8} ⚠ 無資料")
            
    except Exception as e:
        print(f"{name:<30} {'-':<15} {0:<8} ✗ {str(e)[:20]}")

print("=" * 70)
