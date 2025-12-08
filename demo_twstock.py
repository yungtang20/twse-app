import ssl
import requests
import urllib3
import twstock

# --- SSL Patch Start ---
# 由於您的環境有 SSL 憑證問題，使用 twstock 連網功能前需加入這段程式碼
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

old_request = requests.Session.request
def new_request(self, method, url, *args, **kwargs):
    kwargs['verify'] = False
    return old_request(self, method, url, *args, **kwargs)
requests.Session.request = new_request
# --- SSL Patch End ---

# --- TPEX Patch Start ---
from twstock.stock import TPEXFetcher
import datetime

def tpex_fetch(self, year: int, month: int, sid: str, retry: int = 5):
    # TPEX New API URL
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
    
    # Construct date param (use the first day of the month)
    # The API seems to return data for the whole month of the given date
    date_str = f"{year}/{month:02d}/01"
    
    params = {
        "date": date_str,
        "code": sid,
        "response": "json"
    }
    
    for retry_i in range(retry):
        try:
            r = requests.get(url, params=params, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
            data = r.json()
        except Exception:
            continue
        else:
            break
    else:
        # Fail in all retries
        return {"data": []}

    # Parse new response format to match old format expected by purify
    # Old format: {"aaData": [...], ...}
    # New format: {"tables": [{"data": [...], ...}], "stat": "ok"}
    
    result = {"data": []}
    if data.get("stat") == "ok" and data.get("tables"):
        # Extract data from the first table
        raw_data = data["tables"][0]["data"]
        # TPEXFetcher.purify expects a dict with "aaData" key, 
        # BUT here we can just manually call _make_datatuple for each row
        # or construct a dict that looks like old response.
        
        # Let's construct a dict that purify can handle if we were calling it.
        # But wait, fetch() calls purify().
        # self.purify(data) -> expects data["aaData"]
        
        # Let's just do the purification here and return the list of DATATUPLEs
        # because fetch() returns `data` which is a dict, and then `Stock.fetch` extracts `data["data"]`.
        
        # TPEXFetcher.fetch returns a dict.
        # Stock.fetch expects:
        # self.raw_data = [self.fetcher.fetch(year, month, self.sid)]
        # self.data = self.raw_data[0]["data"]
        
        # So fetch() must return a dict with "data" key containing the list of DATATUPLEs.
        
        result["data"] = [self._make_datatuple(row) for row in raw_data]
        
    return result

TPEXFetcher.fetch = tpex_fetch
# --- TPEX Patch End ---

def query_stock(stock_code):
    # 檢查代碼是否存在
    if stock_code not in twstock.codes:
        print(f"錯誤: 找不到代碼 {stock_code}，請確認輸入正確。")
        return

    stock_info = twstock.codes[stock_code]
    print(f"\n正在查詢 {stock_code} {stock_info.name} ...\n")

    # 1. 查詢即時股價 (Realtime)
    print(f"=== 即時股價 ({stock_code} {stock_info.name}) ===")
    stock_realtime = twstock.realtime.get(stock_code)
    if stock_realtime['success']:
        print(f"股票名稱: {stock_realtime['info']['name']}")
        print(f"目前股價: {stock_realtime['realtime']['latest_trade_price']}")
        print(f"開盤: {stock_realtime['realtime']['open']}")
        print(f"最高: {stock_realtime['realtime']['high']}")
        print(f"最低: {stock_realtime['realtime']['low']}")
        
        # 最佳5檔買賣資訊
        rt = stock_realtime['realtime']
        print(f"\n--- 最佳5檔 ---")
        print(f"{'買進價':<12} | {'買進量':<10} | {'賣出價':<12} | {'賣出量':<10}")
        print("-" * 50)
        for i in range(5):
            bid_price = rt['best_bid_price'][i] if i < len(rt.get('best_bid_price', [])) else '-'
            bid_vol = rt['best_bid_volume'][i] if i < len(rt.get('best_bid_volume', [])) else '-'
            ask_price = rt['best_ask_price'][i] if i < len(rt.get('best_ask_price', [])) else '-'
            ask_vol = rt['best_ask_volume'][i] if i < len(rt.get('best_ask_volume', [])) else '-'
            print(f"{bid_price:<12} | {bid_vol:<10} | {ask_price:<12} | {ask_vol:<10}")
    else:
        print(f"查詢失敗: {stock_realtime['rtmessage']}")

    print("\n" + "="*30 + "\n")

    # 2. 查詢歷史資料 (Historical Data)
    print(f"=== 歷史資料 ({stock_code} {stock_info.name}) - 近 5 日 ===")
    stock = twstock.Stock(stock_code)
    
    # 如果沒有資料，嘗試主動抓取近 31 天資料
    if not stock.price:
        print("初始資料為空，嘗試主動抓取近 31 天資料...")
        stock.fetch_31()
    
    # 取得最近 5 筆資料
    recent_data = zip(
        stock.date[-5:],
        stock.open[-5:],
        stock.high[-5:],
        stock.low[-5:],
        stock.price[-5:],
        stock.capacity[-5:]
    )

    if not stock.price[-5:]:
        print("警告: 無法取得歷史資料。")
    else:
        print(f"{'日期':<12} | {'開盤':<8} | {'最高':<8} | {'最低':<8} | {'收盤':<8} | {'成交股數':<10}")
        print("-" * 80)
        for date, open_, high, low, close, capacity in recent_data:
            print(f"{date.strftime('%Y-%m-%d'):<12} | {str(open_):<8} | {str(high):<8} | {str(low):<8} | {str(close):<8} | {str(capacity):<10}")

    print("\n" + "="*30 + "\n")

    # 3. 股票代碼資訊 (Codes)
    print("=== 股票代碼資訊 ===")
    print(f"代碼 {stock_code} 資訊: {stock_info.name} ({stock_info.type})")
    print(f"上市/上櫃: {stock_info.market}")
    print(f"產業別: {stock_info.group}")
    print("\n" + "-"*30 + "\n")

if __name__ == "__main__":
    print("歡迎使用 twstock 查詢工具")
    while True:
        user_input = input("請輸入股票代碼 (輸入 q 離開): ").strip()
        if user_input.lower() == 'q':
            print("程式結束")
            break
        
        if not user_input:
            continue
            
        try:
            query_stock(user_input)
        except Exception as e:
            print(f"發生錯誤: {e}")
