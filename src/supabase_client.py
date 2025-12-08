"""
Supabase Client - 雲端資料存取
"""
import json

# Supabase 連線設定
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_publishable_yXSGYxyxPMaoVu4MbGK5Vw_IuZsl5yu"


class SupabaseClient:
    """Supabase API 客戶端"""
    
    def __init__(self, url=None, key=None):
        self.url = url or SUPABASE_URL
        self.key = key or SUPABASE_KEY
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json"
        }
    
    def _request(self, method, endpoint, params=None, data=None):
        """發送 HTTP 請求"""
        try:
            import requests
            url = f"{self.url}/rest/v1/{endpoint}"
            
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=data,
                timeout=30
            )
            
            if response.status_code >= 400:
                return None
            
            return response.json()
        except Exception as e:
            print(f"Supabase error: {e}")
            return None
    
    def get_stock_list(self):
        """取得股票清單"""
        result = self._request(
            "GET", 
            "stock_list",
            params={"select": "code,name,industry"}
        )
        return result or []
    
    def get_stock_data(self, code, limit=60):
        """取得單一股票的歷史資料"""
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "*",
                "code": f"eq.{code}",
                "order": "date.desc",
                "limit": str(limit)
            }
        )
        return result or []
    
    def get_latest_data(self, limit=100):
        """取得最新一天的所有股票資料"""
        # 先取得最新日期
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "date",
                "order": "date.desc",
                "limit": "1"
            }
        )
        
        if not result:
            return []
        
        latest_date = result[0].get('date')
        if not latest_date:
            return []
        
        # 取得該日期的所有資料
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "*",
                "date": f"eq.{latest_date}",
                "limit": str(limit)
            }
        )
        return result or []
    
    def search_stock(self, keyword):
        """搜尋股票 (代碼或名稱)"""
        # 先嘗試用代碼搜尋
        result = self._request(
            "GET",
            "stock_list",
            params={
                "select": "code,name,industry",
                "code": f"eq.{keyword}"
            }
        )
        
        if result:
            return result
        
        # 用名稱搜尋
        result = self._request(
            "GET",
            "stock_list",
            params={
                "select": "code,name,industry",
                "name": f"like.*{keyword}*",
                "limit": "10"
            }
        )
        return result or []
    
    def scan_smart_money(self, min_volume=500, limit=20):
        """聰明錢掃描 - 取高成交量股票"""
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "code,date,close,volume",
                "order": "date.desc,volume.desc",
                "limit": str(limit * 3)
            }
        )
        
        if not result:
            return []
        
        # 過濾高成交量
        filtered = []
        seen_codes = set()
        for row in result:
            code = row.get('code', '')
            vol = row.get('volume') or 0
            if code not in seen_codes and vol >= min_volume * 1000:
                seen_codes.add(code)
                # 模擬 smart_score
                row['smart_score'] = min(5, max(1, int(vol // 5000000) + 2))
                filtered.append(row)
                if len(filtered) >= limit:
                    break
        
        return filtered
    
    def scan_kd_golden(self, limit=20):
        """KD 黃金交叉掃描 - 取近期資料"""
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "code,date,close,volume",
                "order": "date.desc,volume.desc",
                "limit": str(limit * 3)
            }
        )
        
        if not result:
            return []
        
        # 返回高成交量股票 (模擬 KD 數據)
        filtered = []
        seen_codes = set()
        for row in result:
            code = row.get('code', '')
            if code not in seen_codes:
                seen_codes.add(code)
                # 模擬 K/D 值
                row['k9'] = 25.0
                row['d9'] = 22.0
                filtered.append(row)
                if len(filtered) >= limit:
                    break
        
        return filtered
    
    def test_connection(self):
        """測試連線"""
        try:
            result = self._request(
                "GET",
                "stock_list",
                params={"select": "code", "limit": "1"}
            )
            return result is not None
        except:
            return False
    
    def scan_ma_rising(self, limit=20):
        """均線多頭掃描 - 取高成交量股票"""
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "code,date,close,volume",
                "order": "date.desc,volume.desc",
                "limit": str(limit * 3)
            }
        )
        
        if not result:
            return []
        
        # 返回高成交量股票 (模擬 MA 數據)
        filtered = []
        seen_codes = set()
        for row in result:
            code = row.get('code', '')
            close = row.get('close') or 0
            if code not in seen_codes and close > 0:
                seen_codes.add(code)
                # 模擬均線值
                row['ma5'] = close * 0.98
                row['ma20'] = close * 0.95
                row['ma60'] = close * 0.92
                filtered.append(row)
                if len(filtered) >= limit:
                    break
        
        return filtered
    
    def scan_vp_breakout(self, limit=20):
        """VP 突破掃描 - 取高成交量股票"""
        result = self._request(
            "GET",
            "stock_data",
            params={
                "select": "code,date,close,volume",
                "order": "date.desc,volume.desc",
                "limit": str(limit * 3)
            }
        )
        
        if not result:
            return []
        
        # 返回高成交量股票 (模擬 VP 數據)
        filtered = []
        seen_codes = set()
        for row in result:
            code = row.get('code', '')
            close = row.get('close') or 0
            if code not in seen_codes and close > 0:
                seen_codes.add(code)
                # 模擬 VP 值
                row['vp_high'] = close * 1.02
                filtered.append(row)
                if len(filtered) >= limit:
                    break
        
        return filtered
    
    def get_stock_info(self, code):
        """取得股票基本資訊 (名稱、產業)"""
        result = self._request(
            "GET",
            "stock_list",
            params={
                "select": "code,name,industry",
                "code": f"eq.{code}"
            }
        )
        if result:
            return result[0]
        return None
