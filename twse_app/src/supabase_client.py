"""
Supabase Client 封裝
提供與 Supabase 的 REST API 互動介面
"""
import requests
import json
from datetime import datetime, timedelta

class SupabaseClient:
    """Supabase REST API Client"""
    
    def __init__(self, url, key):
        self.url = url.rstrip('/')
        self.key = key
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json'
        }
    
    def fetch_stock_data(self, code=None, start_date=None, end_date=None, limit=450):
        """
        取得股票歷史資料
        
        Args:
            code: 股票代碼（可選，不指定則取全部）
            start_date: 開始日期 (YYYY-MM-DD)
            end_date: 結束日期 (YYYY-MM-DD)
            limit: 筆數限制
        
        Returns:
            list: 股票資料列表
        """
        endpoint = f'{self.url}/rest/v1/stock_data'
        params = {}
        
        if code:
            params['code'] = f'eq.{code}'
        
        if start_date:
            params['date'] = f'gte.{start_date}'
        
        if end_date:
            if 'date' in params:
                # 需要使用 and 查詢
                params['date'] = f'gte.{start_date}'
                params['date'] += f',lte.{end_date}'
            else:
                params['date'] = f'lte.{end_date}'
        
        params['order'] = 'date.desc'
        params['limit'] = limit
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching stock data: {e}")
            return []
    
    def fetch_latest_indicators(self, min_volume=0):
        """
        取得最新指標快取（用於掃描）
        
        Args:
            min_volume: 最小成交量過濾
        
        Returns:
            dict: {code: indicator_data}
        """
        # 取得最新日期
        endpoint = f'{self.url}/rest/v1/stock_data'
        params = {
            'select': 'date',
            'order': 'date.desc',
            'limit': 1
        }
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            latest_data = response.json()
            
            if not latest_data:
                return {}
            
            latest_date = latest_data[0]['date']
            
            # 取得該日期的所有股票資料
            params = {
                'date': f'eq.{latest_date}',
                'order': 'code.asc',
                'limit': 2000
            }
            
            if min_volume > 0:
                params['volume'] = f'gte.{min_volume}'
            
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # 轉換為 dict
            result = {}
            for item in data:
                result[item['code']] = item
            
            return result
            
        except Exception as e:
            print(f"Error fetching latest indicators: {e}")
            return {}
    
    def upsert_stock_data(self, records):
        """
        批量寫入/更新股票資料
        
        Args:
            records: 資料列表 (list of dict)
        
        Returns:
            bool: 成功與否
        """
        endpoint = f'{self.url}/rest/v1/stock_data'
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=records,
                timeout=30
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error upserting stock data: {e}")
            return False
    
    def fetch_watchlist(self, device_id):
        """
        取得自選股
        
        Args:
            device_id: 裝置識別碼
        
        Returns:
            list: 自選股列表
        """
        endpoint = f'{self.url}/rest/v1/watchlist'
        params = {
            'device_id': f'eq.{device_id}',
            'order': 'added_at.desc'
        }
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching watchlist: {e}")
            return []
    
    def add_watchlist(self, device_id, code, name, notes=''):
        """
        新增自選股
        
        Args:
            device_id: 裝置識別碼
            code: 股票代碼
            name: 股票名稱
            notes: 備註
        
        Returns:
            bool: 成功與否
        """
        endpoint = f'{self.url}/rest/v1/watchlist'
        data = {
            'device_id': device_id,
            'code': code,
            'name': name,
            'notes': notes
        }
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=data,
                timeout=10
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error adding to watchlist: {e}")
            return False
    
    def delete_watchlist(self, device_id, code):
        """
        刪除自選股
        
        Args:
            device_id: 裝置識別碼
            code: 股票代碼
        
        Returns:
            bool: 成功與否
        """
        endpoint = f'{self.url}/rest/v1/watchlist'
        params = {
            'device_id': f'eq.{device_id}',
            'code': f'eq.{code}'
        }
        
        try:
            response = requests.delete(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error deleting from watchlist: {e}")
            return False
