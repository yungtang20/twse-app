# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - FinMind 資料抓取器
"""
import time
import requests
from typing import List, Optional
from datetime import datetime, timedelta

from .base import BaseFetcher
from core.models import StockPrice, InstitutionalData

# FinMind API 設定
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN = ""  # 可選，有 token 可提高請求限制


class FinMindFetcher(BaseFetcher):
    """FinMind API 資料抓取器"""
    
    name = "FinMind"
    
    def __init__(self, token: str = "", silent: bool = False):
        super().__init__(silent)
        self.token = token or FINMIND_TOKEN
        self.url = FINMIND_URL
    
    def _request(self, dataset: str, params: dict, retry: int = 3) -> list:
        """統一 API 請求"""
        params['dataset'] = dataset
        if self.token:
            params['token'] = self.token
        
        for attempt in range(retry):
            try:
                resp = requests.get(self.url, params=params, timeout=30, verify=False)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('status') == 200:
                        return data.get('data', [])
                time.sleep(1)
            except Exception as e:
                self.log(f"[FinMind] 請求失敗 (嘗試 {attempt+1}/{retry}): {e}")
                time.sleep(2)
        return []
    
    def fetch_price(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[StockPrice]:
        """抓取股價資料"""
        if not start_date or not end_date:
            start_date, end_date = self.get_default_date_range(365)
        
        params = {
            'stock_id': code,
            'start_date': start_date,
            'end_date': end_date
        }
        
        raw_data = self._request('TaiwanStockPrice', params)
        
        # 轉換為標準格式
        result = []
        for row in raw_data:
            try:
                result.append(StockPrice(
                    code=code,
                    date=row.get('date', ''),
                    open=float(row.get('open', 0)),
                    high=float(row.get('max', 0)),
                    low=float(row.get('min', 0)),
                    close=float(row.get('close', 0)),
                    volume=int(row.get('Trading_Volume', 0)),
                    amount=int(row.get('Trading_money', 0)) if row.get('Trading_money') else None
                ))
            except:
                pass
        
        return result
    
    def fetch_institutional(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[InstitutionalData]:
        """抓取法人買賣超資料"""
        if not start_date or not end_date:
            start_date, end_date = self.get_default_date_range(90)
        
        params = {
            'stock_id': code,
            'start_date': start_date,
            'end_date': end_date
        }
        
        raw_data = self._request('TaiwanStockInstitutionalInvestorsBuySell', params)
        
        # 按日期分組 (因為 FinMind 回傳每個法人一筆)
        from collections import defaultdict
        daily = defaultdict(lambda: {'foreign': 0, 'trust': 0, 'dealer': 0})
        
        for row in raw_data:
            date = row.get('date', '')
            name = row.get('name', '')
            buy = int(row.get('buy', 0) or 0)
            sell = int(row.get('sell', 0) or 0)
            net = buy - sell
            
            if '外資' in name or 'Foreign' in name:
                daily[date]['foreign'] += net
            elif '投信' in name or 'Investment' in name:
                daily[date]['trust'] += net
            elif '自營' in name or 'Dealer' in name:
                daily[date]['dealer'] += net
        
        # 轉換為標準格式
        result = []
        for date, data in sorted(daily.items()):
            result.append(InstitutionalData(
                code=code,
                date=date,
                foreign_buy=data['foreign'] // 1000,  # 股 -> 張
                trust_buy=data['trust'] // 1000,
                dealer_buy=data['dealer'] // 1000
            ))
        
        return result
