# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - twstock 資料抓取器 (備援)
"""
import time
from typing import List, Optional
from datetime import datetime, timedelta

from .base import BaseFetcher
from core.models import StockPrice


class TwstockFetcher(BaseFetcher):
    """twstock 資料抓取器 (本地套件)"""
    
    name = "twstock"
    
    def __init__(self, silent: bool = False):
        super().__init__(silent)
        self._twstock = None
    
    def _get_twstock(self):
        """延遲載入 twstock"""
        if self._twstock is None:
            import twstock
            self._twstock = twstock
        return self._twstock
    
    def fetch_price(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[StockPrice]:
        """抓取股價資料"""
        twstock = self._get_twstock()
        
        # 計算起始年月
        if start_date:
            try:
                dt = datetime.strptime(start_date, "%Y-%m-%d")
            except:
                dt = datetime.now() - timedelta(days=365)
        else:
            dt = datetime.now() - timedelta(days=365)
        
        try:
            # 加入延遲避免被封鎖
            import numpy as np
            time.sleep(np.random.uniform(2, 4))
            
            stock = twstock.Stock(code)
            raw_data = stock.fetch_from(dt.year, dt.month)
            
            # 轉換為標準格式
            result = []
            for row in raw_data:
                try:
                    result.append(StockPrice(
                        code=code,
                        date=row.date.strftime('%Y-%m-%d'),
                        open=float(row.open) if row.open else 0,
                        high=float(row.high) if row.high else 0,
                        low=float(row.low) if row.low else 0,
                        close=float(row.close) if row.close else 0,
                        volume=int(row.capacity) if row.capacity else 0,
                        amount=int(row.turnover) if row.turnover else None
                    ))
                except:
                    pass
            
            return result
            
        except Exception as e:
            self.log(f"[twstock] 抓取失敗 {code}: {e}")
            return []
    
    def fetch_institutional(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List:
        """twstock 不支援法人資料"""
        return []
