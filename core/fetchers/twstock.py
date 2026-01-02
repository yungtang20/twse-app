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
        """延遲載入 twstock 並套用 Patch"""
        if self._twstock is None:
            import twstock
            
            # [Patch] 修復 TWSE/TPEx 回傳欄位數變更導致的錯誤
            # Error: Data.__new__() takes 10 positional arguments but 11 were given
            
            def patch_make_datatuple(original_method):
                def patched_method(self, data):
                    # 只取前 9 個欄位 (符合 DATATUPLE 定義)
                    if len(data) > 9:
                        data = data[:9]
                    return original_method(self, data)
                return patched_method

            if hasattr(twstock.stock.TWSEFetcher, '_make_datatuple'):
                twstock.stock.TWSEFetcher._make_datatuple = patch_make_datatuple(twstock.stock.TWSEFetcher._make_datatuple)
                
            if hasattr(twstock.stock.TPEXFetcher, '_make_datatuple'):
                twstock.stock.TPEXFetcher._make_datatuple = patch_make_datatuple(twstock.stock.TPEXFetcher._make_datatuple)

            self._twstock = twstock
        return self._twstock
    
    def fetch_price(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[StockPrice]:
        """抓取股價資料"""
        # Guard Clause: 跳過非個股代碼 (如 0000 大盤指數)
        if not code or len(code) != 4 or not code.isdigit() or code == '0000':
            return []
            
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
            # 針對櫃買中心 (TPEx) 的特殊處理
            is_tpex = False
            try:
                if code in twstock.codes and twstock.codes[code].market == '上櫃':
                    is_tpex = True
            except:
                pass
                
            if is_tpex and ("Expecting value" in str(e) or "404" in str(e)):
                self.log(f"[twstock] 抓取失敗 {code}: 櫃買中心網站改版，API 目前失效 (預期中)")
            else:
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
