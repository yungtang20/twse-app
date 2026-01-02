# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 融資融券資料抓取器
"""
import time
import requests
import random
from typing import List, Optional
from datetime import datetime

from .base import BaseFetcher
from core.models import MarginData

class MarginFetcher(BaseFetcher):
    """融資融券資料抓取器 (官方 OpenAPI 優先)"""
    
    name = "Margin"
    
    # TWSE MI_MARGN (網頁版 API)
    API_TWSE = "https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={date}&selectType=ALL"
    
    # TPEx Margin Balance (網頁版 API)
    API_TPEX = "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={date}&s=0,asc,0"
    
    def __init__(self, silent: bool = False):
        super().__init__(silent)
        
    def _safe_int(self, value) -> int:
        if value is None: return 0
        try:
            return int(str(value).replace(',', '').replace(' ', ''))
        except:
            return 0
            
    def _safe_float(self, value) -> float:
        if value is None: return 0.0
        try:
            return float(str(value).replace(',', '').replace(' ', ''))
        except:
            return 0.0

    def fetch_twse(self, date_str: str) -> List[MarginData]:
        """抓取上市融資融券資料"""
        # date_str: YYYYMMDD
        url = self.API_TWSE.format(date=date_str)
        
        try:
            time.sleep(random.uniform(2.0, 4.0))
            resp = requests.get(url, timeout=15, verify=False)
            if resp.status_code != 200:
                return []
                
            data = resp.json()
            if data.get('stat') != 'OK' or not data.get('data'):
                return []
                
            result = []
            for row in data['data']:
                try:
                    code = str(row[0]).strip()
                    if len(code) != 4: continue
                    
                    # TWSE 欄位:
                    # 0:代號, 1:名稱
                    # 2:融資買進, 3:融資賣出, 4:融資現償, 5:融資前日餘額, 6:融資今日餘額, 7:融資限額, 8:融資使用率
                    # 9:融券買進, 10:融券賣出, 11:融券現償, 12:融券前日餘額, 13:融券今日餘額, 14:融券限額, 15:融券使用率
                    
                    result.append(MarginData(
                        date_int=int(date_str),
                        code=code,
                        margin_buy=self._safe_int(row[2]),
                        margin_sell=self._safe_int(row[3]),
                        margin_redemp=self._safe_int(row[4]),
                        margin_balance=self._safe_int(row[6]),
                        margin_util_rate=self._safe_float(row[8]),
                        short_buy=self._safe_int(row[9]),
                        short_sell=self._safe_int(row[10]),
                        short_redemp=self._safe_int(row[11]),
                        short_balance=self._safe_int(row[13]),
                        short_util_rate=self._safe_float(row[15])
                    ))
                except:
                    continue
            return result
            
        except Exception as e:
            self.log(f"[MarginFetcher] TWSE 失敗: {e}")
            return []

    def fetch_tpex(self, date_str: str) -> List[MarginData]:
        """抓取上櫃融資融券資料"""
        # date_str: YYYYMMDD -> 民國年/MM/DD
        try:
            year = int(date_str[:4]) - 1911
            roc_date = f"{year}/{date_str[4:6]}/{date_str[6:8]}"
            
            url = self.API_TPEX.format(date=roc_date)
            
            time.sleep(random.uniform(1.5, 3.0))
            resp = requests.get(url, timeout=15, verify=False)
            if resp.status_code != 200:
                return []
                
            data = resp.json()
            if not data.get('tables'):
                return []
                
            result = []
            # TPEx 可能有多個 table, 通常第一個是主要資料
            for row in data['tables'][0]['data']:
                try:
                    code = str(row[0]).strip()
                    if len(code) != 4: continue
                    
                    # TPEx 欄位:
                    # 0:代號, 1:名稱
                    # 2:融資前日餘額, 3:融資買進, 4:融資賣出, 5:融資現償, 6:融資今日餘額, 7:融資使用率, 8:融資限額
                    # 9:融券前日餘額, 10:融券買進, 11:融券賣出, 12:融券現償, 13:融券今日餘額, 14:融券使用率, 15:融券限額
                    
                    result.append(MarginData(
                        date_int=int(date_str),
                        code=code,
                        margin_buy=self._safe_int(row[3]),
                        margin_sell=self._safe_int(row[4]),
                        margin_redemp=self._safe_int(row[5]),
                        margin_balance=self._safe_int(row[6]),
                        margin_util_rate=self._safe_float(row[7]),
                        short_buy=self._safe_int(row[10]),
                        short_sell=self._safe_int(row[11]),
                        short_redemp=self._safe_int(row[12]),
                        short_balance=self._safe_int(row[13]),
                        short_util_rate=self._safe_float(row[14])
                    ))
                except:
                    continue
            return result
            
        except Exception as e:
            self.log(f"[MarginFetcher] TPEx 失敗: {e}")
            return []
            
    def fetch_all(self, date_str: str) -> List[MarginData]:
        """抓取所有市場融資融券資料"""
        result = []
        
        # TWSE
        twse_data = self.fetch_twse(date_str)
        result.extend(twse_data)
        self.log(f"✓ TWSE 融資融券: {len(twse_data)} 筆")
        
        # TPEx
        tpex_data = self.fetch_tpex(date_str)
        result.extend(tpex_data)
        self.log(f"✓ TPEx 融資融券: {len(tpex_data)} 筆")
        
        return result
