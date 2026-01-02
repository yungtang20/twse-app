# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 大盤指數資料抓取器
"""
import time
import requests
from typing import List, Optional, Tuple
from datetime import datetime

from .base import BaseFetcher

class MarketIndexFetcher(BaseFetcher):
    """大盤指數資料抓取器 (TWSE + TPEx)"""
    
    name = "MarketIndex"
    
    # TWSE FMTQIK (每日市場成交資訊)
    API_TWSE = "https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date}"
    
    # TPEx Index Summary
    API_TPEX = "https://www.tpex.org.tw/web/stock/aftertrading/otc_index_summary/OTC_index_summary_result.php?l=zh-tw&d={date}&o=json"
    
    def __init__(self, silent: bool = False):
        super().__init__(silent)
        
    def _safe_num(self, value) -> float:
        if value is None: return 0.0
        try:
            return float(str(value).replace(',', '').replace(' ', ''))
        except:
            return 0.0
            
    def _safe_int(self, value) -> int:
        if value is None: return 0
        try:
            return int(str(value).replace(',', '').replace(' ', ''))
        except:
            return 0

    def fetch_twse(self, date_str: str) -> Optional[Tuple[int, str, float, float, float, float, int]]:
        """抓取 TWSE 加權指數 (TAIEX)"""
        # date_str: YYYYMMDD
        url = self.API_TWSE.format(date=date_str)
        date_int = int(date_str)
        
        try:
            resp = requests.get(url, timeout=15, verify=False)
            if resp.status_code != 200:
                return None
                
            data = resp.json()
            if data.get('stat') != 'OK' or not data.get('data'):
                return None
                
            # 找當天的資料
            for row in data['data']:
                # row[0] = 日期 (民國年), row[1] = 開盤, row[2] = 最高, row[3] = 最低, row[4] = 收盤
                try:
                    parts = row[0].split('/')
                    western_year = int(parts[0]) + 1911
                    row_date_int = int(f"{western_year}{parts[1]}{parts[2]}")
                    
                    if row_date_int == date_int:
                        return (
                            date_int, 'TAIEX',
                            self._safe_num(row[4]), # close
                            self._safe_num(row[1]), # open
                            self._safe_num(row[2]), # high
                            self._safe_num(row[3]), # low
                            self._safe_int(row[5]) if len(row) > 5 else 0 # volume
                        )
                except:
                    continue
            
            return None
            
        except Exception as e:
            self.log(f"[MarketIndexFetcher] TWSE 失敗: {e}")
            return None

    def fetch_tpex(self, date_str: str) -> Optional[Tuple[int, str, float, float, float, float, int]]:
        """抓取 TPEx 櫃買指數"""
        # date_str: YYYYMMDD -> 民國年/MM/DD
        try:
            year = int(date_str[:4]) - 1911
            roc_date = f"{year}/{date_str[4:6]}/{date_str[6:8]}"
            date_int = int(date_str)
            
            url = self.API_TPEX.format(date=roc_date)
            
            resp = requests.get(url, timeout=15, verify=False)
            if resp.status_code != 200:
                return None
                
            data = resp.json()
            if not data.get('aaData'):
                return None
                
            # aaData[0] 通常是櫃買指數
            for row in data['aaData']:
                if '櫃買指數' in str(row[0]) or 'OTC' in str(row[0]).upper():
                    close_val = self._safe_num(row[1]) if len(row) > 1 else 0
                    if close_val > 0:
                        return (date_int, 'TPEX', close_val, 0.0, 0.0, 0.0, 0)
            return None
            
        except Exception as e:
            self.log(f"[MarketIndexFetcher] TPEx 失敗: {e}")
            return None
            
    def fetch_all(self, date_str: str) -> List[Tuple]:
        """抓取所有大盤指數"""
        result = []
        
        # TWSE
        twse_data = self.fetch_twse(date_str)
        if twse_data:
            result.append(twse_data)
            self.log(f"✓ TWSE 指數: {twse_data[2]}")
        
        time.sleep(0.5)
        
        # TPEx
        tpex_data = self.fetch_tpex(date_str)
        if tpex_data:
            result.append(tpex_data)
            self.log(f"✓ TPEx 指數: {tpex_data[2]}")
            
        return result
