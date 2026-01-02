# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 資料抓取基底類別
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.models import StockPrice, InstitutionalData, MarginData


class BaseFetcher(ABC):
    """資料抓取基底類別 (抽象)"""
    
    name: str = "BaseFetcher"
    
    def __init__(self, silent: bool = False):
        self.silent = silent
    
    def log(self, msg: str):
        """輸出日誌 (可靜默)"""
        if not self.silent:
            print(msg, flush=True)
    
    @abstractmethod
    def fetch_price(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[StockPrice]:
        """
        抓取股價資料
        :param code: 股票代號
        :param start_date: 開始日期 (YYYY-MM-DD)
        :param end_date: 結束日期 (YYYY-MM-DD)
        :return: StockPrice 列表
        """
        pass
    
    def fetch_institutional(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[InstitutionalData]:
        """
        抓取法人買賣超資料 (子類別可覆寫)
        """
        return []
    
    def fetch_margin(
        self, 
        code: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> List[MarginData]:
        """
        抓取融資融券資料 (子類別可覆寫)
        """
        return []
    
    @staticmethod
    def get_default_date_range(days_back: int = 365) -> tuple:
        """取得預設日期範圍"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        return start_date, end_date
