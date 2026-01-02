# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 統一資料模型
所有資料抓取都必須轉換為這些標準格式
"""
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime


@dataclass
class StockPrice:
    """股價資料 (標準格式)"""
    code: str               # 股票代號
    date: str               # 日期 (YYYY-MM-DD)
    open: float             # 開盤價
    high: float             # 最高價
    low: float              # 最低價
    close: float            # 收盤價
    volume: int             # 成交量 (股)
    amount: Optional[int] = None  # 成交金額 (元)
    
    @property
    def date_int(self) -> int:
        """轉換為整數日期 (YYYYMMDD)"""
        return int(self.date.replace('-', ''))
    
    def to_dict(self) -> dict:
        """轉換為字典"""
        return {
            'code': self.code,
            'date': self.date,
            'date_int': self.date_int,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount
        }


@dataclass
class InstitutionalData:
    """法人買賣超資料 (標準格式)"""
    code: str               # 股票代號
    date: str               # 日期 (YYYY-MM-DD)
    
    # 外資
    foreign_buy: int = 0    # 外資買進 (張)
    foreign_sell: int = 0   # 外資賣出 (張)
    
    # 投信
    trust_buy: int = 0      # 投信買進 (張)
    trust_sell: int = 0     # 投信賣出 (張)
    
    # 自營商
    dealer_buy: int = 0     # 自營商買進 (張)
    dealer_sell: int = 0    # 自營商賣出 (張)
    
    @property
    def date_int(self) -> int:
        return int(self.date.replace('-', ''))
    
    @property
    def foreign_net(self) -> int:
        return self.foreign_buy - self.foreign_sell
        
    @property
    def trust_net(self) -> int:
        return self.trust_buy - self.trust_sell
        
    @property
    def dealer_net(self) -> int:
        return self.dealer_buy - self.dealer_sell
    
    @property
    def total_net(self) -> int:
        """三大法人合計買賣超"""
        return self.foreign_net + self.trust_net + self.dealer_net
    
    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'date': self.date,
            'date_int': self.date_int,
            'foreign_buy': self.foreign_buy,
            'foreign_sell': self.foreign_sell,
            'foreign_net': self.foreign_net,
            'trust_buy': self.trust_buy,
            'trust_sell': self.trust_sell,
            'trust_net': self.trust_net,
            'dealer_buy': self.dealer_buy,
            'dealer_sell': self.dealer_sell,
            'dealer_net': self.dealer_net,
            'total_net': self.total_net
        }


@dataclass
class MarginData:
    """融資融券資料 (標準格式)"""
    code: str               # 股票代號
    date: str               # 日期 (YYYY-MM-DD)
    margin_buy: int         # 融資買進 (張)
    margin_sell: int        # 融資賣出 (張)
    margin_balance: int     # 融資餘額 (張)
    short_buy: int          # 融券買進 (張)
    short_sell: int         # 融券賣出 (張)
    short_balance: int      # 融券餘額 (張)
    margin_redemp: int = 0  # 融資現償 (張)
    margin_util_rate: float = 0.0  # 融資使用率 (%)
    short_redemp: int = 0   # 融券現償 (張)
    short_util_rate: float = 0.0   # 融券使用率 (%)
    
    @property
    def date_int(self) -> int:
        return int(self.date.replace('-', ''))
    
    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'date': self.date,
            'date_int': self.date_int,
            'margin_buy': self.margin_buy,
            'margin_sell': self.margin_sell,
            'margin_balance': self.margin_balance,
            'margin_redemp': self.margin_redemp,
            'margin_util_rate': self.margin_util_rate,
            'short_buy': self.short_buy,
            'short_sell': self.short_sell,
            'short_balance': self.short_balance,
            'short_redemp': self.short_redemp,
            'short_util_rate': self.short_util_rate
        }


@dataclass
class StockMeta:
    """股票基本資料 (標準格式)"""
    code: str               # 股票代號
    name: str               # 股票名稱
    market: str             # 市場 (TWSE/TPEx)
    list_date: Optional[str] = None  # 上市日期
    industry: Optional[str] = None   # 產業類別
    
    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'name': self.name,
            'market': self.market,
            'list_date': self.list_date,
            'industry': self.industry
        }
