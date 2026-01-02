# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 法人買賣超資料抓取器
"""
import time
import requests
from typing import List, Optional, Tuple
from datetime import datetime

from .base import BaseFetcher
from core.models import InstitutionalData


class InstitutionalFetcher(BaseFetcher):
    """法人買賣超資料抓取器 (官方 OpenAPI)"""
    
    name = "Institutional"
    
    API_TWSE = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date}&selectType=ALLBUT0999"
    # 新版 OpenAPI (2024 改版後)
    API_TPEX = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_institution_netbuy"
    
    def __init__(self, silent: bool = False):
        super().__init__(silent)
    
    def fetch_price(self, code, start_date=None, end_date=None):
        """InstitutionalFetcher 不支援股價抓取"""
        return []
    
    def _safe_int(self, value) -> int:
        """安全轉換為整數"""
        if value is None:
            return 0
        try:
            return int(str(value).replace(',', '').replace(' ', ''))
        except:
            return 0
    
    def fetch_twse(self, date_str: str) -> List[InstitutionalData]:
        """抓取上市法人資料"""
        url = self.API_TWSE.format(date=date_str.replace('-', ''))
        
        try:
            resp = requests.get(url, timeout=30, verify=False)
            if resp.status_code != 200:
                return []
            
            data = resp.json()
            if data.get('stat') != 'OK' or not data.get('data'):
                return []
            
            result = []
            for row in data['data']:
                try:
                    code = str(row[0]).strip()
                    if len(code) != 4 or not code.isdigit():
                        continue
                    
                    # T86 欄位索引 (根據觀察):
                    # 0:代號, 1:名稱
                    # 2:外陸資買進, 3:外陸資賣出, 4:外陸資淨買賣
                    # 5:投信買進, 6:投信賣出, 7:投信淨買賣
                    # 8:自營(自行)買, 9:自營(自行)賣, 10:自營(自行)淨
                    # 11:自營(避險)買, 12:自營(避險)賣, 13:自營(避險)淨
                    # 14:自營(合計)買, 15:自營(合計)賣, 16:自營(合計)淨
                    
                    # 處理自營商 (若只有部分欄位，則嘗試加總)
                    d_buy = 0
                    d_sell = 0
                    
                    if len(row) >= 17:
                        d_buy = self._safe_int(row[14])
                        d_sell = self._safe_int(row[15])
                    elif len(row) >= 11:
                        # 舊格式或簡化格式，嘗試加總自行+避險 (如果有的話)
                        # 這裡簡化處理，若無合計欄位則取自行
                        d_buy = self._safe_int(row[8])
                        d_sell = self._safe_int(row[9])
                    
                    result.append(InstitutionalData(
                        code=code,
                        date=f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}" if len(date_str) == 8 else date_str,
                        foreign_buy=self._safe_int(row[2]) // 1000,
                        foreign_sell=self._safe_int(row[3]) // 1000,
                        trust_buy=self._safe_int(row[5]) // 1000,
                        trust_sell=self._safe_int(row[6]) // 1000,
                        dealer_buy=d_buy // 1000,
                        dealer_sell=d_sell // 1000
                    ))
                except:
                    continue
            
            return result
            
        except Exception as e:
            self.log(f"[InstitutionalFetcher] TWSE 失敗: {e}")
            return []
    
    def fetch_tpex(self, date_str: str) -> List[InstitutionalData]:
        """抓取上櫃法人資料 (使用新版 OpenAPI)"""
        try:
            resp = requests.get(self.API_TPEX, timeout=30, verify=False)
            
            if resp.status_code != 200:
                self.log(f"[InstitutionalFetcher] TPEx API 回傳 {resp.status_code}")
                return []
            
            data = resp.json()
            if not data:
                return []
            
            result = []
            # 新版 OpenAPI 回傳格式為 list of dict
            # 欄位: "代號", "名稱", "外資及陸資(不含外資自營商)-買進股數", "外資及陸資(不含外資自營商)-賣出股數", "外資及陸資(不含外資自營商)-買賣超股數"
            # "投信-買進股數", "投信-賣出股數", "投信-買賣超股數"
            # "自營商-買進股數(自行買賣)", "自營商-賣出股數(自行買賣)", "自營商-買賣超股數(自行買賣)"
            # "自營商-買進股數(避險)", "自營商-賣出股數(避險)", "自營商-買賣超股數(避險)"
            for row in data:
                try:
                    code = str(row.get('代號', '')).strip()
                    if len(code) != 4 or not code.isdigit():
                        continue
                    
                    # 外資
                    f_buy = self._safe_int(row.get('外資及陸資(不含外資自營商)-買進股數', 0))
                    f_sell = self._safe_int(row.get('外資及陸資(不含外資自營商)-賣出股數', 0))
                    
                    # 投信
                    t_buy = self._safe_int(row.get('投信-買進股數', 0))
                    t_sell = self._safe_int(row.get('投信-賣出股數', 0))
                    
                    # 自營商 (自行 + 避險)
                    d_buy_self = self._safe_int(row.get('自營商-買進股數(自行買賣)', 0))
                    d_sell_self = self._safe_int(row.get('自營商-賣出股數(自行買賣)', 0))
                    d_buy_hedge = self._safe_int(row.get('自營商-買進股數(避險)', 0))
                    d_sell_hedge = self._safe_int(row.get('自營商-賣出股數(避險)', 0))
                    d_buy = d_buy_self + d_buy_hedge
                    d_sell = d_sell_self + d_sell_hedge
                    
                    result.append(InstitutionalData(
                        code=code,
                        date=date_str if '-' in date_str else f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                        foreign_buy=f_buy // 1000,
                        foreign_sell=f_sell // 1000,
                        trust_buy=t_buy // 1000,
                        trust_sell=t_sell // 1000,
                        dealer_buy=d_buy // 1000,
                        dealer_sell=d_sell // 1000
                    ))
                except:
                    continue
            
            return result
            
        except Exception as e:
            self.log(f"[InstitutionalFetcher] TPEx 失敗: {e}")
            return []
    
    def fetch_market_summary(self, date_str: str) -> List[InstitutionalData]:
        """抓取大盤法人買賣超匯總 (0000)"""
        url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json"
        # BFI82U 似乎支援 date 參數? 嘗試加上
        # url = f"{url}&date={date_str.replace('-', '')}"
        # 根據經驗，BFI82U 支援 date, 但原程式沒加。這裡加上以支援歷史查詢。
        url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json&date={date_str.replace('-', '')}"
        
        try:
            resp = requests.get(url, timeout=30, verify=False)
            if resp.status_code != 200:
                return []
            
            data = resp.json()
            if data.get('stat') != 'OK' or not data.get('data'):
                return []
            
            rows = data['data']
            # rows 結構: [名稱, 買進金額, 賣出金額, 買賣差額]
            # 通常:
            # 0: 自營商(自行買賣)
            # 1: 自營商(避險)
            # 2: 投信
            # 3: 外資及陸資
            
            # 需確認 rows 順序，通常是固定的，但最好用名稱判斷
            f_buy = f_sell = t_buy = t_sell = d_buy = d_sell = 0
            
            for row in rows:
                name = row[0]
                buy = self._safe_int(row[1])
                sell = self._safe_int(row[2])
                
                if '外資' in name:
                    f_buy += buy
                    f_sell += sell
                elif '投信' in name:
                    t_buy += buy
                    t_sell += sell
                elif '自營商' in name:
                    d_buy += buy
                    d_sell += sell
            
            # 注意: 這裡是金額 (元)，不是張數。但為了統一格式，我們存入 int 欄位。
            # 使用者需知道 code='0000' 時單位是元。
            
            return [InstitutionalData(
                code='0000',
                date=f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}" if len(date_str) == 8 else date_str,
                foreign_buy=f_buy,
                foreign_sell=f_sell,
                trust_buy=t_buy,
                trust_sell=t_sell,
                dealer_buy=d_buy,
                dealer_sell=d_sell
            )]
            
        except Exception as e:
            self.log(f"[InstitutionalFetcher] Market Summary 失敗: {e}")
            return []

    def fetch_all(self, date_str: str) -> List[InstitutionalData]:
        """抓取所有市場法人資料 (含大盤匯總)"""
        result = []
        
        # TWSE
        twse_data = self.fetch_twse(date_str)
        result.extend(twse_data)
        self.log(f"✓ TWSE 法人: {len(twse_data)} 筆")
        
        time.sleep(1)
        
        # TPEx
        tpex_data = self.fetch_tpex(date_str)
        result.extend(tpex_data)
        self.log(f"✓ TPEx 法人: {len(tpex_data)} 筆")
        
        time.sleep(1)
        
        # Market Summary
        summary_data = self.fetch_market_summary(date_str)
        result.extend(summary_data)
        if summary_data:
            self.log(f"✓ 大盤匯總 (0000) 已抓取")
        
        return result
