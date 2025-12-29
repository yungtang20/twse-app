import abc
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import twstock
from twstock.stock import TPEXFetcher

# Configure logging
logger = logging.getLogger(__name__)

# Constants (Moved from Config)
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
# TODO: Move token to environment variable or secure config
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMi0yMiAwMTo1MDoxMiIsInVzZXJfaWQiOiJ5dW5ndGFuZyAiLCJpcCI6IjExMS43MS4yMTMuNzAifQ._mhmrmnS4SIWRS6Ln9rE0-fZ9j4JZLdq1b7s-m3eDFQ"
REQUEST_TIMEOUT = 30

def safe_num(val):
    """Convert value to float, return None if failed"""
    try:
        if val is None or val == '':
            return None
        return float(val)
    except (ValueError, TypeError):
        return None

def safe_int(val):
    """Convert value to int, return None if failed"""
    try:
        if val is None or val == '':
            return None
        return int(float(val))
    except (ValueError, TypeError):
        return None

class DataSource(abc.ABC):
    """Abstract Base Class for Data Sources"""
    
    def __init__(self, progress_tracker=None):
        self.progress = progress_tracker
        self.name = "BaseDataSource"

    @abc.abstractmethod
    def fetch_history(self, stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch historical stock data.
        
        Args:
            stock_code: Stock code (e.g., "2330")
            start_date: Start date in "YYYY-MM-DD" format
            end_date: End date in "YYYY-MM-DD" format
            
        Returns:
            pd.DataFrame with columns ['date', 'open', 'high', 'low', 'close', 'volume', 'amount'] or None
        """
        pass

class FinMindDataSource(DataSource):
    """FinMind API Data Source"""
    
    def __init__(self, progress_tracker=None, silent=False):
        super().__init__(progress_tracker)
        self.name = "FinMind"
        self.url = FINMIND_URL
        self.token = FINMIND_TOKEN
        self.silent = silent
    
    def fetch_history(self, stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, retry: int = 3) -> Optional[pd.DataFrame]:
        try:
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            params = {
                "dataset": "TaiwanStockPrice",
                "data_id": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "token": self.token,
            }
            
            for attempt in range(retry):
                try:
                    if not self.silent and self.progress:
                        self.progress.info(f"{self.name}: Fetching {stock_code} ({attempt+1}/{retry})")
                    
                    response = requests.get(
                        self.url, 
                        params=params, 
                        timeout=REQUEST_TIMEOUT,
                        verify=False # SSL verify disabled as per original code
                    )
                    
                    if response.status_code == 429:
                        if not self.silent and self.progress:
                            self.progress.warning(f"{self.name}: Rate limit, waiting 2s")
                        time.sleep(2)
                        continue
                    
                    if response.status_code != 200:
                        if not self.silent and self.progress:
                            self.progress.warning(f"{self.name}: Status {response.status_code}")
                        if attempt < retry - 1:
                            time.sleep(1)
                        continue
                    
                    data = response.json()
                    
                    if not data.get('data') or len(data['data']) == 0:
                        return None
                    
                    rows = []
                    for item in data['data']:
                        try:
                            date = item.get('date')
                            if not date:
                                continue
                            close = safe_num(item.get('close'))
                            if close is not None and close > 0:
                                rows.append({
                                    'date': date,
                                    'open': safe_num(item.get('open')),
                                    'high': safe_num(item.get('max')),
                                    'low': safe_num(item.get('min')),
                                    'close': close,
                                    'volume': safe_int(item.get('Trading_Volume')),
                                    'amount': safe_num(item.get('Trading_money'))
                                })
                        except Exception:
                            continue
                    
                    if not rows:
                        return None
                    
                    df = pd.DataFrame(rows)
                    df = df.drop_duplicates(subset=['date'], keep='first')
                    df = df[df['close'] > 0]
                    df = df.sort_values('date').reset_index(drop=True)
                    
                    if not self.silent and self.progress:
                        self.progress.success(f"{self.name}: Fetched {len(df)} records for {stock_code}")
                        
                    return df
                    
                except requests.exceptions.RequestException as e:
                    if not self.silent and self.progress:
                        self.progress.warning(f"{self.name} Request Error: {e}")
                    if attempt < retry - 1:
                        time.sleep(1)
                except Exception as e:
                    logger.error(f"{self.name} Error: {e}")
                    return None
                    
            return None
            
        except Exception as e:
            logger.error(f"{self.name} Critical Error: {e}")
            return None

class OfficialAPIDataSource(DataSource):
    """Official API Data Source (TWSE/TPEx via twstock)"""
    
    def __init__(self, progress_tracker=None, silent=False):
        super().__init__(progress_tracker)
        self.name = "OfficialAPI"
        self.silent = silent
        
    def fetch_history(self, stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        try:
            if not self.silent and self.progress:
                self.progress.info(f"{self.name}: Fetching {stock_code}")
                
            # Random delay to avoid rate limits
            time.sleep(np.random.uniform(3, 6))
            
            stock = twstock.Stock(stock_code)
            
            # Determine start year/month
            if start_date:
                try:
                    dt = datetime.strptime(start_date, "%Y-%m-%d")
                except ValueError:
                    dt = datetime.now() - timedelta(days=1095)
            else:
                dt = datetime.now() - timedelta(days=1095)
            
            # Use fetch_from to get data
            # Note: twstock.fetch_from handles both TWSE and TPEx logic internally
            try:
                stock.fetch_from(dt.year, dt.month)
            except Exception as e:
                if not self.silent and self.progress:
                    self.progress.warning(f"{self.name}: fetch_from failed: {e}")
                # Fallback to fetch_31
                try:
                    stock.fetch_31()
                except:
                    return None

            if not stock.data:
                 # Try fetch_31 again if no data
                try:
                    stock.fetch_31()
                except:
                    pass
                if not stock.data:
                    return None

            rows = []
            for d in stock.data:
                d_str = d.date.strftime("%Y-%m-%d")
                
                if start_date and d_str < start_date:
                    continue
                if end_date and d_str > end_date:
                    continue
                    
                rows.append({
                    'date': d_str,
                    'open': d.open,
                    'high': d.high,
                    'low': d.low,
                    'close': d.close,
                    'volume': d.capacity,
                    'amount': d.turnover
                })
                
            if not rows:
                return None
                
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['date'], keep='first')
            df = df.sort_values('date').reset_index(drop=True)
            
            if not self.silent and self.progress:
                self.progress.success(f"{self.name}: Fetched {len(df)} records for {stock_code}")
                
            return df
            
        except Exception as e:
            if not self.silent and self.progress:
                self.progress.warning(f"{self.name} Error: {e}")
            return None

class DataSourceManager:
    """Manages data sources with failover logic"""
    
    def __init__(self, progress_tracker=None):
        self.sources = [
            FinMindDataSource(progress_tracker),
            OfficialAPIDataSource(progress_tracker)
        ]
        self.progress = progress_tracker
        
    def fetch_history(self, stock_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Try fetching from sources in order until successful"""
        for source in self.sources:
            df = source.fetch_history(stock_code, start_date, end_date)
            if df is not None and not df.empty:
                return df
            
            if self.progress:
                self.progress.warning(f"Source {source.name} failed for {stock_code}, trying next...")
                
        return None
