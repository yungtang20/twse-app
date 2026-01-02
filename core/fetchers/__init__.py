# -*- coding: utf-8 -*-
"""
台灣股市分析系統 - 資料抓取模組
"""
from .base import BaseFetcher
from .finmind import FinMindFetcher
from .twstock import TwstockFetcher
from .institutional import InstitutionalFetcher

__all__ = [
    'BaseFetcher',
    'FinMindFetcher', 
    'TwstockFetcher',
    'InstitutionalFetcher',
]

