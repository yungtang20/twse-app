import sys
import os
import ssl
import requests

# Add current directory to path
sys.path.append(os.getcwd())

# SSL Patch
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass
requests.packages.urllib3.disable_warnings()

from core.fetchers.market_index import MarketIndexFetcher

print("Testing MarketIndexFetcher for 20260102...")
fetcher = MarketIndexFetcher()
result = fetcher.fetch_all('20260102')

print(f"Result: {result}")
if result:
    for r in result:
        print(f"  Date: {r[0]}, Index: {r[1]}, Close: {r[2]}")
else:
    print("No data returned from fetcher.")
