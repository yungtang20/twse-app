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

from core.fetchers.twstock import TwstockFetcher

print("Testing TwstockFetcher for 8093 (保銳)...")
fetcher = TwstockFetcher()
data = fetcher.fetch_price('8093')
print(f"Result: Fetched {len(data)} records.")
