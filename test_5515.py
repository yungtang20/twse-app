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

print("Testing TwstockFetcher for 5515 (建國)...")
fetcher = TwstockFetcher()
data = fetcher.fetch_price('5515')
print(f"Result: Fetched {len(data)} records.")
if data:
    print(f"Sample: {data[0]}")
