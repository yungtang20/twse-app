import ssl
import requests
import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

# SSL Patch
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass
requests.packages.urllib3.disable_warnings()

from core.fetchers.twstock import TwstockFetcher

print("Testing TwstockFetcher for 1414...")
fetcher = TwstockFetcher()
# fetch_price internally calls _get_twstock which applies the patch
data = fetcher.fetch_price('1414')
print(f"Success! Fetched {len(data)} records.")

if data:
    print(f"Sample: {data[0]}")
else:
    print("No data fetched.")
