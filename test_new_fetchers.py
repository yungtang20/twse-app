import sys
import os
import ssl
import requests

sys.path.append(os.getcwd())

# SSL Patch
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass
requests.packages.urllib3.disable_warnings()

print("=== Testing InstitutionalFetcher.fetch_tpex ===")
from core.fetchers.institutional import InstitutionalFetcher
fetcher = InstitutionalFetcher()
data = fetcher.fetch_tpex('20260102')
print(f"TPEx 法人: {len(data)} 筆")
if data:
    print(f"Sample: {data[0]}")

print("\n=== Testing MarginFetcher.fetch_market_summary ===")
from core.fetchers.margin import MarginFetcher
margin_fetcher = MarginFetcher()
margin_data = margin_fetcher.fetch_market_summary('20260102')
print(f"大盤融資券: {len(margin_data)} 筆")
if margin_data:
    print(f"Data: {margin_data[0]}")
