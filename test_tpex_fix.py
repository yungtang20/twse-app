import sys
import os
import ssl
import requests

sys.path.append(os.getcwd())

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except:
    pass
requests.packages.urllib3.disable_warnings()

print("=== Testing InstitutionalFetcher.fetch_tpex ===")
from core.fetchers.institutional import InstitutionalFetcher
fetcher = InstitutionalFetcher()
data = fetcher.fetch_tpex('20260102')
print(f"TPEx 法人: {len(data)} 筆")
if data:
    print(f"Sample: {data[0]}")
