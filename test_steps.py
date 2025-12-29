import sys
sys.path.insert(0, '.')
from 最終修正 import step1_fetch_stock_list, step2_download_tpex_daily, step3_download_twse_daily

print("=" * 60)
print("測試 Step 1: 更新股票清單")
print("=" * 60)
step1_fetch_stock_list()

print("\n" + "=" * 60)
print("測試 Step 2: 下載 TPEx")
print("=" * 60)
step2_download_tpex_daily()

print("\n" + "=" * 60)
print("測試 Step 3: 下載 TWSE")
print("=" * 60)
step3_download_twse_daily()
