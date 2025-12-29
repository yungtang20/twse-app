import sys
sys.path.insert(0, 'd:\\twse')

# 導入最終修正.py 並測試
import importlib.util
spec = importlib.util.spec_from_file_location("module", "d:\\twse\\最終修正.py")
module = importlib.util.module_from_spec(spec)

# 直接查找 get_latest_market_date 函式
print("Loading module...")

# 嘗試 import 部分代碼
code = open("d:\\twse\\最終修正.py", encoding='utf-8').read()

# 找出 get_latest_market_date 定義
import re
matches = re.findall(r'def get_latest_market_date.*?(?=\ndef |\nclass |\Z)', code, re.DOTALL)
if matches:
    print("Found definition:")
    print(matches[0][:500])
else:
    print("Definition not found.")
    
# 搜尋調用位置
call_matches = re.findall(r'.*get_latest_market_date.*', code)
print(f"\nFound {len(call_matches)} calls to get_latest_market_date:")
for m in call_matches[:10]:
    print(f"  {m.strip()[:80]}")
