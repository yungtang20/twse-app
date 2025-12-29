import re

# 讀取檔案
code = open("d:\\twse\\最終修正.py", encoding='utf-8').read()

# 找到所有 def get_latest_market_date 的位置
for m in re.finditer(r'def get_latest_market_date', code):
    start = m.start()
    # 找到函式開頭的行號
    line_num = code[:start].count('\n') + 1
    print(f"Found 'def get_latest_market_date' at line {line_num}")
    
    # 印出附近的代碼
    lines = code.split('\n')
    for i in range(max(0, line_num-1), min(len(lines), line_num+50)):
        print(f"{i+1}: {lines[i]}")
    print("\n" + "="*60 + "\n")
