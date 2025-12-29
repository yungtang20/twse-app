"""
深度檢查程式碼違規項目
規則：
1. 不要有「按任意鍵返回/繼續」
2. A規則：僅普通股（排除 ETF/權證/DR/ETN/債券/指數/創新板/特別股/非數字代碼）
3. 數值精度：小數點後二位
4. 色彩規則：上升/漲＝紅色；下降/跌＝綠色
"""
import re

file_path = r'd:\twse\最終修正.py'

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

violations = []

for i, line in enumerate(lines, 1):
    # 1. 檢查「按 Enter 繼續」或類似暫停
    if re.search(r'(繼續|返回|任意鍵|Enter|readline\(\)|input\(.*(繼續|返回|Enter))', line, re.IGNORECASE):
        if 'def ' not in line and '__enter__' not in line and 'on_enter' not in line.lower():
            violations.append((i, '暫停提示', line.strip()[:100]))
    
    # 2. 檢查顏色規則 (漲＝紅色，跌＝綠色 是否正確)
    # 紅色代碼: \033[91m 或 \033[31m
    # 綠色代碼: \033[92m 或 \033[32m
    if '跌' in line and ('91m' in line or '31m' in line):  # 跌用紅色 = 錯誤
        violations.append((i, '色彩違規:跌應為綠', line.strip()[:80]))
    if '漲' in line and ('92m' in line or '32m' in line):  # 漲用綠色 = 錯誤
        violations.append((i, '色彩違規:漲應為紅', line.strip()[:80]))

print(f"掃描 {len(lines)} 行程式碼")
print(f"發現 {len(violations)} 個潛在違規項目:\n")

for line_num, vtype, content in violations[:30]:
    print(f"  L{line_num:5}: [{vtype}] {content}")

if len(violations) > 30:
    print(f"\n  ... 等共 {len(violations)} 個")
