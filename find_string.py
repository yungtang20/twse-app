
with open(r'd:\twse\最終修正.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if 'def step7_calc_indicators' in line:
        print(f"Line {i+1}: {line.strip()}")
