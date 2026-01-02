import sys

try:
    with open('gaps_report.txt', 'r', encoding='utf-16') as f:
        content = f.read()
except:
    try:
        with open('gaps_report.txt', 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        print("Could not read file")
        sys.exit(1)

lines = content.splitlines()
output_lines = []
printing = False
for line in lines:
    if "=== Stocks Needing Backfill" in line:
        printing = True
    if printing:
        output_lines.append(line)

with open('gaps_list.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_lines))
