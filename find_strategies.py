def find_strategies():
    with open('d:/twse/最終修正.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if 'def scan_' in line or 'extra_headers=' in line:
                print(f"{i+1}: {line.strip()}")

if __name__ == "__main__":
    find_strategies()
