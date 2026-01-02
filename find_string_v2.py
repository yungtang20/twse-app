import os

search_str = "Close 空值詳情"
root_dir = r"d:\twse"

print(f"Searching for '{search_str}' in {root_dir}...")

for root, dirs, files in os.walk(root_dir):
    if ".git" in root or ".venv" in root or "__pycache__" in root:
        continue
    for file in files:
        if file.endswith(".py"):
            path = os.path.join(root, file)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    for i, line in enumerate(lines):
                        if search_str in line:
                            print(f"Found in: {path} at line {i+1}")
            except:
                try:
                    with open(path, 'r', encoding='cp950') as f:
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            if search_str in line:
                                print(f"Found in: {path} at line {i+1}")
                except:
                    pass
