import sys
sys.path.insert(0, 'd:\\twse')

# 手動模擬 get_work_directory
from pathlib import Path
import os

def get_work_directory():
    """獲取工作目錄 - 平台感知"""
    if os.name == 'nt':
        return Path('d:\\twse\\最終修正.py').parent.absolute()
    
    # Android 路徑
    android_paths = [
        Path('/sdcard/Download/stock_app'),
        Path('/storage/emulated/0/Download/stock_app')
    ]
    
    for path in android_paths:
        if path.exists() or path.parent.exists():
            path.mkdir(parents=True, exist_ok=True)
            return path
    
    return Path('d:\\twse\\最終修正.py').parent.absolute()

WORK_DIR = get_work_directory()
DB_FILE = WORK_DIR / 'taiwan_stock.db'

print(f"WORK_DIR: {WORK_DIR}")
print(f"DB_FILE: {DB_FILE}")
print(f"DB_FILE exists: {DB_FILE.exists()}")

# 列出所有 .db 檔案
import glob
dbs = glob.glob(str(WORK_DIR / '*.db'))
print(f"\nDB files in WORK_DIR:")
for db in dbs:
    print(f"  {db}")
