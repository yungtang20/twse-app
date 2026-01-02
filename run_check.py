import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

from 最終修正 import check_db_nulls

# Redirect stdout to a file
with open('check_report.txt', 'w', encoding='utf-8') as f:
    original_stdout = sys.stdout
    sys.stdout = f
    try:
        check_db_nulls()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        sys.stdout = original_stdout
