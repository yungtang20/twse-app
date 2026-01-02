import sys
import os
sys.path.append(os.getcwd())

# Mock print_flush to see output
def print_flush(msg, end="\n"):
    print(msg, end=end)

import builtins
builtins.print_flush = print_flush

from 最終修正 import step4_check_data_gaps, db_manager

print("=== Testing step4_check_data_gaps ===")
step4_check_data_gaps()
