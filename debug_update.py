import sys
import os
import logging

# Configure logging to stdout
logging.basicConfig(level=logging.DEBUG)

# Add path
sys.path.append(os.getcwd())

print("Importing admin router...")
try:
    from backend.routers.admin import run_daily_update
    print("Import successful.")
    
    print("Running daily update...")
    # Mock task_id
    run_daily_update("debug_task_001")
    print("Daily update finished.")
except Exception as e:
    print("Caught exception:")
    import traceback
    traceback.print_exc()
