import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Mock Config
class Config:
    MAX_WORKERS = 4

# Mock print_flush
def print_flush(msg):
    print(msg)

# Mock logger
class Logger:
    def error(self, msg):
        print(f"ERROR: {msg}")
logger = Logger()

# Copy run_parallel_tasks from 最終修正.py (simplified for test)
def run_parallel_tasks(tasks, max_workers=None, show_progress=True, silent_execution=False):
    if max_workers is None:
        max_workers = Config.MAX_WORKERS
    
    results = {}
    task_labels = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_info = {}
        for task in tasks:
            func = task[0]
            args = task[1] if len(task) > 1 else ()
            kwargs = task[2] if len(task) > 2 else {}
            name = task[3] if len(task) > 3 else func.__name__
            label = task[4] if len(task) > 4 else ""
            
            task_labels[name] = label
            future = executor.submit(func, *args, **kwargs)
            future_to_info[future] = name
        
        for future in as_completed(future_to_info):
            name = future_to_info[future]
            try:
                result = future.result()
                results[name] = result
                print(f"Task {name} completed with result: {result}")
            except Exception as e:
                print(f"Task {name} failed: {e}")
                results[name] = None
    
    return results

# Test functions
def task_a(x):
    time.sleep(0.1)
    return x * 2

def task_b(y, silent_header=False):
    time.sleep(0.1)
    return f"B:{y}, Silent:{silent_header}"

def main():
    print("Verifying run_parallel_tasks...")
    
    tasks = [
        (task_a, (10,), {}, "Task A", "1"),
        (task_b, (20,), {'silent_header': True}, "Task B", "2"),
    ]
    
    results = run_parallel_tasks(tasks, max_workers=2)
    
    assert results["Task A"] == 20
    assert results["Task B"] == "B:20, Silent:True"
    
    print("\nVerification Successful!")

if __name__ == "__main__":
    main()
