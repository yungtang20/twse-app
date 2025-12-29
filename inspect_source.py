import sys
import inspect

# Add path
sys.path.append('d:/twse')

# Import the module
try:
    import 最終修正 as app
except ImportError:
    print("Failed to import 最終修正")
    sys.exit(1)

# Print source of _handle_stock_query
print("\n--- Source of _handle_stock_query ---")
try:
    src = inspect.getsource(app._handle_stock_query)
    print(src)
except Exception as e:
    print(f"Failed to get source: {e}")
