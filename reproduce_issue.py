import sys
import os
import io

# Capture stdout
class CaptureOutput(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = io.StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

# Add path
sys.path.append('d:/twse')

# Import the module
try:
    import 最終修正 as app
except ImportError:
    print("Failed to import 最終修正")
    sys.exit(1)

# Patch print_flush to just print (so we can capture it)
def mock_print_flush(*args, **kwargs):
    print(*args, **kwargs)

app.print_flush = mock_print_flush

# Test 3013
code = '3013'
print(f"Testing {code}...")

# 1. Check calculate_stock_history_indicators
print("\n--- calculate_stock_history_indicators ---")
indicators_list = app.calculate_stock_history_indicators(code, display_days=10)

if not indicators_list:
    print("indicators_list is None or empty")
else:
    print(f"Got {len(indicators_list)} items")
    first = indicators_list[0]
    print(f"First item date: {first.get('date')}")
    print(f"First item MA20: {first.get('MA20')}")
    
    # Check if 12-24 is present
    found_1224 = False
    for ind in indicators_list:
        if ind.get('date') == '2025-12-24':
            found_1224 = True
            print("Found 2025-12-24 data!")
            print(f"MA20: {ind.get('MA20')}")
            break
    if not found_1224:
        print("2025-12-24 data NOT found in list")

# 2. Check format_scan_result
print("\n--- format_scan_result ---")
if indicators_list:
    # Format the first item (should be 12-24 if present, or 12-23)
    first_item = indicators_list[0]
    formatted = app.format_scan_result(code, '晟銘電', first_item, show_date=True)
    print("Formatted output:")
    print(formatted)
    
    # Check if lines are missing
    lines = formatted.split('\n')
    print(f"Line count: {len(lines)}")
    for i, line in enumerate(lines):
        print(f"Line {i+1}: {line}")

# 3. Check format_scan_result_list
print("\n--- format_scan_result_list ---")
formatted_list = app.format_scan_result_list(code, '晟銘電', indicators_list)
print("Formatted list start:")
print(formatted_list[:500]) # Print first 500 chars
