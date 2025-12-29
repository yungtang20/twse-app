import re

file_path = r'd:\twse\backend\routers\scan.py'

def fix_scan_price():
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        with open(file_path, 'rb') as f:
            content = f.read().decode('utf-8', errors='ignore')

    # 1. Update execute_scan_query signature and implementation
    # Current: def execute_scan_query(..., min_vol: int = 500) -> List[Dict]:
    # New: def execute_scan_query(..., min_vol: int = 500, max_price: float = None) -> List[Dict]:
    
    content = content.replace(
        'min_vol: int = 500\n) -> List[Dict]:',
        'min_vol: int = 500,\n    max_price: Optional[float] = None\n) -> List[Dict]:'
    )
    
    # Update query in execute_scan_query
    # Add logic for max_price
    # We need to insert it before {conditions}
    # Let's verify where {conditions} is.
    # It is:
    # AND s.volume >= {min_vol * 1000}
    # {conditions}
    
    # We will replace 'AND s.volume >= {min_vol * 1000}' 
    # with 'AND s.volume >= {min_vol * 1000}\n        {f"AND s.close <= {max_price}" if max_price else ""}'
    
    content = content.replace(
        'AND s.volume >= {min_vol * 1000}',
        'AND s.volume >= {min_vol * 1000}\n        {f"AND s.close <= {max_price}" if max_price else ""}'
    )
    
    # 2. Update all route functions to accept max_price and pass it
    # Pattern: min_vol: int = Query(500, ge=0, description="最小成交量")
    # We want to append: , max_price: float = Query(None, gt=0, description="最高股價")
    
    # We can replace the min_vol line with min_vol + max_price
    # Regex approach
    
    # Replace signature
    content = re.sub(
        r'(min_vol: int = Query\(500, ge=0, description="最小成交量"\))',
        r'\1,\n    max_price: float = Query(None, gt=0, description="最高股價")',
        content
    )
    
    # Replace call to execute_scan_query
    # Pattern: execute_scan_query(conditions, order_by, limit, min_vol)
    # New: execute_scan_query(conditions, order_by, limit, min_vol, max_price)
    
    # Note: Some calls might use keyword args or different order if I modified them manually before?
    # No, I used a script to append min_vol.
    # Let's assume they are `execute_scan_query(conditions, order_by, limit, min_vol)`
    # Or `execute_scan_query(conditions, "...", limit, min_vol)`
    
    # We can use a regex to append max_price to the call
    content = re.sub(
        r'(execute_scan_query\([^)]+min_vol)',
        r'\1, max_price',
        content
    )
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("Successfully updated scan.py with max_price support")

if __name__ == "__main__":
    fix_scan_price()
