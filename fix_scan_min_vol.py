import re

file_path = r'd:\twse\backend\routers\scan.py'

def fix_scan_min_vol():
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        with open(file_path, 'rb') as f:
            content = f.read().decode('utf-8', errors='ignore')

    # 1. Update execute_scan_query signature and implementation
    # It currently takes (conditions, order_by, limit)
    # We want (conditions, order_by, limit, min_vol=500)
    # And use min_vol * 1000 in query
    
    # Replace the function signature
    content = content.replace(
        'def execute_scan_query(\n    conditions: str,\n    order_by: str = "s.close DESC",\n    limit: int = 30\n) -> List[Dict]:',
        'def execute_scan_query(\n    conditions: str,\n    order_by: str = "s.close DESC",\n    limit: int = 30,\n    min_vol: int = 500\n) -> List[Dict]:'
    )
    
    # Replace the query part
    # Old: AND s.volume >= 500
    # New: AND s.volume >= {min_vol * 1000}
    content = content.replace(
        'AND s.volume >= 500',
        'AND s.volume >= {min_vol * 1000}'
    )
    
    # 2. Update all route functions to accept min_vol and pass it
    # We need to find all @router.get functions and update their signature and call
    
    # List of functions to update
    funcs = [
        'scan_vp', 'scan_mfi', 'scan_ma', 'scan_kd_cross', 'scan_vsbc', 
        'scan_smart_money', 'scan_builtin', 'scan_2560', 'scan_five_stage',
        'scan_institutional_value', 'scan_six_dim', 'scan_patterns', 'scan_pv_divergence'
    ]
    
    for func in funcs:
        # Update signature: add min_vol param if not exists
        # scan_builtin already has it, others don't
        
        if func == 'scan_builtin':
            # It already has min_vol, just need to ensure it passes it correctly
            # Currently: results = execute_scan_query(conditions, order_by, limit)
            # Change to: results = execute_scan_query(conditions, order_by, limit, min_vol)
            # But wait, scan_builtin constructs query manually with min_vol?
            # Let's check scan_builtin implementation in previous file view
            # It has: AND s.volume >= {min_vol}
            # We should change it to rely on execute_scan_query's filter or update its manual filter to * 1000
            # If we rely on execute_scan_query, we should remove manual filter.
            pass
        else:
            # Add min_vol to signature
            # Pattern: async def func_name(..., limit: int = Query(30, ...)):
            # We want: async def func_name(..., limit: int = Query(30, ...), min_vol: int = Query(500, ...)):
            
            # Regex to find the limit param and append min_vol
            pattern = f'async def {func}\((.*?)(limit: int = Query\(30, ge=1, le=100\))\):'
            replacement = f'async def {func}(\\1\\2, min_vol: int = Query(500, ge=0, description="最小成交量")):'
            content = re.sub(pattern, replacement, content, flags=re.DOTALL)
            
            # Update the call to execute_scan_query
            # Pattern: results = execute_scan_query(conditions, order_by, limit)
            # We need to be careful because order_by might be a string literal or variable
            # But in most cases it's positional or keyword.
            # Let's just replace the call inside the function.
            # This is hard with regex because the call might be far away.
            # But we can replace 'execute_scan_query(conditions, order_by, limit)'
            # with 'execute_scan_query(conditions, order_by, limit, min_vol)'
            # However, order_by varies.
            
            # Alternative: Replace 'limit)' with 'limit, min_vol)' in execute_scan_query calls?
            # No, limit is passed as variable.
            pass

    # Since regex replacement for function calls is risky, let's do a more manual replacement for each function's call
    
    # scan_vp
    content = content.replace(
        'results = execute_scan_query(conditions, order_by, limit)',
        'results = execute_scan_query(conditions, order_by, limit, min_vol)'
    )
    
    # scan_vsbc uses "s.volume DESC" as order_by
    content = content.replace(
        'results = execute_scan_query(conditions, "s.volume DESC", limit)',
        'results = execute_scan_query(conditions, "s.volume DESC", limit, min_vol)'
    )
    
    # scan_smart_money uses "change_pct DESC"
    content = content.replace(
        'results = execute_scan_query(conditions, "change_pct DESC", limit)',
        'results = execute_scan_query(conditions, "change_pct DESC", limit, min_vol)'
    )
    
    # scan_institutional_value uses "s.rsi12 ASC"
    content = content.replace(
        'results = execute_scan_query(conditions, "s.rsi12 ASC", limit)',
        'results = execute_scan_query(conditions, "s.rsi12 ASC", limit, min_vol)'
    )
    
    # scan_pv_divergence uses "s.volume DESC"
    # (Already covered by vsbc replacement if string matches exactly, but let's be safe)
    
    # scan_builtin
    # It constructs query manually: AND s.volume >= {min_vol}
    # We should change it to use min_vol * 1000
    content = content.replace(
        'AND s.volume >= {min_vol}',
        'AND s.volume >= {min_vol * 1000}'
    )
    # And it calls execute_scan_query(conditions, order_by, limit)
    # We should pass min_vol=0 to avoid double filtering (since we added it to conditions manually)
    # OR better: remove manual filter and pass min_vol.
    # Let's just pass min_vol to execute_scan_query and remove manual filter from builtin conditions?
    # Actually, execute_scan_query now enforces AND s.volume >= {min_vol * 1000}
    # If scan_builtin also adds it, it's fine (redundant but safe).
    # But we need to pass min_vol to execute_scan_query.
    
    # scan_builtin call
    # It uses a variable order_by
    # So the generic replacement 'results = execute_scan_query(conditions, order_by, limit)' works.
    
    # scan_patterns
    # uses "change_pct DESC"
    # Covered by smart_money replacement? No, smart_money replacement was specific string.
    # We need to replace all instances of execute_scan_query calls that don't have min_vol yet.
    
    # Let's use a regex for the call
    content = re.sub(
        r'execute_scan_query\(([^)]+), limit\)',
        r'execute_scan_query(\1, limit, min_vol)',
        content
    )
    
    # Fix scan_builtin signature (it already had min_vol, so our regex above didn't match it, which is good)
    # But we need to make sure it's passed.
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("Successfully updated scan.py with min_vol support")

if __name__ == "__main__":
    fix_scan_min_vol()
