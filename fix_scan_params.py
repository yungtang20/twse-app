import re

file_path = r'd:\twse\backend\routers\scan.py'

def fix_scan_params():
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except:
        with open(file_path, 'rb') as f:
            content = f.read().decode('utf-8', errors='ignore')
            lines = content.splitlines(keepends=True)

    new_lines = []
    # We look for async def scan_...
    # And check if it spans multiple lines.
    # Usually the limit param is on a line by itself or at the end.
    
    # Strategy:
    # Iterate lines. If we find `limit: int = Query(30, ge=1, le=100)` AND the function doesn't have min_vol yet.
    # But we need to know which function we are in.
    
    # Simpler approach: Read the whole content, find function blocks.
    # But regex on whole content is what failed last time.
    
    # Let's iterate line by line.
    
    in_function = False
    current_func_name = ""
    has_min_vol = False
    
    for i, line in enumerate(lines):
        # Check for function start
        if line.strip().startswith("async def scan_"):
            in_function = True
            current_func_name = line.split("def ")[1].split("(")[0]
            has_min_vol = "min_vol" in line
        
        # If inside function signature (before ): )
        if in_function:
            if "min_vol" in line:
                has_min_vol = True
            
            if "limit: int = Query(30, ge=1, le=100)" in line:
                # Found the limit param.
                # If we haven't seen min_vol yet, we should append it here.
                # But we need to be careful about the closing parenthesis.
                
                # Check if this function already has min_vol (e.g. scan_builtin)
                # We might have seen it in previous lines of this function def.
                
                # Wait, scan_builtin has min_vol BEFORE limit.
                # So has_min_vol should be True by now if it exists.
                
                if not has_min_vol:
                    # Replace line
                    # Case 1: limit is followed by ):
                    # Case 2: limit is followed by ,
                    
                    # In our file, it seems limit is usually the last one:
                    # limit: int = Query(30, ge=1, le=100)
                    # ):
                    
                    # Or: limit: int = Query(30, ge=1, le=100)):
                    
                    if "limit: int = Query(30, ge=1, le=100)" in line:
                        # We replace it with limit + min_vol
                        replacement = '    limit: int = Query(30, ge=1, le=100),\n    min_vol: int = Query(500, ge=0, description="最小成交量")'
                        
                        # Preserve indentation
                        indent = line[:line.find("limit")]
                        line = line.replace(
                            "limit: int = Query(30, ge=1, le=100)",
                            f"limit: int = Query(30, ge=1, le=100),\n{indent}min_vol: int = Query(500, ge=0, description=\"最小成交量\")"
                        )
                        has_min_vol = True # Mark as added
            
            if "):" in line:
                in_function = False
        
        new_lines.append(line)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("Successfully added min_vol params")

if __name__ == "__main__":
    fix_scan_params()
