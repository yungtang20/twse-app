import os

file_path = r'd:\twse\backend\routers\scan.py'

new_scan_vp_code = '''@router.get("/scan/vp", response_model=ScanResponse)
async def scan_vp(
    direction: str = Query("support", description="support=支撐區, resistance=壓力區"),
    tolerance: float = Query(0.02, ge=0, le=0.1, description="容忍度 (%)"),
    limit: int = Query(30, ge=1, le=100)
):
    """VP 掃描"""
    try:
        # 容忍度
        tol = tolerance
        
        if direction == "support":
            # 接近支撐 (VP Lower)
            conditions = f"""
                AND s.vp_lower IS NOT NULL
                AND s.close >= s.vp_lower * {1 - tol}
                AND s.close <= s.vp_lower * {1 + tol}
            """
            order_by = "ABS(s.close - s.vp_lower) ASC"
        else:
            # 接近壓力 (VP Upper)
            conditions = f"""
                AND s.vp_upper IS NOT NULL
                AND s.close >= s.vp_upper * {1 - tol}
                AND s.close <= s.vp_upper * {1 + tol}
            """
            order_by = "ABS(s.close - s.vp_upper) ASC"
            
        results = execute_scan_query(conditions, order_by, limit)
        
        return {
            "success": True, 
            "data": {
                "scan_type": "vp", 
                "direction": direction,
                "results": results, 
                "count": len(results)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
except:
    with open(file_path, 'rb') as f:
        content = f.read().decode('utf-8', errors='ignore')
        lines = content.splitlines(keepends=True)

start_idx = -1
end_idx = -1

for i, line in enumerate(lines):
    if '@router.get("/scan/vp"' in line:
        start_idx = i
        break

if start_idx != -1:
    # Find the next router or end of file
    for i in range(start_idx + 1, len(lines)):
        if '@router.get' in line or line.strip().startswith('@router'): # Check next decorator
             # But wait, we need to be careful not to match the current one if I started search from start_idx
             pass
        
    # Actually, let's just find the next @router definition
    for i in range(start_idx + 1, len(lines)):
        if lines[i].strip().startswith('@router'):
            end_idx = i
            break
    
    if end_idx == -1:
        end_idx = len(lines) # End of file? No, scan_mfi follows scan_vp usually
        
    # Double check if scan_mfi is after scan_vp
    # In the file content I saw earlier:
    # @router.get("/scan/vp", ...)
    # ...
    # @router.get("/scan/mfi", ...)
    
    # So end_idx should be the line of scan_mfi
    
    print(f"Replacing lines {start_idx} to {end_idx}")
    
    new_lines = lines[:start_idx] + [new_scan_vp_code + "\n"] + lines[end_idx:]
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("Successfully updated scan_vp")

else:
    print("Could not find scan_vp definition")
