import os

file_path = r'd:\twse\backend\routers\scan.py'

new_scan_vp = '''    """VP 掃描"""
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
        raise HTTPException(status_code=500, detail=str(e))'''

target_str = '''    """VP 掃描 (暫停使用 - 缺資料)"""
    return {"success": True, "data": {"scan_type": "vp", "results": [], "count": 0, "message": "資料庫缺少 VP 數據"}}'''

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
except UnicodeDecodeError:
    # Try reading with cp950 (Big5) or other common encodings if utf-8 fails
    try:
        with open(file_path, 'r', encoding='cp950') as f:
            content = f.read()
    except:
         # Fallback to binary and decode with ignore
        with open(file_path, 'rb') as f:
            content = f.read().decode('utf-8', errors='ignore')

if target_str in content:
    new_content = content.replace(target_str, new_scan_vp)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully updated scan_vp")
else:
    print("Target string not found")
    # Print a snippet to help debug
    start = content.find("def scan_vp")
    if start != -1:
        print("Found scan_vp at:", start)
        print(content[start:start+300])
