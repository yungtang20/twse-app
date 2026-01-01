import sys
import os

# Add current directory to path to import 最終修正
sys.path.append(os.getcwd())

from 最終修正 import normalize_stock_name, is_normal_stock

def test_truncation():
    print("Testing normalize_stock_name...")
    
    test_cases = [
        ("台積電股份有限公司", "台積電"),
        ("聯發科技股份有限公司", "聯發科技"),
        ("某某某某某股份有限公司", "某某某某"), # Should be truncated
        ("短名", "短名"),
        ("1234567890", "1234"), # Should be truncated
        (None, None),
        ("", "")
    ]
    
    for input_name, expected in test_cases:
        result = normalize_stock_name(input_name)
        status = "PASS" if result == expected else "FAIL"
        print(f"Input: '{input_name}' -> Output: '{result}' | Expected: '{expected}' [{status}]")

def test_a_rules_logic():
    print("\nTesting A Rules Logic (Simulation)...")
    
    # Simulate the logic in step2_download_lists
    # 1. Raw name check (is_normal_stock)
    # 2. Normalization
    
    cases = [
        ("2330", "台積電股份有限公司", True, "台積電"), # Normal stock
        ("9105", "泰金寶-DR", False, None), # DR stock (should fail is_normal_stock)
        ("0050", "元大台灣50", False, None), # ETF (should fail is_normal_stock)
    ]
    
    for code, raw_name, expected_pass, expected_final_name in cases:
        print(f"Testing Code: {code}, Raw Name: {raw_name}")
        
        # Step 1: Check A Rule with raw name
        is_normal = is_normal_stock(code, raw_name)
        print(f"  is_normal_stock('{code}', '{raw_name}') -> {is_normal}")
        
        if is_normal != expected_pass:
            print(f"  FAIL: Expected is_normal to be {expected_pass}")
            continue
            
        if is_normal:
            # Step 2: Normalize
            final_name = normalize_stock_name(raw_name)
            print(f"  Final Name: '{final_name}'")
            if final_name != expected_final_name:
                 print(f"  FAIL: Expected final name '{expected_final_name}', got '{final_name}'")
            else:
                print("  PASS")
        else:
            print("  PASS (Correctly filtered out)")

if __name__ == "__main__":
    test_truncation()
    test_a_rules_logic()
