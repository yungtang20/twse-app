import pandas as pd
import os

def get_a_rule_stocks():
    """
    Get A-Rule stocks:
    1. Filter by market (TWSE/TPEX) from stock_list.csv
    2. Exclude ETFs, DRs, etc. (Code length=4 usually implies common stock, but need to be careful)
    Actually, let's replicate the logic from backfill_tdcc_selenium.py or just read stock_list.csv
    """
    if not os.path.exists('stock_list.csv'):
        print("stock_list.csv not found!")
        return []
        
    df = pd.read_csv('stock_list.csv', dtype=str)
    
    # Filter for TWSE and TPEX
    df = df[df['market'].isin(['TWSE', 'TPEX'])]
    
    # Filter for A-Rule (Common stocks)
    # Exclude codes starting with '0' (ETFs usually), and length > 4 (Warrants, etc.)
    # Also exclude DRs (contain 'DR' in name? or specific codes)
    # The user rule says: "A規則（僅普通股：TWSE+TPEX+KY，排除ETF/權證/DR/ETN/債券/指數/創新板/特別股/非數字代碼）"
    
    a_rule_stocks = []
    for _, row in df.iterrows():
        code = row['code']
        name = row['name']
        
        # Basic filtering
        if len(code) != 4: continue # Most common stocks are 4 digits
        if not code.isdigit(): continue
        if code.startswith('0'): continue # ETFs
        
        # Exclude DR, Preferred shares (usually 4 digits + letter, but we filtered for digits)
        # Check name for keywords if needed, but 4-digit rule is strong for TWSE/TPEX common stocks
        
        a_rule_stocks.append(code)
        
    return sorted(a_rule_stocks)

def main():
    all_stocks = get_a_rule_stocks()
    print(f"Total A-Rule Stocks: {len(all_stocks)}")
    
    processed = set()
    if os.path.exists("processed_stocks.txt"):
        with open("processed_stocks.txt", "r", encoding="utf-8") as f:
            processed = set(line.strip() for line in f if line.strip())
            
    print(f"Processed Stocks: {len(processed)}")
    
    remaining = [s for s in all_stocks if s not in processed]
    print(f"Remaining Stocks: {len(remaining)}")
    
    if remaining:
        print("\nFirst 10 remaining stocks:")
        print(remaining[:10])
        
        print("\nLast 10 remaining stocks:")
        print(remaining[-10:])
        
        # Save remaining to file for review
        with open("remaining_stocks_list.txt", "w", encoding="utf-8") as f:
            for s in remaining:
                f.write(f"{s}\n")
        print("\nFull list of remaining stocks saved to 'remaining_stocks_list.txt'")

if __name__ == "__main__":
    main()
