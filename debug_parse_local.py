import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

def main():
    try:
        with open("debug_failed_1101.html", "r", encoding="utf-8") as f:
            html = f.read()
            
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {'id': 'tblDetail'})
        
        if not table:
            print("Table not found")
            return
            
        print("Table found. Parsing with pandas...")
        dfs = pd.read_html(StringIO(str(table)))
        df = dfs[0]
        
        print("Original Columns:")
        print(df.columns)
        
        if isinstance(df.columns, pd.MultiIndex):
            print("\nMultiIndex detected. Flattening...")
            df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
            print("Flattened Columns:")
            print(df.columns)
            
        print("\nFirst row:")
        print(df.iloc[0])
        
        # Test date column finding
        date_col = next((c for c in df.columns if '日期' in c), None)
        print(f"\nDate column found: {date_col}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
