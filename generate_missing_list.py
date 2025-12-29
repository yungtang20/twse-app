import sqlite3

def generate_list():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    # 1. 查詢回補期間 (以 2330 為例)
    cur.execute("SELECT MIN(date_int), MAX(date_int) FROM stock_shareholding_all WHERE code='2330'")
    date_range = cur.fetchone()
    print(f"回補期間: {date_range[0]} ~ {date_range[1]}")
    
    # 2. 找出缺資料股票
    # 取得所有 4 碼股票
    cur.execute("SELECT DISTINCT code FROM stock_meta WHERE code GLOB '[0-9][0-9][0-9][0-9]'")
    all_stocks = set(row[0] for row in cur.fetchall())
    
    # 過濾 A 規則 (排除 00xx, 91xx)
    target_stocks = {s for s in all_stocks if not s.startswith('00') and not s.startswith('91')}
    
    # 取得有歷史資料的股票 (日期數 > 5)
    cur.execute("SELECT code FROM stock_shareholding_all GROUP BY code HAVING count(DISTINCT date_int) > 5")
    has_history = set(row[0] for row in cur.fetchall())
    
    missing = sorted(list(target_stocks - has_history))
    
    print(f"缺資料股票數: {len(missing)}")
    
    # 3. 寫入檔案
    with open('missing_stocks.txt', 'w') as f:
        f.write(','.join(missing))
        
    print("已產生 missing_stocks.txt")
    
    conn.close()

if __name__ == "__main__":
    generate_list()
