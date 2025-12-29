import sqlite3

def check_missing():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    
    # 取得所有 4 碼股票
    cur.execute("SELECT DISTINCT code FROM stock_meta WHERE code GLOB '[0-9][0-9][0-9][0-9]'")
    all_stocks = set(row[0] for row in cur.fetchall())
    
    # 過濾 A 規則 (簡單過濾)
    # 排除 00xx, 91xx
    target_stocks = {s for s in all_stocks if not s.startswith('00') and not s.startswith('91')}
    
    # 取得有歷史資料的股票 (日期數 > 5)
    cur.execute("SELECT code FROM stock_shareholding_all GROUP BY code HAVING count(DISTINCT date_int) > 5")
    has_history = set(row[0] for row in cur.fetchall())
    
    missing = sorted(list(target_stocks - has_history))
    
    # 取得缺資料股票的名稱
    if missing:
        placeholders = ','.join('?' for _ in missing)
        cur.execute(f"SELECT code, name FROM stock_meta WHERE code IN ({placeholders})", missing)
        missing_info = {row[0]: row[1] for row in cur.fetchall()}
        
        print(f"目標股票數 (A規則): {len(target_stocks)}")
        print(f"有歷史資料股票數: {len(has_history)}")
        print(f"缺歷史資料股票數: {len(missing)}")
        print("-" * 30)
        print(f"缺資料股票範例 (前 50 檔):")
        for code in missing[:50]:
            name = missing_info.get(code, "Unknown")
            print(f"{code} {name}")
    else:
        print("所有 A 規則股票皆已回補完成！")
    
    conn.close()

if __name__ == "__main__":
    check_missing()
