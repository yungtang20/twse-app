"""
刪除已下市股票的資料
"""
import sqlite3

db_path = 'd:\\twse\\taiwan_stock.db'

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 找出所有已下市的股票
cur.execute("""
    SELECT code, name, delist_date 
    FROM stock_meta 
    WHERE delist_date IS NOT NULL AND delist_date != ''
""")
delisted = cur.fetchall()

print("="*60)
print("刪除已下市股票資料")
print("="*60)

if not delisted:
    print("沒有找到已下市的股票")
else:
    print(f"找到 {len(delisted)} 支已下市股票:")
    for code, name, delist_date in delisted:
        print(f"  - {code} {name} (下市日: {delist_date})")
    
    print("\n開始刪除...")
    
    for code, name, delist_date in delisted:
        # 刪除 stock_history
        cur.execute("DELETE FROM stock_history WHERE code = ?", (code,))
        h_deleted = cur.rowcount
        
        # 刪除 institutional_investors
        cur.execute("DELETE FROM institutional_investors WHERE code = ?", (code,))
        i_deleted = cur.rowcount
        
        # 刪除 margin_data
        cur.execute("DELETE FROM margin_data WHERE code = ?", (code,))
        m_deleted = cur.rowcount
        
        # 刪除 stock_snapshot
        cur.execute("DELETE FROM stock_snapshot WHERE code = ?", (code,))
        s_deleted = cur.rowcount
        
        # 刪除 stock_meta
        cur.execute("DELETE FROM stock_meta WHERE code = ?", (code,))
        
        print(f"  ✓ {code} {name}: 刪除 history={h_deleted}, inst={i_deleted}, margin={m_deleted}, snapshot={s_deleted}")
    
    conn.commit()
    print("\n✓ 已刪除所有已下市股票資料")

conn.close()
print("\n完成！")
