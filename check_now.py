import sqlite3

def get_stats():
    conn = sqlite3.connect('taiwan_stock.db')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_meta")
    meta = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT code) FROM stock_history")
    h = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT h.code) FROM stock_history h LEFT JOIN stock_meta m ON h.code=m.code WHERE m.code IS NULL")
    o = cur.fetchone()[0]
    # 找幾個孤兒代碼樣本
    cur.execute("SELECT DISTINCT h.code FROM stock_history h LEFT JOIN stock_meta m ON h.code=m.code WHERE m.code IS NULL LIMIT 5")
    samples = [r[0] for r in cur.fetchall()]
    conn.close()
    return meta, h, o, samples

m, h, o, s = get_stats()
print(f"meta: {m}, history: {h}, orphans: {o}")
print(f"孤兒樣本: {s}")
