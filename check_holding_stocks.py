import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# 檢查 institutional_investors 表中有 holding 資料的股票
c.execute("""
    SELECT code, foreign_holding_shares, foreign_holding_pct, trust_holding_shares, trust_holding_pct
    FROM institutional_investors 
    WHERE date_int = (SELECT MAX(date_int) FROM institutional_investors WHERE foreign_holding_shares > 0)
    AND (foreign_holding_shares > 0 OR trust_holding_shares > 0)
    ORDER BY foreign_holding_shares DESC
    LIMIT 10
""")
print("有 holding 資料的股票 (TOP 10):")
for row in c.fetchall():
    print(f"  {row}")

# 檢查這些排行榜股票是否有資料
codes = ['3481', '2002', '2610', '2892', '1101']
c.execute(f"""
    SELECT code, date_int, foreign_holding_shares, trust_holding_shares
    FROM institutional_investors 
    WHERE code IN ({','.join(['?']*len(codes))})
    ORDER BY code, date_int DESC
""", codes)
print("\n排行榜股票在 institutional_investors 中的資料:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()
