import sqlite3

conn = sqlite3.connect('taiwan_stock.db')
c = conn.cursor()

# 有 holding 資料的股票
c.execute("""
    SELECT code, foreign_holding_shares, foreign_holding_pct 
    FROM institutional_investors 
    WHERE foreign_holding_shares IS NOT NULL AND foreign_holding_shares > 0
    ORDER BY foreign_holding_shares DESC
    LIMIT 10
""")
print("有 foreign_holding 資料的股票 (TOP 10):")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]:,} 股, {row[2]}%")

# 排行榜股票是否有資料
print("\n" + "="*50)
print("排行榜前10名股票的 holding 資料:")
ranking_codes = ['3481', '2002', '2610', '2892', '1101', '2618', '2891', '2317', '2885', '2882']
for code in ranking_codes:
    c.execute("""
        SELECT foreign_holding_shares, foreign_holding_pct 
        FROM institutional_investors 
        WHERE code = ? AND foreign_holding_shares IS NOT NULL
        ORDER BY date_int DESC LIMIT 1
    """, (code,))
    result = c.fetchone()
    if result and result[0]:
        print(f"  {code}: {result[0]:,} 股, {result[1]}%")
    else:
        print(f"  {code}: 無資料")

conn.close()
