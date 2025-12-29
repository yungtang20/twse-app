"""
把加權指數 (0000) 從 yfinance 抓下來存入資料庫
"""
import yfinance as yf
import sqlite3

# 抓取加權指數最大歷史資料
print("正在從 yfinance 抓取加權指數...")
ticker = yf.Ticker("^TWII")
hist = ticker.history(period="max")
print(f"從 yfinance 抓到 {len(hist)} 筆加權指數資料")
print(f"日期範圍: {hist.index[0].strftime('%Y-%m-%d')} ~ {hist.index[-1].strftime('%Y-%m-%d')}")

# 存入資料庫
conn = sqlite3.connect("taiwan_stock.db")
cur = conn.cursor()

# 確認表格結構
cur.execute("PRAGMA table_info(stock_history)")
columns = [col[1] for col in cur.fetchall()]
print(f"stock_history 欄位: {columns}")

# 插入資料
inserted = 0
for date, row in hist.iterrows():
    date_int = int(date.strftime("%Y%m%d"))
    try:
        cur.execute("""
            INSERT OR REPLACE INTO stock_history (code, date_int, open, high, low, close, volume, amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ("0000", date_int, round(float(row["Open"]), 2), round(float(row["High"]), 2), 
              round(float(row["Low"]), 2), round(float(row["Close"]), 2), int(row["Volume"]), 0))
        inserted += 1
    except Exception as e:
        print(f"Error: {e}")
        break

conn.commit()
conn.close()
print(f"成功插入 {inserted} 筆加權指數資料到 taiwan_stock.db")
