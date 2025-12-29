import sqlite3
from datetime import datetime

db_path = 'd:\\twse\\taiwan_stock.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print('='*60)
print('檢查 6904 伯鑫 資料完整性')
print('='*60)

# 1. 獲取 6904 資料
cur.execute("SELECT date_int FROM stock_history WHERE code = '6904' ORDER BY date_int")
dates_6904 = set(row[0] for row in cur.fetchall())
print(f'6904 筆數: {len(dates_6904)}')

if not dates_6904:
    print("6904 無資料")
    conn.close()
    exit()

min_date = min(dates_6904)
max_date = max(dates_6904)
print(f'資料範圍: {min_date} ~ {max_date}')

# 2. 獲取基準交易日 (使用 2330)
cur.execute(f"SELECT date_int FROM stock_history WHERE code = '2330' AND date_int BETWEEN {min_date} AND {max_date} ORDER BY date_int")
dates_ref = [row[0] for row in cur.fetchall()]
print(f'基準 (2330) 同期筆數: {len(dates_ref)}')

# 3. 比對缺漏
missing = []
for d in dates_ref:
    if d not in dates_6904:
        missing.append(d)

if missing:
    print(f'\n⚠ 發現 {len(missing)} 天缺漏:')
    # 為了版面整潔，只列出部分或範圍
    missing.sort()
    
    # 嘗試分組顯示連續缺漏
    ranges = []
    if missing:
        start = missing[0]
        prev = missing[0]
        for d in missing[1:]:
            # 簡單判斷是否連續 (這裡用 date_int 近似，跨月/跨年會不準確但足夠顯示)
            # 轉成 datetime 比較準
            d_dt = datetime.strptime(str(d), "%Y%m%d")
            prev_dt = datetime.strptime(str(prev), "%Y%m%d")
            if (d_dt - prev_dt).days > 5: # 假設超過 5 天算斷開 (包含週末)
                ranges.append((start, prev))
                start = d
            prev = d
        ranges.append((start, prev))
    
    for s, e in ranges:
        if s == e:
            print(f'  - {s}')
        else:
            print(f'  - {s} ~ {e}')
            
    print('\n建議：需要補件')
else:
    print('\n✓ 與 2330 交易日完全一致 (無缺漏)')
    print('說明：雖然筆數 448 < 450，但若與大盤一致，表示期間無交易或暫停交易，或者 2330 也休市？')
    print('      (但 2330 筆數若明顯較多，則 6904 確實有缺)')

conn.close()
