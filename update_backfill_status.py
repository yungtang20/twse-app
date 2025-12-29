import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "taiwan_stock.db"
MD_PATH = Path(r"C:\Users\詠棠\.gemini\antigravity\brain\dee58762-0a54-40bb-b366-874e8ef18780\backfill_status.md")

def update_status():
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    
    # 1. 取得所有目標股票 (A規則)
    cur.execute("SELECT DISTINCT code, name FROM stock_meta WHERE code GLOB '[0-9][0-9][0-9][0-9]' ORDER BY code")
    all_stocks = []
    for code, name in cur.fetchall():
        if not code.startswith('00') and not code.startswith('91') and "特" not in name:
            all_stocks.append((code, name))
            
    # 2. 取得已完成股票
    cur.execute("SELECT code FROM stock_shareholding_all GROUP BY code HAVING count(DISTINCT date_int) > 5")
    completed_stocks = set(row[0] for row in cur.fetchall())
    
    conn.close()
    
    # 3. 產生 Markdown 內容
    total = len(all_stocks)
    done = len(completed_stocks)
    remaining = total - done
    
    content = [
        "# 集保資料回補狀態追蹤 (SDD)",
        "",
        "## 說明",
        "此文件用於追蹤集保資料的回補進度。",
        "- `[ ]` : 等待回補",
        "- `[x]` : 已完成回補 (資料庫中有 > 5 筆歷史資料)",
        "",
        "## 統計",
        f"- **總目標**: {total} 檔",
        f"- **已完成**: {done} 檔",
        f"- **待處理**: {remaining} 檔",
        f"- **進度**: {done/total*100:.1f}%",
        "",
        "## 待處理股票清單 (Missing List)",
        "以下列出尚未回補的股票，方便確認目標：",
        "",
    ]
    
    # 列出缺資料的股票
    missing_list = []
    for code, name in all_stocks:
        if code not in completed_stocks:
            missing_list.append(f"- [ ] {code} {name}")
            
    if missing_list:
        content.extend(missing_list)
    else:
        content.append("🎉 所有股票皆已完成回補！")
        
    content.append("")
    content.append("## 所有股票狀態總表")
    content.append("| 代碼 | 名稱 | 狀態 |")
    content.append("|---|---|---|")
    
    # 完整表格
    for code, name in all_stocks:
        status = "✅ 已完成" if code in completed_stocks else "⬜ 待處理"
        content.append(f"| {code} | {name} | {status} |")
        
    # 寫入檔案
    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(content))
        
    print(f"已更新 backfill_status.md (待處理: {remaining} 檔)")

if __name__ == "__main__":
    update_status()
