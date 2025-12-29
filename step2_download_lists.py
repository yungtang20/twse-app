def step2_download_lists(silent_header=False):
    """步驟2: 下載清單 (TPEx/TWSE/處置/新上市/終止)"""
    if not silent_header:
        print_flush("\n[Step 2] 下載股票清單 (含處置/新上市/終止)...")
    
    # 1. 基本清單 (TWSE/TPEx) - 沿用舊邏輯
    # 這裡我們呼叫舊的 _legacy_fetch_stock_list (稍後會將其內容移入)
    # 但為了保持代碼整潔，我們先直接在這裡實作整合邏輯
    
    conn = db_manager.get_connection()
    cur = conn.cursor()
    
    # 確保表格存在
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_meta (
            code TEXT PRIMARY KEY,
            name TEXT,
            market TEXT,
            industry TEXT,
            list_date TEXT,
            delist_date TEXT,
            is_normal INTEGER DEFAULT 1,
            status TEXT DEFAULT 'Normal' -- Normal, Punished, Suspended
        )
    """)
    # 檢查是否有 status 欄位，若無則新增
    try:
        cur.execute("SELECT status FROM stock_meta LIMIT 1")
    except:
        try:
            cur.execute("ALTER TABLE stock_meta ADD COLUMN status TEXT DEFAULT 'Normal'")
        except: pass
    conn.commit()
    
    total_added = 0
    
    # --- A. TWSE Stock List ---
    try:
        print_flush("  [TWSE] 基本資料 (上市日期)... ", end="")
        url = get_api_url('twse', 'stock_list')
        res = requests.get(url, timeout=15, verify=False)
        if res.status_code == 200:
            data = res.json()
            count = 0
            for item in data:
                code = item.get('公司代號', '').strip()
                name = item.get('公司名稱', '').strip()
                l_date = item.get('上市日期', '').strip() # YYYYMMDD
                ind = item.get('產業別', '').strip()
                
                if not is_normal_stock(code, name): continue
                
                # 轉換日期
                if len(l_date) == 8:
                    l_date = f"{l_date[:4]}-{l_date[4:6]}-{l_date[6:]}"
                
                cur.execute("""
                    INSERT INTO stock_meta (code, name, market, industry, list_date)
                    VALUES (?, ?, 'TWSE', ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                        name=excluded.name,
                        market=excluded.market,
                        industry=excluded.industry,
                        list_date=excluded.list_date
                """, (code, name, ind, l_date))
                count += 1
            print_flush(f"✓ (取得 {count} 檔)")
            total_added += count
    except Exception as e:
        print_flush(f"❌ 失敗: {e}")

    # --- B. TPEx Stock List ---
    try:
        print_flush("  [TPEx] 基本資料 (上市日期)... ", end="")
        url = get_api_url('tpex', 'stock_list')
        res = requests.get(url, timeout=15, verify=False)
        if res.status_code == 200:
            data = res.json()
            count = 0
            for item in data:
                code = item.get('SecuritiesCompanyCode', '').strip()
                name = item.get('CompanyName', '').strip()
                l_date = item.get('DateOfListing', '').strip() # YYYYMMDD
                ind = item.get('Industry', '').strip()
                
                if not is_normal_stock(code, name): continue
                
                if len(l_date) == 8:
                    l_date = f"{l_date[:4]}-{l_date[4:6]}-{l_date[6:]}"
                
                cur.execute("""
                    INSERT INTO stock_meta (code, name, market, industry, list_date)
                    VALUES (?, ?, 'TPEx', ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                        name=excluded.name,
                        market=excluded.market,
                        industry=excluded.industry,
                        list_date=excluded.list_date
                """, (code, name, ind, l_date))
                count += 1
            print_flush(f"✓ (取得 {count} 檔)")
            total_added += count
    except Exception as e:
        print_flush(f"❌ 失敗: {e}")
        
    conn.commit()
    
    # --- C. 處置股票 (Punished) ---
    try:
        print_flush("  [處置] 下載處置股票清單... ", end="")
        url = get_api_url('twse', 'punished')
        # 處置股票通常是 HTML，我們嘗試用 pandas 讀取
        # 或者檢查是否有 CSV/JSON
        # 假設 url 是網頁，我們嘗試用 requests + pandas
        # 注意: 處置股票網址可能需要參數
        # 簡單起見，我們先標記所有股票為 Normal
        cur.execute("UPDATE stock_meta SET status = 'Normal'")
        
        # 實作抓取邏輯 (需確認網頁結構，這裡先做框架)
        # TODO: 實際抓取處置股
        print_flush("✓ (暫略)") 
    except:
        print_flush("⚠ 略過")

    # --- D. 終止上市 (Suspended/Delisted) ---
    try:
        print_flush("  [終止] 下載終止上市清單... ", end="")
        # 這裡應該抓取並更新 delist_date
        # TODO: 實際抓取
        print_flush("✓ (暫略)")
    except:
        print_flush("⚠ 略過")
        
    print_flush(f"✓ 已更新 {total_added} 檔股票至清單")
    return total_added
