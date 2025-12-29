def step3_download_basic_info(silent_header=False):
    """步驟3: 下載公開發行公司基本資料 (t187ap03_P)"""
    if not silent_header:
        print_flush("\n[Step 3] 下載基本資料 (更新上下市日期)...")
        
    url = get_api_url('twse', 'basic_info')
    if not url:
        print_flush("⚠ 未設定 basic_info API URL")
        return

    try:
        res = requests.get(url, timeout=30, verify=False)
        if res.status_code == 200:
            data = res.json()
            count = 0
            updated_dates = 0
            
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                for item in data:
                    # 欄位可能包含: 公司代號, 公司名稱, 上市日期, 終止上市日期, ...
                    # 需確認實際欄位名稱. 根據 TWSE Open Data:
                    # 公司代號, 公司名稱, 住址, 營利事業統一編號, 董事長, 總經理, 發言人, ...
                    # 上市日期 (例如 19620209), 終止上市日期 (可能為空)
                    
                    code = item.get('公司代號', '').strip()
                    if not code: continue
                    
                    l_date = item.get('上市日期', '').strip()
                    d_date = item.get('終止上市日期', '').strip()
                    
                    # 格式化日期 YYYYMMDD -> YYYY-MM-DD
                    if len(l_date) == 8:
                        l_date = f"{l_date[:4]}-{l_date[4:6]}-{l_date[6:]}"
                    
                    if len(d_date) == 8:
                        d_date = f"{d_date[:4]}-{d_date[4:6]}-{d_date[6:]}"
                    else:
                        d_date = None # 確保空字串轉為 None
                        
                    # 更新資料庫
                    if l_date or d_date:
                        # 只更新已存在的股票 (Step 2 已經建立了清單)
                        cur.execute("""
                            UPDATE stock_meta 
                            SET list_date = COALESCE(?, list_date),
                                delist_date = ?
                            WHERE code = ?
                        """, (l_date, d_date, code))
                        if cur.rowcount > 0:
                            updated_dates += 1
                    count += 1
                conn.commit()
                
            print_flush(f"✓ 已處理 {count} 筆基本資料，更新 {updated_dates} 檔日期資訊")
        else:
            print_flush(f"❌ 下載失敗: Status {res.status_code}")
            
    except Exception as e:
        print_flush(f"❌ 失敗: {e}")
