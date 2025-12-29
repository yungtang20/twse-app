def step1_check_holiday():
    """步驟1: 檢查今日是否為休市日"""
    print_flush("\n[Step 1] 檢查開休市日期...")
    
    today = datetime.now()
    today_str = today.strftime("%Y%m%d")
    
    # 1. 檢查週末
    if today.weekday() >= 5:
        print_flush(f"今日 ({today_str}) 是週末，休市。")
        return True
        
    # 2. 檢查 API (TWSE Holiday Schedule)
    try:
        url = get_api_url('twse', 'holiday_schedule')
        # 參數: response=json
        res = requests.get(f"{url}?response=json", timeout=10, verify=False)
        if res.status_code == 200:
            data = res.json()
            if data.get('stat') == 'OK':
                # data['data'] 包含休市列表
                # 格式: ["114年01月01日", "中華民國開國紀念日", "休市", "114年01月01日"]
                # 我們需要解析日期
                holidays = set()
                for row in data.get('data', []):
                    try:
                        d_str = row[0] # e.g. "114年01月01日"
                        # 簡單解析: 114年 -> 2025
                        year_roc = int(re.search(r'(\d+)年', d_str).group(1))
                        month = int(re.search(r'(\d+)月', d_str).group(1))
                        day = int(re.search(r'(\d+)日', d_str).group(1))
                        year_west = year_roc + 1911
                        h_date = f"{year_west}{month:02d}{day:02d}"
                        holidays.add(h_date)
                    except:
                        pass
                
                if today_str in holidays:
                    print_flush(f"今日 ({today_str}) 是國定假日，休市。")
                    return True
    except Exception as e:
        print_flush(f"⚠ 無法取得休市表 ({e})，使用備援檢查...")
        
    # 3. 備援: 檢查是否為已知假日 (靜態列表)
    # (這裡可以保留原本的 MARKET_HOLIDAYS_2025 邏輯作為最後防線)
    if is_market_holiday(int(today_str)):
        print_flush(f"今日 ({today_str}) 是已知假日，休市。")
        return True
        
    print_flush(f"今日 ({today_str}) 是交易日。")
    return False
