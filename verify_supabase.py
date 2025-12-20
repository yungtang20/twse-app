import os
import sys
from supabase import create_client

# Supabase Config
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_secret_XSeaHx_76CRxA6j8nZ3qDg_nzgFgTAN"

def verify_sync():
    print(f"正在連接 Supabase: {SUPABASE_URL} ...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # 查詢 institutional_investors 表格的最後 5 筆資料
        print("正在查詢 institutional_investors (法人買賣超) 最新資料...")
        response = supabase.table("institutional_investors") \
            .select("*") \
            .order("date_int", desc=True) \
            .limit(5) \
            .execute()
            
        data = response.data
        
        if not data:
            print("❌ 錯誤: 雲端資料庫是空的！")
            return
            
        print(f"✓ 成功讀取！共 {len(data)} 筆資料：")
        print("-" * 60)
        print(f"{'日期':<10} | {'代號':<6} | {'外資買超':<10} | {'投信買超':<10} | {'自營商買超':<10}")
        print("-" * 60)
        
        for row in data:
            date = str(row.get('date_int', ''))
            code = row.get('code', '')
            f_buy = row.get('foreign_buy', 0)
            t_buy = row.get('trust_buy', 0)
            d_buy = row.get('dealer_buy', 0)
            
            print(f"{date:<10} | {code:<6} | {f_buy:<10,} | {t_buy:<10,} | {d_buy:<10,}")
            
        print("-" * 60)
        print("🎉 測試成功！您的資料已經安全地儲存在雲端了。")
        
    except Exception as e:
        print(f"❌ 測試失敗: {e}")

if __name__ == "__main__":
    verify_sync()
