import sys
import os
import time

# Add current directory to path
sys.path.append(os.getcwd())

from 最終修正 import (
    step1_fetch_stock_list,
    step2_download_tpex_daily,
    step3_download_twse_daily,
    step3_5_download_institutional,
    step3_6_download_major_holders,
    step3_7_fetch_margin_data,
    step3_8_fetch_market_index,
    step4_check_data_gaps,
    step5_clean_delisted,
    step4_load_data,
    step6_verify_and_backfill,
    step7_calc_indicators,
    step8_sync_supabase,
    clear_progress
)

def run_full_update():
    print("開始執行每日更新 (Steps 1-8)...")
    start_time = time.time()
    
    try:
        step1_fetch_stock_list()
        step2_download_tpex_daily()
        step3_download_twse_daily()
        step3_5_download_institutional(days=3)
        step3_6_download_major_holders()
        step3_7_fetch_margin_data()
        step3_8_fetch_market_index()
        
        step4_check_data_gaps()
        step5_clean_delisted()
        
        # Step 4 loads data for subsequent steps
        data = step4_load_data()
        
        # step5_calc_tech_indicators is not in the auto-update list, 
        # but step7_calc_indicators likely handles it or uses data.
        # Wait, step7_calc_indicators calculates indicators.
        
        step6_verify_and_backfill(data, resume=True)
        step7_calc_indicators(data)
        # step8_sync_supabase() # Skip sync for now unless requested
        
        clear_progress()
        
        elapsed = time.time() - start_time
        print(f"\n✅ 每日更新完成! 耗時: {elapsed:.2f} 秒")
        
    except Exception as e:
        print(f"\n❌ 更新失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_full_update()
