import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

from 最終修正 import step3_6_download_major_holders

if __name__ == "__main__":
    print("強制執行 Step 3.6 (集保戶股權分散表)...")
    try:
        step3_6_download_major_holders(force=True)
        print("\n✅ 更新完成")
    except Exception as e:
        print(f"\n❌ 更新失敗: {e}")
