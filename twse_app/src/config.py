"""
配置文件 - Supabase 連線資訊
"""

# Supabase 連線設定
SUPABASE_URL = "https://gqiyvefcldxslrqpqlri.supabase.co"
SUPABASE_KEY = "sb_publishable_yXSGYxyxPMaoVu4MbGK5Vw_IuZsl5yu"

# 裝置識別碼（首次啟動時產生）
import uuid
import os

CONFIG_FILE = "device_config.json"

def get_device_id():
    """取得或建立裝置識別碼"""
    import json
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                return config.get('device_id')
        except:
            pass
    
    # 產生新的 device_id
    device_id = str(uuid.uuid4())
    
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump({'device_id': device_id}, f)
    except:
        pass
    
    return device_id

DEVICE_ID = get_device_id()
