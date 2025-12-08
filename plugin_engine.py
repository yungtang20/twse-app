"""
Plugin Engine - 插件執行引擎

提供:
1. PluginManager - 載入/儲存/管理插件
2. PluginExecutor - 安全沙盒執行插件程式碼
"""

import json
import os
from pathlib import Path


class PluginManager:
    """插件管理器"""
    
    def __init__(self, plugin_dir=None):
        """
        初始化插件管理器
        
        Args:
            plugin_dir: 插件目錄路徑, 預設為 ./plugins/
        """
        if plugin_dir is None:
            # 預設: 與主程式同目錄的 plugins/
            plugin_dir = Path(__file__).parent / "plugins"
        
        self.plugin_dir = Path(plugin_dir)
        self.default_path = self.plugin_dir / "default_plugins.json"
        self.user_path = self.plugin_dir / "user_plugins.json"
        
        self._plugins = {}  # id -> plugin_def
        self._load_all()
    
    def _load_all(self):
        """載入所有插件"""
        self._plugins = {}
        
        # 載入預設插件
        if self.default_path.exists():
            self._load_from_file(self.default_path)
        
        # 載入使用者插件 (會覆蓋同 ID 的預設插件)
        if self.user_path.exists():
            self._load_from_file(self.user_path, is_user=True)
    
    def _load_from_file(self, path, is_user=False):
        """從 JSON 檔載入插件"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for plugin in data.get('plugins', []):
                plugin['is_user'] = is_user
                self._plugins[plugin['id']] = plugin
                
        except Exception as e:
            print(f"[PluginManager] 載入 {path} 失敗: {e}")
    
    def get_all_plugins(self):
        """取得所有插件"""
        return list(self._plugins.values())
    
    def get_enabled_plugins(self):
        """取得已啟用的插件"""
        return [p for p in self._plugins.values() if p.get('enabled', True)]
    
    def get_plugin(self, plugin_id):
        """取得指定插件"""
        return self._plugins.get(plugin_id)
    
    def save_user_plugin(self, plugin_def):
        """
        儲存使用者自訂插件
        
        Args:
            plugin_def: 插件定義 dict
        
        Returns:
            bool: 是否成功
        """
        try:
            # 載入現有使用者插件
            user_plugins = {'version': '1.0', 'plugins': []}
            if self.user_path.exists():
                with open(self.user_path, 'r', encoding='utf-8') as f:
                    user_plugins = json.load(f)
            
            # 更新或新增
            plugins = user_plugins.get('plugins', [])
            found = False
            for i, p in enumerate(plugins):
                if p['id'] == plugin_def['id']:
                    plugins[i] = plugin_def
                    found = True
                    break
            
            if not found:
                plugins.append(plugin_def)
            
            user_plugins['plugins'] = plugins
            
            # 儲存
            self.plugin_dir.mkdir(parents=True, exist_ok=True)
            with open(self.user_path, 'w', encoding='utf-8') as f:
                json.dump(user_plugins, f, ensure_ascii=False, indent=2)
            
            # 重新載入
            self._load_all()
            return True
            
        except Exception as e:
            print(f"[PluginManager] 儲存失敗: {e}")
            return False
    
    def delete_user_plugin(self, plugin_id):
        """刪除使用者自訂插件"""
        try:
            if not self.user_path.exists():
                return False
            
            with open(self.user_path, 'r', encoding='utf-8') as f:
                user_plugins = json.load(f)
            
            plugins = user_plugins.get('plugins', [])
            user_plugins['plugins'] = [p for p in plugins if p['id'] != plugin_id]
            
            with open(self.user_path, 'w', encoding='utf-8') as f:
                json.dump(user_plugins, f, ensure_ascii=False, indent=2)
            
            self._load_all()
            return True
            
        except Exception as e:
            print(f"[PluginManager] 刪除失敗: {e}")
            return False


class PluginExecutor:
    """插件執行器 (安全沙盒)"""
    
    # 允許在插件中使用的內建函數
    ALLOWED_BUILTINS = {
        'abs', 'all', 'any', 'bool', 'dict', 'enumerate', 'float',
        'int', 'len', 'list', 'max', 'min', 'range', 'round',
        'sorted', 'str', 'sum', 'tuple', 'zip'
    }
    
    def __init__(self, helper_functions=None):
        """
        初始化執行器
        
        Args:
            helper_functions: 額外的輔助函數 dict
        """
        self.helpers = helper_functions or {}
    
    def execute(self, code, data, params):
        """
        在沙盒中執行插件程式碼
        
        Args:
            code: 插件程式碼 (包含 def scan(data, params) 函數)
            data: {code: indicators_dict} 指標數據
            params: 使用者參數
        
        Returns:
            list: 掃描結果 [(code, value, indicators), ...]
        """
        # 建立安全的執行環境
        safe_builtins = {k: __builtins__[k] for k in self.ALLOWED_BUILTINS 
                        if k in __builtins__}
        
        # 加入輔助函數
        safe_globals = {
            '__builtins__': safe_builtins,
            **self.helpers
        }
        
        local_vars = {}
        
        try:
            # 編譯並執行程式碼
            exec(code, safe_globals, local_vars)
            
            # 取得 scan 函數
            scan_func = local_vars.get('scan')
            if not callable(scan_func):
                raise ValueError("插件必須定義 scan(data, params) 函數")
            
            # 執行掃描
            results = scan_func(data, params)
            return results
            
        except Exception as e:
            print(f"[PluginExecutor] 執行錯誤: {e}")
            return []
    
    def validate_code(self, code):
        """
        驗證程式碼安全性
        
        Args:
            code: 插件程式碼
        
        Returns:
            tuple: (is_valid, error_message)
        """
        # 禁止的關鍵字
        forbidden = ['import', 'exec', 'eval', 'open', 'file', 
                    '__import__', 'globals', 'locals', 'compile']
        
        for kw in forbidden:
            if kw in code:
                return False, f"禁止使用 '{kw}'"
        
        # 檢查是否有 scan 函數定義
        if 'def scan(' not in code:
            return False, "必須定義 scan(data, params) 函數"
        
        return True, None


# 全域實例
_plugin_manager = None

def get_plugin_manager():
    """取得全域 PluginManager 實例"""
    global _plugin_manager
    if _plugin_manager is None:
        _plugin_manager = PluginManager()
    return _plugin_manager


class AIPluginGenerator:
    """AI 插件生成器 - 使用 Gemini API 從自然語言生成掃描插件"""
    
    PROMPT_TEMPLATE = """你是一個台股技術分析專家。請根據使用者需求生成一個 Python 掃描插件。

## 可用的指標欄位:
- close, open, high, low (價格)
- close_prev, vol_prev (前一日數據)
- volume (成交量, 單位:股)
- ma3, ma5, ma10, ma20, ma60, ma120, ma200 (均線)
- mfi14 (資金流量指標, 0-100)
- k9, d9 (KD指標, 0-100)
- rsi14 (RSI指標, 0-100)
- vwap20 (成交量加權平均價)
- vp_poc, vp_upper, vp_lower (籌碼分布)
- smart_score (聰明錢指標, 0-5)
- change_pct (漲跌幅%)

## 輸出格式:
只輸出 def scan(data, params): 函數程式碼，不要任何說明文字。

## 範例:
def scan(data, params):
    results = []
    min_vol = params.get('min_volume', 100000)
    for code, ind in data.items():
        vol = ind.get('volume', 0) or 0
        if vol < min_vol:
            continue
        mfi = ind.get('mfi14', 0) or 0
        if mfi > 70:
            results.append((code, mfi, ind))
    results.sort(key=lambda x: x[1], reverse=True)
    return results

## 使用者需求:
{user_request}
"""
    
    CONFIG_FILE = "gemini_config.json"

    def __init__(self, api_key=None):
        """
        初始化 AI 插件生成器
        
        Args:
            api_key: Gemini API Key (可選)
        """
        self.api_key = api_key or self._load_api_key()
        self.api_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
    
    def _get_config_path(self):
        """取得設定檔路徑"""
        return Path(__file__).parent / self.CONFIG_FILE
    
    def _load_api_key(self):
        """從設定檔載入 API Key"""
        config_path = self._get_config_path()
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                return config.get('gemini_api_key', '')
            except:
                pass
        # 回退到環境變數
        return os.environ.get('GEMINI_API_KEY', '')
    
    def save_api_key(self, api_key):
        """儲存 API Key 到設定檔"""
        config_path = self._get_config_path()
        config = {}
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            except:
                pass
        
        config['gemini_api_key'] = api_key
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        self.api_key = api_key
        return True
    
    def has_api_key(self):
        """檢查是否有 API Key"""
        return bool(self.api_key)
    
    def generate(self, user_request):
        """
        從自然語言生成插件程式碼
        
        Args:
            user_request: 使用者的需求描述 (中文)
        
        Returns:
            tuple: (success, code_or_error)
        """
        if not self.api_key:
            return False, "請先設定 Gemini API Key"
        
        try:
            import requests
        except ImportError:
            return False, "請安裝 requests 套件"
        
        prompt = self.PROMPT_TEMPLATE.format(user_request=user_request)
        
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }]
        }
        
        try:
            response = requests.post(
                f"{self.api_url}?key={self.api_key}",
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                return False, f"API 錯誤: {response.status_code}"
            
            result = response.json()
            text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            
            # 清理回應 (移除 markdown 標記)
            code = self._clean_code(text)
            
            # 驗證程式碼
            executor = PluginExecutor()
            is_valid, error = executor.validate_code(code)
            
            if not is_valid:
                return False, f"程式碼驗證失敗: {error}"
            
            return True, code
            
        except Exception as e:
            return False, f"生成失敗: {str(e)}"
    
    def _clean_code(self, text):
        """清理 AI 回應中的程式碼"""
        # 移除 markdown 程式碼區塊標記
        text = text.strip()
        if text.startswith('```python'):
            text = text[9:]
        elif text.startswith('```'):
            text = text[3:]
        if text.endswith('```'):
            text = text[:-3]
        return text.strip()
    
    def create_plugin_def(self, name, description, code):
        """
        建立插件定義 dict
        
        Args:
            name: 插件名稱
            description: 插件描述
            code: scan() 函數程式碼
        
        Returns:
            dict: 插件定義
        """
        import re
        # 生成 ID (轉小寫, 空格變底線)
        plugin_id = f"ai_{re.sub(r'[^a-z0-9]', '_', name.lower())}"
        
        return {
            "id": plugin_id,
            "name": name,
            "description": description,
            "version": "1.0.0",
            "author": "ai_generated",
            "builtin": False,
            "enabled": True,
            "params": {
                "min_volume": {"type": "int", "default": 100000, "label": "最小成交量(股)"}
            },
            "code": code
        }
    
    def generate_and_save(self, user_request, name=None):
        """
        生成並儲存插件
        
        Args:
            user_request: 使用者需求描述
            name: 插件名稱 (可選, 未提供則從需求自動生成)
        
        Returns:
            tuple: (success, message)
        """
        # 生成程式碼
        success, result = self.generate(user_request)
        if not success:
            return False, result
        
        code = result
        
        # 生成名稱
        if not name:
            name = user_request[:20] + "..." if len(user_request) > 20 else user_request
        
        # 建立插件定義
        plugin_def = self.create_plugin_def(name, user_request, code)
        
        # 儲存
        pm = get_plugin_manager()
        if pm.save_user_plugin(plugin_def):
            return True, f"插件 '{name}' 已儲存！"
        else:
            return False, "儲存失敗"


def get_ai_generator(api_key=None):
    """取得 AI 插件生成器實例"""
    return AIPluginGenerator(api_key)

