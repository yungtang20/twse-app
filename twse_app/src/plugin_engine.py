"""
Plugin Engine - 插件引擎核心模組
提供插件的載入、驗證、執行功能
"""
import json
import ast
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Callable

# 危險關鍵字列表 - 禁止在插件代碼中使用
FORBIDDEN_KEYWORDS = [
    'import', 'from', '__import__', 'exec', 'eval', 'compile',
    'open', 'file', 'input', 'raw_input',
    'os', 'sys', 'subprocess', 'shutil', 'glob',
    '__builtins__', '__class__', '__bases__', '__subclasses__',
    'getattr', 'setattr', 'delattr', 'globals', 'locals',
    'breakpoint', 'exit', 'quit'
]

# 允許的內建函數
ALLOWED_BUILTINS = {
    'abs': abs,
    'all': all,
    'any': any,
    'bool': bool,
    'dict': dict,
    'enumerate': enumerate,
    'filter': filter,
    'float': float,
    'int': int,
    'len': len,
    'list': list,
    'map': map,
    'max': max,
    'min': min,
    'range': range,
    'round': round,
    'set': set,
    'sorted': sorted,
    'str': str,
    'sum': sum,
    'tuple': tuple,
    'zip': zip,
    'True': True,
    'False': False,
    'None': None,
}


class PluginValidationError(Exception):
    """插件驗證錯誤"""
    pass


class PluginExecutionError(Exception):
    """插件執行錯誤"""
    pass


class PluginValidator:
    """插件代碼驗證器"""
    
    @staticmethod
    def check_syntax(code: str) -> Tuple[bool, str]:
        """
        檢查 Python 語法是否正確
        
        Returns:
            (is_valid, error_message)
        """
        try:
            ast.parse(code)
            return True, ""
        except SyntaxError as e:
            return False, f"語法錯誤 (第 {e.lineno} 行): {e.msg}"
    
    @staticmethod
    def check_security(code: str) -> Tuple[bool, str]:
        """
        檢查代碼是否包含危險關鍵字
        
        Returns:
            (is_safe, error_message)
        """
        code_lower = code.lower()
        
        for keyword in FORBIDDEN_KEYWORDS:
            # 使用正則表達式匹配完整單詞
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, code_lower):
                return False, f"禁止使用關鍵字: {keyword}"
        
        return True, ""
    
    @staticmethod
    def validate(code: str) -> Tuple[bool, str]:
        """
        完整驗證插件代碼
        
        Returns:
            (is_valid, error_message)
        """
        # 1. 語法檢查
        is_valid, error = PluginValidator.check_syntax(code)
        if not is_valid:
            return False, error
        
        # 2. 安全性檢查
        is_safe, error = PluginValidator.check_security(code)
        if not is_safe:
            return False, error
        
        return True, ""


class SafeExecutor:
    """安全執行器 - 在受限環境中執行插件代碼"""
    
    def __init__(self, helpers: Dict[str, Callable] = None):
        """
        初始化安全執行器
        
        Args:
            helpers: 提供給插件使用的輔助函數字典
        """
        self.helpers = helpers or {}
    
    def create_namespace(self) -> Dict[str, Any]:
        """建立受限的執行命名空間"""
        namespace = {
            '__builtins__': ALLOWED_BUILTINS.copy(),
        }
        # 加入輔助函數
        namespace.update(self.helpers)
        return namespace
    
    def execute(self, code: str, data: Dict, params: Dict) -> List[Tuple]:
        """
        執行插件代碼
        
        Args:
            code: 插件 Python 代碼
            data: 股票指標數據 {code: indicators}
            params: 執行參數 {min_vol, limit}
        
        Returns:
            掃描結果列表
        """
        # 驗證代碼
        is_valid, error = PluginValidator.validate(code)
        if not is_valid:
            raise PluginValidationError(error)
        
        # 建立命名空間
        namespace = self.create_namespace()
        
        try:
            # 執行插件代碼 (定義 scan 函數)
            exec(code, namespace)
            
            # 檢查是否定義了 scan 函數
            if 'scan' not in namespace:
                raise PluginExecutionError("插件必須定義 scan(data, params) 函數")
            
            scan_func = namespace['scan']
            
            # 執行掃描
            results = scan_func(data, params)
            
            return results if results else []
            
        except PluginValidationError:
            raise
        except Exception as e:
            raise PluginExecutionError(f"執行錯誤: {str(e)}")


class Plugin:
    """插件資料結構"""
    
    def __init__(self, 
                 id: str,
                 name: str,
                 description: str = "",
                 code: str = "",
                 builtin: bool = False,
                 enabled: bool = True):
        self.id = id
        self.name = name
        self.description = description
        self.code = code
        self.builtin = builtin
        self.enabled = enabled
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'code': self.code,
            'builtin': self.builtin,
            'enabled': self.enabled
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Plugin':
        return cls(
            id=data.get('id', ''),
            name=data.get('name', ''),
            description=data.get('description', ''),
            code=data.get('code', ''),
            builtin=data.get('builtin', False),
            enabled=data.get('enabled', True)
        )


class PluginEngine:
    """
    插件引擎 - 管理插件的生命週期
    
    功能:
    - 載入內建插件和用戶插件
    - 插件 CRUD 操作
    - 執行插件掃描
    """
    
    def __init__(self, data_dir: str = None):
        """
        初始化插件引擎
        
        Args:
            data_dir: 資料目錄路徑 (存放 JSON 檔案)
        """
        self.data_dir = Path(data_dir) if data_dir else Path(__file__).parent.parent / 'data'
        self.plugins: Dict[str, Plugin] = {}
        self.executor = SafeExecutor()
        
        # 載入插件
        self._load_plugins()
    
    def _load_plugins(self):
        """載入所有插件"""
        # 載入內建插件
        default_path = self.data_dir / 'default_plugins.json'
        if default_path.exists():
            with open(default_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    plugin = Plugin.from_dict(item)
                    plugin.builtin = True
                    self.plugins[plugin.id] = plugin
        
        # 載入用戶插件
        user_path = self.data_dir / 'user_plugins.json'
        if user_path.exists():
            with open(user_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    plugin = Plugin.from_dict(item)
                    plugin.builtin = False
                    self.plugins[plugin.id] = plugin
    
    def _save_user_plugins(self):
        """儲存用戶插件"""
        user_plugins = [p.to_dict() for p in self.plugins.values() if not p.builtin]
        user_path = self.data_dir / 'user_plugins.json'
        
        # 確保目錄存在
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        with open(user_path, 'w', encoding='utf-8') as f:
            json.dump(user_plugins, f, ensure_ascii=False, indent=2)
    
    def get_all_plugins(self) -> List[Plugin]:
        """取得所有插件列表"""
        return list(self.plugins.values())
    
    def get_enabled_plugins(self) -> List[Plugin]:
        """取得已啟用的插件"""
        return [p for p in self.plugins.values() if p.enabled]
    
    def get_plugin(self, plugin_id: str) -> Optional[Plugin]:
        """取得單個插件"""
        return self.plugins.get(plugin_id)
    
    def create_plugin(self, name: str, description: str, code: str) -> Plugin:
        """
        建立新插件
        
        Args:
            name: 插件名稱
            description: 插件描述
            code: Python 代碼
        
        Returns:
            新建立的 Plugin
        """
        # 驗證代碼
        is_valid, error = PluginValidator.validate(code)
        if not is_valid:
            raise PluginValidationError(error)
        
        # 生成 ID
        import time
        plugin_id = f"custom_{int(time.time())}"
        
        plugin = Plugin(
            id=plugin_id,
            name=name,
            description=description,
            code=code,
            builtin=False,
            enabled=True
        )
        
        self.plugins[plugin_id] = plugin
        self._save_user_plugins()
        
        return plugin
    
    def update_plugin(self, plugin_id: str, name: str = None, 
                      description: str = None, code: str = None) -> Plugin:
        """更新插件"""
        plugin = self.plugins.get(plugin_id)
        if not plugin:
            raise ValueError(f"插件不存在: {plugin_id}")
        
        if plugin.builtin:
            raise ValueError("無法修改內建插件")
        
        if code:
            is_valid, error = PluginValidator.validate(code)
            if not is_valid:
                raise PluginValidationError(error)
            plugin.code = code
        
        if name:
            plugin.name = name
        if description:
            plugin.description = description
        
        self._save_user_plugins()
        return plugin
    
    def delete_plugin(self, plugin_id: str) -> bool:
        """刪除插件"""
        plugin = self.plugins.get(plugin_id)
        if not plugin:
            return False
        
        if plugin.builtin:
            raise ValueError("無法刪除內建插件")
        
        del self.plugins[plugin_id]
        self._save_user_plugins()
        return True
    
    def toggle_plugin(self, plugin_id: str) -> bool:
        """切換插件啟用狀態"""
        plugin = self.plugins.get(plugin_id)
        if not plugin:
            return False
        
        plugin.enabled = not plugin.enabled
        self._save_user_plugins()
        return plugin.enabled
    
    def execute_plugin(self, plugin_id: str, data: Dict, params: Dict) -> List[Tuple]:
        """
        執行插件掃描
        
        Args:
            plugin_id: 插件 ID
            data: 股票指標數據
            params: 執行參數
        
        Returns:
            掃描結果列表
        """
        plugin = self.plugins.get(plugin_id)
        if not plugin:
            raise ValueError(f"插件不存在: {plugin_id}")
        
        if not plugin.enabled:
            raise ValueError(f"插件已停用: {plugin.name}")
        
        return self.executor.execute(plugin.code, data, params)
    
    def export_plugin(self, plugin_id: str) -> str:
        """
        導出插件為 JSON 字串
        
        Returns:
            JSON 格式的插件資料
        """
        plugin = self.plugins.get(plugin_id)
        if not plugin:
            raise ValueError(f"插件不存在: {plugin_id}")
        
        return json.dumps(plugin.to_dict(), ensure_ascii=False, indent=2)
    
    def import_plugin(self, json_str: str) -> Plugin:
        """
        從 JSON 字串導入插件
        
        Args:
            json_str: JSON 格式的插件資料
        
        Returns:
            導入的 Plugin
        """
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            raise ValueError("無效的 JSON 格式")
        
        # 驗證代碼
        code = data.get('code', '')
        is_valid, error = PluginValidator.validate(code)
        if not is_valid:
            raise PluginValidationError(error)
        
        # 生成新 ID (避免衝突)
        import time
        new_id = f"imported_{int(time.time())}"
        
        plugin = Plugin(
            id=new_id,
            name=data.get('name', '導入的插件'),
            description=data.get('description', ''),
            code=code,
            builtin=False,
            enabled=True
        )
        
        self.plugins[new_id] = plugin
        self._save_user_plugins()
        
        return plugin
