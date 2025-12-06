"""
AI Generator - Gemini API 整合模組
用於生成插件代碼和股票分析
"""
import json
import re
from typing import Dict, Optional, Tuple, List


# Gemini API 配置
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

# 插件生成的系統提示
PLUGIN_GENERATION_PROMPT = """你是一個股票指標插件生成器。根據用戶的自然語言描述，生成符合以下格式的 Python 代碼。

## 可用資料欄位
- close: 收盤價
- open: 開盤價
- high: 最高價
- low: 最低價
- volume: 成交量 (張)
- ma3, ma5, ma10, ma20, ma60, ma120, ma200: 移動平均線
- wma20: 加權移動平均
- mfi14: MFI 資金流量指標 (0-100)
- rsi6, rsi12, rsi14: RSI 指標
- month_k, month_d: 月 KD 指標
- smart_score: 聰明錢分數 (0-5)
- vp_upper, vp_lower: VP 籌碼上下界
- vwap20: 成交量加權均價
- poc: 籌碼集中價

## 可用輔助函數
- safe_float(val, default=0): 安全轉換浮點數
- safe_int(val, default=0): 安全轉換整數
- is_crossing_up(curr, prev, curr_ref, prev_ref): 判斷向上穿越
- is_crossing_down(...): 判斷向下穿越
- percent_change(curr, prev): 計算變化百分比
- in_range(value, lower, upper): 判斷是否在範圍內

## 輸出格式
只返回 Python 代碼，不要加任何說明文字。代碼必須定義 scan 函數：

def scan(data, params):
    results = []
    min_vol = params.get('min_vol', 500000)
    limit = params.get('limit', 20)
    
    for code, ind in data.items():
        vol = safe_int(ind.get('volume', 0))
        if vol < min_vol:
            continue
        
        # 你的條件邏輯
        
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:limit]

## 規則
1. 必須定義 scan(data, params) 函數
2. 必須返回 list of (code, sort_value, indicators)
3. 必須使用 safe_float/safe_int 處理數值
4. 禁止使用 import, os, sys, open 等危險操作
5. 只能使用上述列出的資料欄位和輔助函數
"""

# 股票分析的系統提示
STOCK_ANALYSIS_PROMPT = """你是一個專業的股票技術分析師 AI 助手。
根據提供的股票數據進行分析，給出客觀的技術面解讀。

## 回覆規則
1. 使用繁體中文回覆
2. 分析要具體，引用實際數據
3. 說明趨勢、支撐壓力、資金流向
4. 最後加上風險提示
5. 保持客觀，不做投資建議

## 回覆格式
📊 **{股票代碼} {股票名稱}** 技術面分析

**價格趨勢**
- ...

**均線分析**
- ...

**資金指標**
- ...

**籌碼分布**
- ...

**總結**
- ...

⚠️ **風險提示**: 以上僅為技術面分析，不構成投資建議。
"""


class AIGenerator:
    """
    AI 生成器 - 使用 Gemini API
    """
    
    def __init__(self, api_key: str = None):
        """
        初始化 AI 生成器
        
        Args:
            api_key: Gemini API Key
        """
        self.api_key = api_key
        self._chat_history: List[Dict] = []
    
    def set_api_key(self, api_key: str):
        """設定 API Key"""
        self.api_key = api_key
    
    def is_configured(self) -> bool:
        """檢查是否已設定 API Key"""
        return bool(self.api_key)
    
    async def _call_gemini_api(self, prompt: str, system_prompt: str = None) -> str:
        """
        呼叫 Gemini API
        
        Args:
            prompt: 用戶輸入
            system_prompt: 系統提示 (可選)
        
        Returns:
            AI 回覆文字
        """
        if not self.api_key:
            raise ValueError("未設定 Gemini API Key")
        
        # 使用 aiohttp 或 kivy 的 HTTP 請求
        # 這裡先使用同步方式實作，之後可改為 async
        import urllib.request
        import ssl
        
        url = f"{GEMINI_API_URL}?key={self.api_key}"
        
        # 建立請求內容
        contents = []
        
        if system_prompt:
            contents.append({
                "role": "user",
                "parts": [{"text": system_prompt}]
            })
            contents.append({
                "role": "model", 
                "parts": [{"text": "了解，我會按照指示回覆。"}]
            })
        
        contents.append({
            "role": "user",
            "parts": [{"text": prompt}]
        })
        
        request_data = {
            "contents": contents,
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 2048
            }
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(request_data).encode('utf-8'),
                headers=headers,
                method='POST'
            )
            
            # 忽略 SSL 驗證 (開發用)
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            with urllib.request.urlopen(req, context=context, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                # 解析回覆
                if 'candidates' in result and len(result['candidates']) > 0:
                    candidate = result['candidates'][0]
                    if 'content' in candidate and 'parts' in candidate['content']:
                        return candidate['content']['parts'][0].get('text', '')
                
                return "AI 無法生成回覆"
                
        except Exception as e:
            raise Exception(f"API 呼叫失敗: {str(e)}")
    
    def generate_plugin_code(self, description: str) -> Tuple[str, str, str]:
        """
        根據描述生成插件代碼
        
        Args:
            description: 用戶的自然語言描述
        
        Returns:
            (plugin_name, plugin_description, python_code)
        """
        import asyncio
        
        prompt = f"用戶需求: {description}\n\n請生成符合格式的 Python 插件代碼。"
        
        try:
            # 呼叫 API
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果已在 async context，使用 create_task
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._call_gemini_api(prompt, PLUGIN_GENERATION_PROMPT)
                    )
                    response = future.result(timeout=60)
            else:
                response = asyncio.run(
                    self._call_gemini_api(prompt, PLUGIN_GENERATION_PROMPT)
                )
            
            # 提取代碼
            code = self._extract_code(response)
            
            # 生成名稱和描述
            name = self._generate_plugin_name(description)
            desc = description[:100] if len(description) > 100 else description
            
            return name, desc, code
            
        except Exception as e:
            raise Exception(f"生成失敗: {str(e)}")
    
    def _extract_code(self, response: str) -> str:
        """從 AI 回覆中提取 Python 代碼"""
        # 嘗試提取 ```python ... ``` 區塊
        pattern = r'```python\s*(.*?)\s*```'
        match = re.search(pattern, response, re.DOTALL)
        
        if match:
            return match.group(1).strip()
        
        # 嘗試提取 ``` ... ``` 區塊
        pattern = r'```\s*(.*?)\s*```'
        match = re.search(pattern, response, re.DOTALL)
        
        if match:
            return match.group(1).strip()
        
        # 如果沒有代碼區塊，檢查是否直接是代碼
        if 'def scan' in response:
            return response.strip()
        
        return response.strip()
    
    def _generate_plugin_name(self, description: str) -> str:
        """根據描述生成插件名稱"""
        # 簡單實作：取描述前 20 字
        if len(description) > 20:
            return description[:20] + "..."
        return description
    
    def analyze_stock(self, code: str, name: str, indicators: Dict) -> str:
        """
        分析股票技術面
        
        Args:
            code: 股票代碼
            name: 股票名稱
            indicators: 指標數據
        
        Returns:
            分析文字
        """
        import asyncio
        
        # 建立 context 資訊
        context = f"""
股票代碼: {code}
股票名稱: {name}
收盤價: {indicators.get('close', 'N/A')}
開盤價: {indicators.get('open', 'N/A')}
成交量: {indicators.get('volume', 'N/A')} 張

均線:
- MA5: {indicators.get('ma5', 'N/A')}
- MA20: {indicators.get('ma20', 'N/A')}
- MA60: {indicators.get('ma60', 'N/A')}
- MA120: {indicators.get('ma120', 'N/A')}
- MA200: {indicators.get('ma200', 'N/A')}

技術指標:
- MFI(14): {indicators.get('mfi14', 'N/A')}
- RSI(14): {indicators.get('rsi14', 'N/A')}
- 月K: {indicators.get('month_k', 'N/A')}
- 月D: {indicators.get('month_d', 'N/A')}
- Smart Score: {indicators.get('smart_score', 'N/A')}/5

籌碼:
- VP 上緣: {indicators.get('vp_upper', 'N/A')}
- VP 下緣: {indicators.get('vp_lower', 'N/A')}
- POC: {indicators.get('poc', 'N/A')}
- VWAP: {indicators.get('vwap20', 'N/A')}
"""
        
        prompt = f"請分析以下股票的技術面:\n{context}"
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._call_gemini_api(prompt, STOCK_ANALYSIS_PROMPT)
                    )
                    return future.result(timeout=60)
            else:
                return asyncio.run(
                    self._call_gemini_api(prompt, STOCK_ANALYSIS_PROMPT)
                )
        except Exception as e:
            return f"分析失敗: {str(e)}"
    
    def chat(self, message: str, stock_context: Dict = None) -> str:
        """
        AI 對話
        
        Args:
            message: 用戶訊息
            stock_context: 股票上下文 (可選)
        
        Returns:
            AI 回覆
        """
        import asyncio
        
        # 建立提示
        system_prompt = """你是一個專業的股票分析 AI 助手。
使用繁體中文回覆，保持友善和專業。
如果用戶問到股票相關問題，請提供技術面分析。
最後加上風險提示。"""
        
        prompt = message
        
        # 如果有股票上下文，附加資訊
        if stock_context:
            context_str = "\n".join([
                f"- {k}: {v}" for k, v in stock_context.items()
            ])
            prompt = f"{message}\n\n[當前股票資料]\n{context_str}"
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._call_gemini_api(prompt, system_prompt)
                    )
                    return future.result(timeout=60)
            else:
                return asyncio.run(
                    self._call_gemini_api(prompt, system_prompt)
                )
        except Exception as e:
            return f"發生錯誤: {str(e)}"
    
    def clear_history(self):
        """清除對話歷史"""
        self._chat_history = []


# 全域實例
_ai_generator: Optional[AIGenerator] = None


def get_ai_generator() -> AIGenerator:
    """取得 AI Generator 實例"""
    global _ai_generator
    if _ai_generator is None:
        _ai_generator = AIGenerator()
    return _ai_generator
