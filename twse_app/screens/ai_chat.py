"""
AI 助手畫面 - Gemini AI 整合
支援語音輸入和股票數據分析
"""
from kivy.uix.screenmanager import Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.graphics import Color, Rectangle, RoundedRectangle
from kivy.clock import Clock
from kivy.properties import StringProperty, BooleanProperty, ListProperty
from kivy.uix.popup import Popup
from kivy.core.clipboard import Clipboard
import threading

try:
    from src.ai_generator import get_ai_generator, AIGenerator
except ImportError:
    def get_ai_generator():
        return None


class ChatBubble(BoxLayout):
    """聊天氣泡"""
    
    def __init__(self, message: str, is_user: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.orientation = 'horizontal'
        self.size_hint_y = None
        self.padding = [10, 5]
        self.spacing = 10
        
        # 根據發送者調整排版
        if is_user:
            self.add_widget(Label(size_hint_x=0.2))  # 左側留白
            bg_color = (0.2, 0.4, 0.3, 1)
        else:
            bg_color = (0.15, 0.25, 0.2, 1)
        
        # 訊息內容
        content = BoxLayout(orientation='vertical', size_hint_x=0.8)
        
        # 發送者標籤
        sender = Label(
            text='👤 你' if is_user else '🤖 AI 助手',
            font_size=12,
            size_hint_y=None,
            height=20,
            halign='left' if not is_user else 'right',
            color=(0.6, 0.6, 0.6, 1)
        )
        sender.bind(size=sender.setter('text_size'))
        content.add_widget(sender)
        
        # 訊息文字
        msg_label = Label(
            text=message,
            font_size=14,
            size_hint_y=None,
            halign='left',
            valign='top',
            color=(0.9, 0.9, 0.9, 1)
        )
        msg_label.bind(
            width=lambda *x: setattr(msg_label, 'text_size', (msg_label.width - 20, None)),
            texture_size=lambda *x: setattr(msg_label, 'height', msg_label.texture_size[1] + 20)
        )
        
        # 背景
        with content.canvas.before:
            Color(*bg_color)
            content.rect = RoundedRectangle(
                pos=content.pos,
                size=content.size,
                radius=[10]
            )
        content.bind(pos=self._update_rect, size=self._update_rect)
        content._rect = content.rect
        
        content.add_widget(msg_label)
        self.add_widget(content)
        
        if not is_user:
            self.add_widget(Label(size_hint_x=0.2))  # 右側留白
        
        # 計算高度
        self.bind(minimum_height=self.setter('height'))
        self.height = max(60, msg_label.height + 40)
    
    def _update_rect(self, instance, value):
        if hasattr(instance, '_rect'):
            instance._rect.pos = instance.pos
            instance._rect.size = instance.size


class AIChatScreen(Screen):
    """AI 助手畫面"""
    
    is_recording = BooleanProperty(False)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.ai_generator = get_ai_generator()
        self.chat_history = []
        self.current_stock_context = None
        
        self._build_ui()
    
    def _build_ui(self):
        main_layout = BoxLayout(orientation='vertical', padding=5, spacing=5)
        
        # 標題
        header = BoxLayout(size_hint_y=0.06)
        title = Label(
            text='🤖 AI 助手',
            font_size=18,
            bold=True,
            color=(0.075, 0.925, 0.357, 1)
        )
        header.add_widget(title)
        
        # 清除對話按鈕
        clear_btn = Button(
            text='🗑️',
            size_hint_x=0.15,
            background_color=(0.5, 0.3, 0.3, 1)
        )
        clear_btn.bind(on_press=self.on_clear_chat)
        header.add_widget(clear_btn)
        
        main_layout.add_widget(header)
        
        # 對話區域
        self.chat_scroll = ScrollView(size_hint_y=0.68)
        self.chat_layout = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=10,
            padding=[5, 10]
        )
        self.chat_layout.bind(minimum_height=self.chat_layout.setter('height'))
        self.chat_scroll.add_widget(self.chat_layout)
        main_layout.add_widget(self.chat_scroll)
        
        # 輸入區域
        input_layout = BoxLayout(size_hint_y=0.1, spacing=5, padding=[0, 5])
        
        self.message_input = TextInput(
            hint_text='輸入問題...',
            multiline=False,
            font_size=14,
            size_hint_x=0.65
        )
        self.message_input.bind(on_text_validate=self.on_send_message)
        input_layout.add_widget(self.message_input)
        
        # 語音按鈕
        self.voice_btn = Button(
            text='🎤',
            font_size=20,
            size_hint_x=0.15,
            background_color=(0.3, 0.5, 0.4, 1)
        )
        self.voice_btn.bind(on_press=self.on_voice_input)
        input_layout.add_widget(self.voice_btn)
        
        # 送出按鈕
        send_btn = Button(
            text='➤',
            font_size=18,
            size_hint_x=0.2,
            background_color=(0.075, 0.925, 0.357, 1)
        )
        send_btn.bind(on_press=self.on_send_message)
        input_layout.add_widget(send_btn)
        
        main_layout.add_widget(input_layout)
        
        # 快速按鈕
        quick_layout = BoxLayout(size_hint_y=0.08, spacing=5)
        
        quick_btns = [
            ('🔌 生成插件', self.on_quick_plugin),
            ('📊 分析股票', self.on_quick_analysis),
            ('📖 解釋指標', self.on_quick_explain),
            ('📰 新聞', self.on_quick_news)
        ]
        
        for text, callback in quick_btns:
            btn = Button(
                text=text,
                font_size=11,
                background_color=(0.2, 0.3, 0.25, 1)
            )
            btn.bind(on_press=callback)
            quick_layout.add_widget(btn)
        
        main_layout.add_widget(quick_layout)
        
        # API 狀態
        self.status_label = Label(
            text='⚠️ 請在設定頁輸入 Gemini API Key',
            size_hint_y=0.04,
            font_size=12,
            color=(0.8, 0.6, 0.2, 1)
        )
        if self.ai_generator and self.ai_generator.is_configured():
            self.status_label.text = '✅ AI 已就緒'
            self.status_label.color = (0.2, 0.8, 0.4, 1)
        main_layout.add_widget(self.status_label)
        
        self.add_widget(main_layout)
        
        # 歡迎訊息
        self._add_message("你好！我是 AI 助手，可以幫你：\n• 分析股票技術面\n• 解釋技術指標\n• 生成掃描插件\n• 查詢財經新聞\n\n請問有什麼可以幫你的？", is_user=False)
    
    def _add_message(self, message: str, is_user: bool = True):
        """新增訊息到對話區"""
        bubble = ChatBubble(message, is_user)
        self.chat_layout.add_widget(bubble)
        self.chat_history.append({'role': 'user' if is_user else 'assistant', 'content': message})
        
        # 捲動到底部
        Clock.schedule_once(lambda dt: setattr(self.chat_scroll, 'scroll_y', 0), 0.1)
    
    def on_send_message(self, *args):
        """送出訊息"""
        message = self.message_input.text.strip()
        if not message:
            return
        
        # 清空輸入
        self.message_input.text = ''
        
        # 新增用戶訊息
        self._add_message(message, is_user=True)
        
        # 檢查 AI 服務
        if not self.ai_generator or not self.ai_generator.is_configured():
            self._add_message("⚠️ 請先在設定頁輸入 Gemini API Key", is_user=False)
            return
        
        # 顯示載入中
        self._add_message("思考中...", is_user=False)
        
        # 非同步呼叫 AI
        threading.Thread(target=self._call_ai, args=(message,)).start()
    
    def _call_ai(self, message: str):
        """呼叫 AI API (在背景執行緒)"""
        try:
            response = self.ai_generator.chat(message, self.current_stock_context)
            
            # 回到主執行緒更新 UI
            Clock.schedule_once(lambda dt: self._update_ai_response(response), 0)
            
        except Exception as e:
            Clock.schedule_once(
                lambda dt: self._update_ai_response(f"發生錯誤: {str(e)}"), 0
            )
    
    def _update_ai_response(self, response: str):
        """更新 AI 回覆 (在主執行緒)"""
        # 移除 "思考中..." 訊息
        if len(self.chat_layout.children) > 0:
            last_bubble = self.chat_layout.children[0]
            self.chat_layout.remove_widget(last_bubble)
        if self.chat_history and self.chat_history[-1]['content'] == "思考中...":
            self.chat_history.pop()
        
        # 新增 AI 回覆
        self._add_message(response, is_user=False)
    
    def on_voice_input(self, *args):
        """語音輸入"""
        if self.is_recording:
            self.is_recording = False
            self.voice_btn.text = '🎤'
            self.voice_btn.background_color = (0.3, 0.5, 0.4, 1)
            # TODO: 停止錄音並轉文字
        else:
            self.is_recording = True
            self.voice_btn.text = '⏹️'
            self.voice_btn.background_color = (0.8, 0.3, 0.3, 1)
            # TODO: 開始錄音
            self._start_voice_recognition()
    
    def _start_voice_recognition(self):
        """開始語音辨識"""
        # Android 上使用 pyjnius 呼叫語音辨識 API
        try:
            from jnius import autoclass
            
            Intent = autoclass('android.content.Intent')
            RecognizerIntent = autoclass('android.speech.RecognizerIntent')
            
            intent = Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH)
            intent.putExtra(
                RecognizerIntent.EXTRA_LANGUAGE_MODEL,
                RecognizerIntent.LANGUAGE_MODEL_FREE_FORM
            )
            intent.putExtra(RecognizerIntent.EXTRA_LANGUAGE, 'zh-TW')
            
            # TODO: 啟動 Activity 並處理結果
            
        except ImportError:
            # 非 Android 環境
            Clock.schedule_once(lambda dt: self._simulate_voice_input(), 1.5)
    
    def _simulate_voice_input(self):
        """模擬語音輸入 (開發用)"""
        self.is_recording = False
        self.voice_btn.text = '🎤'
        self.voice_btn.background_color = (0.3, 0.5, 0.4, 1)
        
        self.message_input.text = "2330 最近走勢如何？"
    
    def on_clear_chat(self, *args):
        """清除對話"""
        self.chat_layout.clear_widgets()
        self.chat_history = []
        
        if self.ai_generator:
            self.ai_generator.clear_history()
        
        self._add_message("對話已清除。有什麼可以幫你的？", is_user=False)
    
    def on_quick_plugin(self, *args):
        """快速: 生成插件"""
        self.message_input.text = "幫我建立一個掃描插件: "
        self.message_input.focus = True
    
    def on_quick_analysis(self, *args):
        """快速: 分析股票"""
        self.message_input.text = "分析股票: "
        self.message_input.focus = True
    
    def on_quick_explain(self, *args):
        """快速: 解釋指標"""
        self.message_input.text = "請解釋什麼是 "
        self.message_input.focus = True
    
    def on_quick_news(self, *args):
        """快速: 查詢新聞"""
        self.message_input.text = "最近有什麼財經新聞？"
        self.on_send_message()
    
    def set_stock_context(self, code: str, name: str, indicators: dict):
        """設定股票上下文 (從其他頁面傳入)"""
        self.current_stock_context = {
            'code': code,
            'name': name,
            **indicators
        }
        self.status_label.text = f'📊 已載入 {code} {name} 數據'
        self.status_label.color = (0.2, 0.8, 0.4, 1)
    
    def update_api_status(self, api_key: str):
        """更新 API Key 狀態"""
        if self.ai_generator:
            self.ai_generator.set_api_key(api_key)
            if api_key:
                self.status_label.text = '✅ AI 已就緒'
                self.status_label.color = (0.2, 0.8, 0.4, 1)
            else:
                self.status_label.text = '⚠️ 請在設定頁輸入 Gemini API Key'
                self.status_label.color = (0.8, 0.6, 0.2, 1)
