"""
台股分析 App - v1.0.4 改進 UI
- 更大的字體
- 更好的間距
- 底部導航欄更清晰
"""
import os
from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen, SlideTransition
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.graphics import Color, Rectangle, RoundedRectangle
from kivy.clock import Clock
from kivy.core.text import LabelBase
from kivy.metrics import sp, dp

# 註冊中文字體
FONT_PATH = os.path.join(os.path.dirname(__file__), 'fonts', 'NotoSansTC.ttf')
if os.path.exists(FONT_PATH):
    LabelBase.register(name='NotoSansTC', fn_regular=FONT_PATH)
    DEFAULT_FONT = 'NotoSansTC'
else:
    DEFAULT_FONT = 'Roboto'


# ==================== 查詢頁面 ====================
class QueryScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(15), spacing=dp(15))
        
        with layout.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        # 頁面標題
        layout.add_widget(Label(
            text='📊 個股查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.12,
            color=(0.075, 0.925, 0.357, 1),
            bold=True
        ))
        
        # 輸入框區域
        input_box = BoxLayout(size_hint_y=0.12, spacing=dp(10))
        self.code_input = TextInput(
            hint_text='輸入股票代碼 (如: 2330)',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            multiline=False,
            size_hint_x=0.65,
            background_color=(0.15, 0.25, 0.18, 1),
            foreground_color=(1, 1, 1, 1),
            hint_text_color=(0.5, 0.5, 0.5, 1),
            padding=[dp(15), dp(12)]
        )
        input_box.add_widget(self.code_input)
        
        search_btn = Button(
            text='查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(20),
            size_hint_x=0.35,
            background_color=(0.075, 0.925, 0.357, 1),
            color=(0.063, 0.133, 0.086, 1),
            bold=True
        )
        search_btn.bind(on_press=self.on_search)
        input_box.add_widget(search_btn)
        layout.add_widget(input_box)
        
        # 結果區
        self.result_label = Label(
            text='請輸入股票代碼進行查詢\n\n支援台股上市櫃股票',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.76,
            color=(0.7, 0.7, 0.7, 1),
            halign='center',
            valign='middle'
        )
        self.result_label.bind(size=self.result_label.setter('text_size'))
        layout.add_widget(self.result_label)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def on_search(self, instance):
        code = self.code_input.text.strip()
        if code:
            self.result_label.text = f'查詢 {code} 中...\n\n(需要連接雲端才能取得資料)'
        else:
            self.result_label.text = '請輸入股票代碼'


# ==================== 掃描頁面 ====================
class ScanScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(15), spacing=dp(15))
        
        with layout.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='📈 策略掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.1,
            color=(0.075, 0.925, 0.357, 1),
            bold=True
        ))
        
        # 策略按鈕 - 更大的按鈕
        btn_layout = GridLayout(cols=2, spacing=dp(15), size_hint_y=0.5, padding=dp(5))
        
        strategies = [
            '聰明錢掃描',
            'KD 黃金交叉',
            '均線多頭',
            'VP 突破'
        ]
        
        for name in strategies:
            btn = Button(
                text=name,
                font_name=DEFAULT_FONT,
                font_size=sp(20),
                background_color=(0.1, 0.25, 0.15, 1),
                color=(1, 1, 1, 1),
                bold=True
            )
            btn.bind(on_press=lambda x, n=name: self.on_scan(n))
            btn_layout.add_widget(btn)
        
        layout.add_widget(btn_layout)
        
        self.result_label = Label(
            text='選擇策略開始掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.4,
            color=(0.7, 0.7, 0.7, 1),
            halign='center'
        )
        layout.add_widget(self.result_label)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def on_scan(self, strategy_name):
        self.result_label.text = f'執行 {strategy_name}...\n\n(需要連接雲端才能掃描)'


# ==================== 自選頁面 ====================
class WatchlistScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(15), spacing=dp(15))
        
        with layout.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='⭐ 自選股',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.1,
            color=(0.075, 0.925, 0.357, 1),
            bold=True
        ))
        
        layout.add_widget(Label(
            text='自選股清單\n\n(需要連接雲端才能同步)',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.9,
            color=(0.7, 0.7, 0.7, 1),
            halign='center'
        ))
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size


# ==================== AI 助手頁面 ====================
class AIChatScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(15), spacing=dp(15))
        
        with layout.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='🤖 AI 助手',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.1,
            color=(0.075, 0.925, 0.357, 1),
            bold=True
        ))
        
        layout.add_widget(Label(
            text='AI 股票分析助手\n\n(需要設定 Gemini API 金鑰)',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.9,
            color=(0.7, 0.7, 0.7, 1),
            halign='center'
        ))
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size


# ==================== 設定頁面 ====================
class SettingsScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(15), spacing=dp(15))
        
        with layout.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='⚙️ 設定',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.1,
            color=(0.075, 0.925, 0.357, 1),
            bold=True
        ))
        
        # 設定選項
        settings_layout = BoxLayout(orientation='vertical', size_hint_y=0.9, spacing=dp(20))
        
        settings_layout.add_widget(Label(
            text='版本: 1.0.4',
            font_name=DEFAULT_FONT,
            font_size=sp(20),
            color=(0.9, 0.9, 0.9, 1)
        ))
        
        settings_layout.add_widget(Label(
            text='狀態: 離線模式',
            font_name=DEFAULT_FONT,
            font_size=sp(20),
            color=(1, 0.8, 0.3, 1)
        ))
        
        settings_layout.add_widget(Label(
            text='\n下一步:\n• 連接 Supabase 雲端\n• 啟用股票資料同步\n• 設定 AI API 金鑰',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            color=(0.7, 0.7, 0.7, 1),
            halign='center'
        ))
        
        layout.add_widget(settings_layout)
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size


# ==================== 導航按鈕 ====================
class NavButton(Button):
    def __init__(self, icon, label, screen_name, **kwargs):
        super().__init__(**kwargs)
        self.screen_name = screen_name
        self.text = f'{icon}\n{label}'
        self.font_name = DEFAULT_FONT
        self.font_size = sp(14)
        self.halign = 'center'
        self.valign = 'middle'
        self.background_normal = ''
        self.background_color = (0.082, 0.161, 0.114, 1)
        self.color = (0.5, 0.5, 0.5, 1)
        self.is_active = False
    
    def set_active(self, active):
        self.is_active = active
        if active:
            self.color = (0.075, 0.925, 0.357, 1)
            self.bold = True
        else:
            self.color = (0.5, 0.5, 0.5, 1)
            self.bold = False


# ==================== 主 App ====================
class TWSEApp(App):
    def build(self):
        # 主容器
        root = BoxLayout(orientation='vertical')
        
        with root.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg_rect = Rectangle(pos=root.pos, size=root.size)
        root.bind(pos=self._update_bg, size=self._update_bg)
        
        # 頂部標題
        header = BoxLayout(size_hint_y=0.07, padding=[dp(15), dp(8)])
        with header.canvas.before:
            Color(0.082, 0.161, 0.114, 1)
            header.rect = Rectangle(pos=header.pos, size=header.size)
        header.bind(
            pos=lambda i, v: setattr(header.rect, 'pos', v),
            size=lambda i, v: setattr(header.rect, 'size', v)
        )
        
        header.add_widget(Label(
            text='📊 台股分析',
            font_name=DEFAULT_FONT,
            font_size=sp(22),
            color=(0.075, 0.925, 0.357, 1),
            bold=True
        ))
        root.add_widget(header)
        
        # Screen Manager
        self.sm = ScreenManager(transition=SlideTransition())
        self.sm.add_widget(QueryScreen(name='query'))
        self.sm.add_widget(ScanScreen(name='scan'))
        self.sm.add_widget(WatchlistScreen(name='watchlist'))
        self.sm.add_widget(AIChatScreen(name='ai_chat'))
        self.sm.add_widget(SettingsScreen(name='settings'))
        root.add_widget(self.sm)
        
        # 底部導航 - 更大更清晰
        nav = BoxLayout(size_hint_y=0.1, spacing=dp(2), padding=[dp(5), dp(8)])
        with nav.canvas.before:
            Color(0.082, 0.161, 0.114, 1)
            nav.rect = Rectangle(pos=nav.pos, size=nav.size)
        nav.bind(
            pos=lambda i, v: setattr(nav.rect, 'pos', v),
            size=lambda i, v: setattr(nav.rect, 'size', v)
        )
        
        nav_items = [
            ('📊', '查詢', 'query'),
            ('📈', '掃描', 'scan'),
            ('⭐', '自選', 'watchlist'),
            ('🤖', 'AI', 'ai_chat'),
            ('⚙️', '設定', 'settings'),
        ]
        
        self.nav_buttons = {}
        for icon, label, screen_name in nav_items:
            btn = NavButton(icon, label, screen_name)
            btn.bind(on_press=self.on_nav_press)
            nav.add_widget(btn)
            self.nav_buttons[screen_name] = btn
        
        self.nav_buttons['query'].set_active(True)
        root.add_widget(nav)
        
        return root
    
    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size
    
    def on_nav_press(self, instance):
        screen_name = instance.screen_name
        for name, btn in self.nav_buttons.items():
            btn.set_active(name == screen_name)
        self.sm.current = screen_name


if __name__ == '__main__':
    TWSEApp().run()
