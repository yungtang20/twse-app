"""
台股分析 App - v1.0.6
- 加入 Supabase 雲端連線
- 查詢/掃描功能可取得真實資料
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
from kivy.graphics import Color, Rectangle
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

# Supabase Client
try:
    from src.supabase_client import SupabaseClient
    supabase = SupabaseClient()
except ImportError:
    supabase = None

# 顏色設定 - 黑色主題
COLORS = {
    'bg': (0.05, 0.05, 0.05, 1),
    'header': (0.1, 0.1, 0.1, 1),
    'nav': (0.08, 0.08, 0.08, 1),
    'primary': (0.075, 0.925, 0.357, 1),
    'text': (0.9, 0.9, 0.9, 1),
    'text_dim': (0.5, 0.5, 0.5, 1),
    'button': (0.15, 0.15, 0.15, 1),
    'input': (0.12, 0.12, 0.12, 1),
    'success': (0.2, 0.8, 0.4, 1),
    'warning': (1, 0.8, 0.3, 1),
    'error': (1, 0.4, 0.4, 1),
}


# ==================== 查詢頁面 ====================
class QueryScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(20), spacing=dp(15))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='個股查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.08,
            color=COLORS['primary'],
            bold=True
        ))
        
        input_box = BoxLayout(size_hint_y=0.1, spacing=dp(10))
        self.code_input = TextInput(
            hint_text='輸入股票代碼 (如: 2330)',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            multiline=False,
            size_hint_x=0.65,
            background_color=COLORS['input'],
            foreground_color=COLORS['text'],
            hint_text_color=COLORS['text_dim'],
            padding=[dp(15), dp(12)]
        )
        input_box.add_widget(self.code_input)
        
        search_btn = Button(
            text='查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(20),
            size_hint_x=0.35,
            background_color=COLORS['primary'],
            color=(0, 0, 0, 1),
            bold=True
        )
        search_btn.bind(on_press=self.on_search)
        input_box.add_widget(search_btn)
        layout.add_widget(input_box)
        
        # 結果區 - 使用 ScrollView
        scroll = ScrollView(size_hint_y=0.82)
        self.result_label = Label(
            text='請輸入股票代碼進行查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            color=COLORS['text_dim'],
            halign='left',
            valign='top',
            size_hint_y=None,
            text_size=(None, None)
        )
        self.result_label.bind(texture_size=self._update_label_size)
        scroll.add_widget(self.result_label)
        layout.add_widget(scroll)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def _update_label_size(self, instance, value):
        instance.height = value[1]
        instance.text_size = (instance.width, None)
    
    def on_search(self, instance):
        code = self.code_input.text.strip()
        if not code:
            self.result_label.text = '請輸入股票代碼'
            return
        
        self.result_label.text = f'查詢 {code} 中...'
        
        if supabase:
            Clock.schedule_once(lambda dt: self._fetch_data(code), 0.1)
        else:
            self.result_label.text = '無法連接雲端服務'
    
    def _fetch_data(self, code):
        try:
            data = supabase.get_stock_data(code, limit=10)
            if data:
                lines = [f'股票代碼: {code}\n']
                for row in data[:5]:
                    date = row.get('date', '')
                    close = row.get('close', 0)
                    volume = row.get('volume', 0)
                    change = row.get('change_pct', 0)
                    lines.append(f'{date}: ${close:.2f} ({change:+.2f}%) 量:{volume//1000}張')
                self.result_label.text = '\n'.join(lines)
            else:
                self.result_label.text = f'找不到 {code} 的資料'
        except Exception as e:
            self.result_label.text = f'查詢錯誤: {str(e)}'


# ==================== 掃描頁面 ====================
class ScanScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(20), spacing=dp(15))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='策略掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.08,
            color=COLORS['primary'],
            bold=True
        ))
        
        btn_layout = GridLayout(cols=2, spacing=dp(15), size_hint_y=0.35, padding=dp(5))
        
        strategies = [
            ('聰明錢掃描', self.scan_smart_money),
            ('KD 黃金交叉', self.scan_kd_golden),
            ('均線多頭', self.scan_ma_rising),
            ('VP 突破', self.scan_vp_breakout)
        ]
        
        for name, callback in strategies:
            btn = Button(
                text=name,
                font_name=DEFAULT_FONT,
                font_size=sp(18),
                background_color=COLORS['button'],
                color=COLORS['text'],
                bold=True
            )
            btn.bind(on_press=callback)
            btn_layout.add_widget(btn)
        
        layout.add_widget(btn_layout)
        
        # 結果區
        scroll = ScrollView(size_hint_y=0.57)
        self.result_label = Label(
            text='選擇策略開始掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            color=COLORS['text_dim'],
            halign='left',
            valign='top',
            size_hint_y=None
        )
        self.result_label.bind(texture_size=self._update_label_size)
        scroll.add_widget(self.result_label)
        layout.add_widget(scroll)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def _update_label_size(self, instance, value):
        instance.height = value[1]
        instance.text_size = (instance.width, None)
    
    def scan_smart_money(self, instance):
        self.result_label.text = '掃描聰明錢訊號中...'
        if supabase:
            Clock.schedule_once(lambda dt: self._run_smart_money_scan(), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
    
    def _run_smart_money_scan(self):
        try:
            data = supabase.scan_smart_money(min_volume=500, limit=10)
            if data:
                lines = ['聰明錢掃描結果:\n']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    score = row.get('smart_score', 0)
                    close = row.get('close', 0)
                    vol = row.get('volume', 0)
                    lines.append(f'{i}. {code} - ${close:.0f} 評分:{score} 量:{vol//1000}張')
                self.result_label.text = '\n'.join(lines)
            else:
                self.result_label.text = '沒有符合條件的股票'
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
    
    def scan_kd_golden(self, instance):
        self.result_label.text = '掃描 KD 黃金交叉中...'
        if supabase:
            Clock.schedule_once(lambda dt: self._run_kd_scan(), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
    
    def _run_kd_scan(self):
        try:
            data = supabase.scan_kd_golden(limit=10)
            if data:
                lines = ['KD 黃金交叉結果:\n']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    k = row.get('k9', 0)
                    d = row.get('d9', 0)
                    lines.append(f'{i}. {code} - K:{k:.1f} D:{d:.1f}')
                self.result_label.text = '\n'.join(lines)
            else:
                self.result_label.text = '沒有符合條件的股票'
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
    
    def scan_ma_rising(self, instance):
        self.result_label.text = '均線多頭掃描\n\n(功能開發中)'
    
    def scan_vp_breakout(self, instance):
        self.result_label.text = 'VP 突破掃描\n\n(功能開發中)'


# ==================== 自選頁面 ====================
class WatchlistScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(20), spacing=dp(15))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='自選股',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.1,
            color=COLORS['primary'],
            bold=True
        ))
        
        layout.add_widget(Label(
            text='自選股清單\n\n(功能開發中)',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.9,
            color=COLORS['text_dim'],
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
        layout = BoxLayout(orientation='vertical', padding=dp(20), spacing=dp(15))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='AI 助手',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.1,
            color=COLORS['primary'],
            bold=True
        ))
        
        layout.add_widget(Label(
            text='AI 股票分析助手\n\n(需要設定 Gemini API 金鑰)',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.9,
            color=COLORS['text_dim'],
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
        layout = BoxLayout(orientation='vertical', padding=dp(20), spacing=dp(15))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        layout.add_widget(Label(
            text='設定',
            font_name=DEFAULT_FONT,
            font_size=sp(28),
            size_hint_y=0.08,
            color=COLORS['primary'],
            bold=True
        ))
        
        settings_layout = BoxLayout(orientation='vertical', size_hint_y=0.92, spacing=dp(15))
        
        settings_layout.add_widget(Label(
            text='版本: 1.0.6',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            color=COLORS['text']
        ))
        
        # 連線狀態
        self.status_label = Label(
            text='雲端狀態: 檢查中...',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            color=COLORS['warning']
        )
        settings_layout.add_widget(self.status_label)
        
        # 測試按鈕
        test_btn = Button(
            text='測試雲端連線',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            size_hint_y=0.15,
            background_color=COLORS['button'],
            color=COLORS['text']
        )
        test_btn.bind(on_press=self.test_connection)
        settings_layout.add_widget(test_btn)
        
        settings_layout.add_widget(Label(
            text='\n功能:\n- 連接 Supabase 雲端\n- 取得即時股票資料\n- 執行策略掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            color=COLORS['text_dim'],
            halign='center'
        ))
        
        layout.add_widget(settings_layout)
        self.add_widget(layout)
        
        # 啟動時檢查連線
        Clock.schedule_once(lambda dt: self.test_connection(None), 1)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def test_connection(self, instance):
        self.status_label.text = '雲端狀態: 測試中...'
        self.status_label.color = COLORS['warning']
        
        if supabase:
            Clock.schedule_once(lambda dt: self._do_test(), 0.1)
        else:
            self.status_label.text = '雲端狀態: 模組未載入'
            self.status_label.color = COLORS['error']
    
    def _do_test(self):
        try:
            if supabase.test_connection():
                self.status_label.text = '雲端狀態: 已連線'
                self.status_label.color = COLORS['success']
            else:
                self.status_label.text = '雲端狀態: 連線失敗'
                self.status_label.color = COLORS['error']
        except Exception as e:
            self.status_label.text = f'雲端狀態: {str(e)[:20]}'
            self.status_label.color = COLORS['error']


# ==================== 導航按鈕 ====================
class NavButton(Button):
    def __init__(self, label, screen_name, **kwargs):
        super().__init__(**kwargs)
        self.screen_name = screen_name
        self.text = label
        self.font_name = DEFAULT_FONT
        self.font_size = sp(16)
        self.halign = 'center'
        self.valign = 'middle'
        self.background_normal = ''
        self.background_color = COLORS['nav']
        self.color = COLORS['text_dim']
        self.is_active = False
    
    def set_active(self, active):
        self.is_active = active
        if active:
            self.color = COLORS['primary']
            self.bold = True
        else:
            self.color = COLORS['text_dim']
            self.bold = False


# ==================== 主 App ====================
class TWSEApp(App):
    def build(self):
        root = BoxLayout(orientation='vertical')
        
        with root.canvas.before:
            Color(*COLORS['bg'])
            self.bg_rect = Rectangle(pos=root.pos, size=root.size)
        root.bind(pos=self._update_bg, size=self._update_bg)
        
        header = BoxLayout(size_hint_y=0.07, padding=[dp(15), dp(10)])
        with header.canvas.before:
            Color(*COLORS['header'])
            header.rect = Rectangle(pos=header.pos, size=header.size)
        header.bind(
            pos=lambda i, v: setattr(header.rect, 'pos', v),
            size=lambda i, v: setattr(header.rect, 'size', v)
        )
        
        header.add_widget(Label(
            text='台股分析',
            font_name=DEFAULT_FONT,
            font_size=sp(24),
            color=COLORS['primary'],
            bold=True
        ))
        root.add_widget(header)
        
        self.sm = ScreenManager(transition=SlideTransition())
        self.sm.add_widget(QueryScreen(name='query'))
        self.sm.add_widget(ScanScreen(name='scan'))
        self.sm.add_widget(WatchlistScreen(name='watchlist'))
        self.sm.add_widget(AIChatScreen(name='ai_chat'))
        self.sm.add_widget(SettingsScreen(name='settings'))
        root.add_widget(self.sm)
        
        nav = BoxLayout(size_hint_y=0.08, spacing=dp(1))
        with nav.canvas.before:
            Color(*COLORS['nav'])
            nav.rect = Rectangle(pos=nav.pos, size=nav.size)
        nav.bind(
            pos=lambda i, v: setattr(nav.rect, 'pos', v),
            size=lambda i, v: setattr(nav.rect, 'size', v)
        )
        
        nav_items = [
            ('查詢', 'query'),
            ('掃描', 'scan'),
            ('自選', 'watchlist'),
            ('AI', 'ai_chat'),
            ('設定', 'settings'),
        ]
        
        self.nav_buttons = {}
        for label, screen_name in nav_items:
            btn = NavButton(label, screen_name)
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
