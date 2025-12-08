"""
台股分析 App - v1.0.8
- 專業商業風格 UI
- 深藍灰色主題
- 卡片式佈局
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

# Supabase Client
try:
    from src.supabase_client import SupabaseClient
    supabase = SupabaseClient()
except ImportError:
    supabase = None

# 專業商業風格配色
COLORS = {
    'bg': (0.11, 0.12, 0.14, 1),           # 深灰藍 #1C1E24
    'card': (0.16, 0.17, 0.20, 1),          # 卡片背景 #292B33
    'header': (0.13, 0.14, 0.17, 1),        # Header #21232B
    'nav': (0.13, 0.14, 0.17, 1),           # 導航欄
    'primary': (0.25, 0.56, 0.96, 1),       # 藍色主色 #408FF5
    'accent': (0.30, 0.78, 0.55, 1),        # 綠色強調 #4DC78C
    'text': (0.95, 0.95, 0.97, 1),          # 白色文字
    'text_secondary': (0.60, 0.62, 0.68, 1),# 次要文字
    'text_dim': (0.45, 0.47, 0.52, 1),      # 灰色文字
    'button': (0.25, 0.56, 0.96, 1),        # 按鈕藍色
    'button_secondary': (0.22, 0.24, 0.28, 1),  # 次要按鈕
    'input': (0.18, 0.20, 0.24, 1),         # 輸入框
    'success': (0.30, 0.78, 0.55, 1),       # 成功綠
    'warning': (0.96, 0.73, 0.25, 1),       # 警告黃
    'error': (0.91, 0.38, 0.38, 1),         # 錯誤紅
    'up': (0.30, 0.78, 0.55, 1),            # 上漲綠
    'down': (0.91, 0.38, 0.38, 1),          # 下跌紅
}


# ==================== 查詢頁面 ====================
class QueryScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(16), spacing=dp(12))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        # 頁面標題
        title_box = BoxLayout(size_hint_y=0.08)
        title_box.add_widget(Label(
            text='個股查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(24),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        layout.add_widget(title_box)
        
        # 搜尋卡片
        search_card = BoxLayout(orientation='vertical', size_hint_y=0.18, padding=dp(12), spacing=dp(8))
        with search_card.canvas.before:
            Color(*COLORS['card'])
            search_card.rect = RoundedRectangle(pos=search_card.pos, size=search_card.size, radius=[dp(12)])
        search_card.bind(
            pos=lambda i, v: setattr(search_card.rect, 'pos', v),
            size=lambda i, v: setattr(search_card.rect, 'size', v)
        )
        
        search_card.add_widget(Label(
            text='股票代碼',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            color=COLORS['text_secondary'],
            halign='left',
            size_hint_y=0.3
        ))
        
        input_row = BoxLayout(spacing=dp(10), size_hint_y=0.7)
        self.code_input = TextInput(
            hint_text='輸入代碼 (2330)',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            multiline=False,
            size_hint_x=0.65,
            background_color=COLORS['input'],
            foreground_color=COLORS['text'],
            hint_text_color=COLORS['text_dim'],
            padding=[dp(12), dp(10)]
        )
        input_row.add_widget(self.code_input)
        
        search_btn = Button(
            text='查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            size_hint_x=0.35,
            background_normal='',
            background_color=COLORS['button'],
            color=COLORS['text'],
            bold=True
        )
        search_btn.bind(on_press=self.on_search)
        input_row.add_widget(search_btn)
        search_card.add_widget(input_row)
        layout.add_widget(search_card)
        
        # 結果卡片
        result_card = BoxLayout(orientation='vertical', size_hint_y=0.74, padding=dp(16))
        with result_card.canvas.before:
            Color(*COLORS['card'])
            result_card.rect = RoundedRectangle(pos=result_card.pos, size=result_card.size, radius=[dp(12)])
        result_card.bind(
            pos=lambda i, v: setattr(result_card.rect, 'pos', v),
            size=lambda i, v: setattr(result_card.rect, 'size', v)
        )
        
        scroll = ScrollView()
        self.result_label = Label(
            text='輸入股票代碼開始查詢',
            font_name=DEFAULT_FONT,
            font_size=sp(15),
            color=COLORS['text_secondary'],
            halign='left',
            valign='top',
            size_hint_y=None
        )
        self.result_label.bind(texture_size=self._update_label_size)
        scroll.add_widget(self.result_label)
        result_card.add_widget(scroll)
        layout.add_widget(result_card)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def _update_label_size(self, instance, value):
        instance.height = value[1]
        instance.text_size = (instance.width - dp(20), None)
    
    def on_search(self, instance):
        code = self.code_input.text.strip()
        if not code:
            self.result_label.text = '請輸入股票代碼'
            return
        
        self.result_label.text = f'正在查詢 {code}...'
        self.result_label.color = COLORS['text_secondary']
        
        if supabase:
            Clock.schedule_once(lambda dt: self._fetch_data(code), 0.1)
        else:
            self.result_label.text = '無法連接雲端服務'
            self.result_label.color = COLORS['error']
    
    def _fetch_data(self, code):
        try:
            # 取得股票基本資訊
            info = supabase.get_stock_info(code)
            data = supabase.get_stock_data(code, limit=10)
            
            if data:
                name = info.get('name', '') if info else ''
                industry = info.get('industry', '') if info else ''
                
                lines = []
                lines.append(f'{code} {name}')
                if industry:
                    lines.append(f'產業: {industry}\n')
                else:
                    lines.append('')
                
                lines.append('近期走勢:')
                lines.append('─' * 20)
                
                for row in data[:5]:
                    date = row.get('date', '')[:10]
                    close = row.get('close', 0)
                    volume = row.get('volume', 0)
                    change = row.get('change_pct', 0)
                    
                    arrow = '▲' if change >= 0 else '▼'
                    color_sign = '+' if change >= 0 else ''
                    lines.append(f'{date}')
                    lines.append(f'  收盤 ${close:,.2f}  {arrow} {color_sign}{change:.2f}%')
                    lines.append(f'  成交量 {volume//1000:,} 張')
                    lines.append('')
                
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = f'找不到 {code} 的資料'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'查詢錯誤: {str(e)}'
            self.result_label.color = COLORS['error']


# ==================== 掃描頁面 ====================
class ScanScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(16), spacing=dp(12))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        # 標題
        title_box = BoxLayout(size_hint_y=0.08)
        title_box.add_widget(Label(
            text='策略掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(24),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        layout.add_widget(title_box)
        
        # 策略按鈕卡片
        btn_card = BoxLayout(orientation='vertical', size_hint_y=0.35, padding=dp(12), spacing=dp(10))
        with btn_card.canvas.before:
            Color(*COLORS['card'])
            btn_card.rect = RoundedRectangle(pos=btn_card.pos, size=btn_card.size, radius=[dp(12)])
        btn_card.bind(
            pos=lambda i, v: setattr(btn_card.rect, 'pos', v),
            size=lambda i, v: setattr(btn_card.rect, 'size', v)
        )
        
        btn_grid = GridLayout(cols=2, spacing=dp(10))
        
        strategies = [
            ('聰明錢掃描', self.scan_smart_money, COLORS['accent']),
            ('KD 黃金交叉', self.scan_kd_golden, COLORS['button']),
            ('均線多頭', self.scan_ma_rising, COLORS['button']),
            ('VP 突破', self.scan_vp_breakout, COLORS['button'])
        ]
        
        for name, callback, color in strategies:
            btn = Button(
                text=name,
                font_name=DEFAULT_FONT,
                font_size=sp(15),
                background_normal='',
                background_color=color,
                color=COLORS['text'],
                bold=True
            )
            btn.bind(on_press=callback)
            btn_grid.add_widget(btn)
        
        btn_card.add_widget(btn_grid)
        layout.add_widget(btn_card)
        
        # 結果卡片
        result_card = BoxLayout(orientation='vertical', size_hint_y=0.57, padding=dp(16))
        with result_card.canvas.before:
            Color(*COLORS['card'])
            result_card.rect = RoundedRectangle(pos=result_card.pos, size=result_card.size, radius=[dp(12)])
        result_card.bind(
            pos=lambda i, v: setattr(result_card.rect, 'pos', v),
            size=lambda i, v: setattr(result_card.rect, 'size', v)
        )
        
        scroll = ScrollView()
        self.result_label = Label(
            text='選擇策略開始掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(15),
            color=COLORS['text_secondary'],
            halign='left',
            valign='top',
            size_hint_y=None
        )
        self.result_label.bind(texture_size=self._update_label_size)
        scroll.add_widget(self.result_label)
        result_card.add_widget(scroll)
        layout.add_widget(result_card)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def _update_label_size(self, instance, value):
        instance.height = value[1]
        instance.text_size = (instance.width - dp(20), None)
    
    def scan_smart_money(self, instance):
        self.result_label.text = '掃描聰明錢訊號中...'
        self.result_label.color = COLORS['text_secondary']
        if supabase:
            Clock.schedule_once(lambda dt: self._run_smart_money_scan(), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
            self.result_label.color = COLORS['error']
    
    def _run_smart_money_scan(self):
        try:
            data = supabase.scan_smart_money(min_volume=500, limit=10)
            if data:
                lines = ['聰明錢掃描結果', '─' * 25, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    score = row.get('smart_score', 0)
                    close = row.get('close', 0)
                    vol = row.get('volume', 0)
                    lines.append(f'{i}. {code}')
                    lines.append(f'   價格 ${close:,.0f}  評分 {score}/5  量 {vol//1000:,}張')
                    lines.append('')
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']
    
    def scan_kd_golden(self, instance):
        self.result_label.text = '掃描 KD 黃金交叉中...'
        self.result_label.color = COLORS['text_secondary']
        if supabase:
            Clock.schedule_once(lambda dt: self._run_kd_scan(), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
    
    def _run_kd_scan(self):
        try:
            data = supabase.scan_kd_golden(limit=10)
            if data:
                lines = ['KD 黃金交叉結果 (K>D, K<30)', '─' * 25, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    k = row.get('k9', 0)
                    d = row.get('d9', 0)
                    lines.append(f'{i}. {code}')
                    lines.append(f'   K: {k:.1f}  D: {d:.1f}')
                    lines.append('')
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']
    
    def scan_ma_rising(self, instance):
        self.result_label.text = '掃描均線多頭中...'
        self.result_label.color = COLORS['text_secondary']
        if supabase:
            Clock.schedule_once(lambda dt: self._run_ma_scan(), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
    
    def _run_ma_scan(self):
        try:
            data = supabase.scan_ma_rising(limit=10)
            if data:
                lines = ['均線多頭結果 (MA5>MA20>MA60)', '─' * 25, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    close = row.get('close', 0)
                    ma5 = row.get('ma5', 0)
                    ma20 = row.get('ma20', 0)
                    lines.append(f'{i}. {code}  ${close:,.0f}')
                    lines.append(f'   MA5: {ma5:.0f}  MA20: {ma20:.0f}')
                    lines.append('')
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']
    
    def scan_vp_breakout(self, instance):
        self.result_label.text = '掃描 VP 突破中...'
        self.result_label.color = COLORS['text_secondary']
        if supabase:
            Clock.schedule_once(lambda dt: self._run_vp_scan(), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
    
    def _run_vp_scan(self):
        try:
            data = supabase.scan_vp_breakout(limit=10)
            if data:
                lines = ['VP 突破結果 (價格接近VP上界)', '─' * 25, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    close = row.get('close', 0)
                    vp_high = row.get('vp_high', 0)
                    lines.append(f'{i}. {code}  ${close:,.0f}')
                    lines.append(f'   VP上界: ${vp_high:,.0f}')
                    lines.append('')
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']


# ==================== 自選頁面 ====================
class WatchlistScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(16), spacing=dp(12))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        title_box = BoxLayout(size_hint_y=0.08)
        title_box.add_widget(Label(
            text='自選股',
            font_name=DEFAULT_FONT,
            font_size=sp(24),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        layout.add_widget(title_box)
        
        # 內容卡片
        content_card = BoxLayout(orientation='vertical', size_hint_y=0.92, padding=dp(20))
        with content_card.canvas.before:
            Color(*COLORS['card'])
            content_card.rect = RoundedRectangle(pos=content_card.pos, size=content_card.size, radius=[dp(12)])
        content_card.bind(
            pos=lambda i, v: setattr(content_card.rect, 'pos', v),
            size=lambda i, v: setattr(content_card.rect, 'size', v)
        )
        
        content_card.add_widget(Label(
            text='自選股清單\n\n功能開發中...',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            color=COLORS['text_secondary'],
            halign='center'
        ))
        
        layout.add_widget(content_card)
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size


# ==================== AI 助手頁面 ====================
class AIChatScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(16), spacing=dp(12))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        title_box = BoxLayout(size_hint_y=0.08)
        title_box.add_widget(Label(
            text='AI 助手',
            font_name=DEFAULT_FONT,
            font_size=sp(24),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        layout.add_widget(title_box)
        
        content_card = BoxLayout(orientation='vertical', size_hint_y=0.92, padding=dp(20))
        with content_card.canvas.before:
            Color(*COLORS['card'])
            content_card.rect = RoundedRectangle(pos=content_card.pos, size=content_card.size, radius=[dp(12)])
        content_card.bind(
            pos=lambda i, v: setattr(content_card.rect, 'pos', v),
            size=lambda i, v: setattr(content_card.rect, 'size', v)
        )
        
        content_card.add_widget(Label(
            text='AI 股票分析助手\n\n請在設定頁面輸入\nGemini API 金鑰',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            color=COLORS['text_secondary'],
            halign='center'
        ))
        
        layout.add_widget(content_card)
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size


# ==================== 設定頁面 ====================
class SettingsScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(16), spacing=dp(12))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        title_box = BoxLayout(size_hint_y=0.08)
        title_box.add_widget(Label(
            text='設定',
            font_name=DEFAULT_FONT,
            font_size=sp(24),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        layout.add_widget(title_box)
        
        # 狀態卡片
        status_card = BoxLayout(orientation='vertical', size_hint_y=0.35, padding=dp(16), spacing=dp(10))
        with status_card.canvas.before:
            Color(*COLORS['card'])
            status_card.rect = RoundedRectangle(pos=status_card.pos, size=status_card.size, radius=[dp(12)])
        status_card.bind(
            pos=lambda i, v: setattr(status_card.rect, 'pos', v),
            size=lambda i, v: setattr(status_card.rect, 'size', v)
        )
        
        status_card.add_widget(Label(
            text='系統狀態',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            color=COLORS['text'],
            bold=True,
            size_hint_y=0.2
        ))
        
        status_card.add_widget(Label(
            text='版本: 1.0.8',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            color=COLORS['text_secondary'],
            size_hint_y=0.2
        ))
        
        self.status_label = Label(
            text='雲端: 檢查中...',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            color=COLORS['warning'],
            size_hint_y=0.2
        )
        status_card.add_widget(self.status_label)
        
        test_btn = Button(
            text='測試連線',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            size_hint_y=0.4,
            background_normal='',
            background_color=COLORS['button_secondary'],
            color=COLORS['text']
        )
        test_btn.bind(on_press=self.test_connection)
        status_card.add_widget(test_btn)
        
        layout.add_widget(status_card)
        
        # 功能卡片
        info_card = BoxLayout(orientation='vertical', size_hint_y=0.57, padding=dp(16))
        with info_card.canvas.before:
            Color(*COLORS['card'])
            info_card.rect = RoundedRectangle(pos=info_card.pos, size=info_card.size, radius=[dp(12)])
        info_card.bind(
            pos=lambda i, v: setattr(info_card.rect, 'pos', v),
            size=lambda i, v: setattr(info_card.rect, 'size', v)
        )
        
        info_card.add_widget(Label(
            text='功能說明\n\n• 個股查詢 - 搜尋股票資料\n• 策略掃描 - 4種掃描策略\n• 自選股 - 追蹤清單 (開發中)\n• AI助手 - 智慧分析 (開發中)',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            color=COLORS['text_secondary'],
            halign='left',
            valign='top'
        ))
        
        layout.add_widget(info_card)
        self.add_widget(layout)
        
        Clock.schedule_once(lambda dt: self.test_connection(None), 1)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def test_connection(self, instance):
        self.status_label.text = '雲端: 測試中...'
        self.status_label.color = COLORS['warning']
        
        if supabase:
            Clock.schedule_once(lambda dt: self._do_test(), 0.1)
        else:
            self.status_label.text = '雲端: 模組未載入'
            self.status_label.color = COLORS['error']
    
    def _do_test(self):
        try:
            if supabase.test_connection():
                self.status_label.text = '雲端: 已連線 ✓'
                self.status_label.color = COLORS['success']
            else:
                self.status_label.text = '雲端: 連線失敗'
                self.status_label.color = COLORS['error']
        except Exception as e:
            self.status_label.text = f'雲端: 錯誤'
            self.status_label.color = COLORS['error']


# ==================== 導航按鈕 ====================
class NavButton(Button):
    def __init__(self, label, screen_name, **kwargs):
        super().__init__(**kwargs)
        self.screen_name = screen_name
        self.text = label
        self.font_name = DEFAULT_FONT
        self.font_size = sp(14)
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
        
        # 頂部 Header
        header = BoxLayout(size_hint_y=0.07, padding=[dp(16), dp(10)])
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
            font_size=sp(20),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        root.add_widget(header)
        
        # Screens
        self.sm = ScreenManager(transition=SlideTransition())
        self.sm.add_widget(QueryScreen(name='query'))
        self.sm.add_widget(ScanScreen(name='scan'))
        self.sm.add_widget(WatchlistScreen(name='watchlist'))
        self.sm.add_widget(AIChatScreen(name='ai_chat'))
        self.sm.add_widget(SettingsScreen(name='settings'))
        root.add_widget(self.sm)
        
        # 底部導航
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
