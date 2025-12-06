"""
查詢畫面 - 個股查詢 + K 線圖分析
"""
from kivy.uix.screenmanager import Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.uix.widget import Widget
from kivy.uix.popup import Popup
from kivy.uix.spinner import Spinner
from kivy.graphics import Color, Rectangle
from kivy.clock import Clock
from kivy.properties import StringProperty, BooleanProperty

try:
    from src.supabase_client import SupabaseClient
    from src.chart_engine import ChartData, ChartRenderer
    from config import SUPABASE_URL, SUPABASE_KEY
except ImportError:
    SUPABASE_URL = ""
    SUPABASE_KEY = ""


class ChartWidget(Widget):
    """K 線圖 Widget"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.renderer = None
        self.bind(size=self.on_size_change, pos=self.on_size_change)
    
    def set_renderer(self, renderer):
        self.renderer = renderer
        self.redraw()
    
    def on_size_change(self, *args):
        self.redraw()
    
    def redraw(self):
        self.canvas.clear()
        with self.canvas:
            # 背景
            Color(0.063, 0.133, 0.086, 1)  # #102216
            Rectangle(pos=self.pos, size=self.size)
            
            # 圖表
            if self.renderer:
                self.canvas.add(self.renderer.render())
    
    def on_touch_down(self, touch):
        if self.collide_point(*touch.pos):
            touch.ud['start_x'] = touch.x
            # 觸發點擊事件
            if self.renderer:
                data = self.renderer.on_touch(touch.x, touch.y)
                if data and hasattr(self, 'on_crosshair_move'):
                    self.on_crosshair_move(data)
                self.redraw()
            return True
        return super().on_touch_down(touch)
    
    def on_touch_move(self, touch):
        if 'start_x' in touch.ud:
            dx = touch.x - touch.ud['start_x']
            if abs(dx) > 10 and self.renderer:
                # 拖曳捲動
                self.renderer.scroll(int(-dx / 10))
                touch.ud['start_x'] = touch.x
                self.redraw()
            elif self.renderer:
                # 查價線移動
                data = self.renderer.on_touch(touch.x, touch.y)
                if data and hasattr(self, 'on_crosshair_move'):
                    self.on_crosshair_move(data)
                self.redraw()
            return True
        return super().on_touch_move(touch)


class QueryScreen(Screen):
    """查詢畫面"""
    
    current_stock = StringProperty("")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 初始化
        self.supabase = None
        if SUPABASE_URL and SUPABASE_KEY:
            self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        self.chart_data = None
        self.chart_renderer = None
        self.current_period = 'day'  # day/week/month
        
        self._build_ui()
    
    def _build_ui(self):
        main_layout = BoxLayout(orientation='vertical', padding=5, spacing=5)
        
        # 搜尋區
        search_layout = BoxLayout(size_hint_y=0.08, spacing=5)
        
        self.search_input = TextInput(
            hint_text='輸入股票代碼或名稱...',
            multiline=False,
            size_hint_x=0.7,
            font_size=16
        )
        self.search_input.bind(on_text_validate=self.on_search)
        
        search_btn = Button(
            text='🔍 查詢',
            size_hint_x=0.3,
            background_color=(0.075, 0.925, 0.357, 1)
        )
        search_btn.bind(on_press=self.on_search)
        
        search_layout.add_widget(self.search_input)
        search_layout.add_widget(search_btn)
        main_layout.add_widget(search_layout)
        
        # 股票資訊
        self.info_label = Label(
            text='請輸入股票代碼搜尋',
            size_hint_y=0.05,
            font_size=14,
            color=(0.8, 0.8, 0.8, 1)
        )
        main_layout.add_widget(self.info_label)
        
        # 週期切換
        period_layout = BoxLayout(size_hint_y=0.06, spacing=5, padding=[0, 5])
        
        self.period_btns = {}
        for period, label in [('day', '日K'), ('week', '週K'), ('month', '月K')]:
            btn = Button(
                text=label,
                background_color=(0.2, 0.4, 0.3, 1) if period != 'day' else (0.075, 0.925, 0.357, 1)
            )
            btn.bind(on_press=lambda x, p=period: self.on_period_change(p))
            period_layout.add_widget(btn)
            self.period_btns[period] = btn
        
        main_layout.add_widget(period_layout)
        
        # K 線圖區域
        self.chart_widget = ChartWidget(size_hint_y=0.45)
        self.chart_widget.on_crosshair_move = self.on_crosshair_move
        main_layout.add_widget(self.chart_widget)
        
        # 均線控制
        ma_layout = BoxLayout(size_hint_y=0.06, spacing=2, padding=[0, 5])
        
        self.ma_btns = {}
        for period in [3, 20, 60, 120, 200]:
            btn = Button(
                text=f'MA{period}',
                font_size=12,
                background_color=(0.3, 0.3, 0.3, 1)
            )
            btn.bind(on_press=lambda x, p=period: self.on_toggle_ma(p))
            ma_layout.add_widget(btn)
            self.ma_btns[period] = btn
        
        # +自訂均線
        custom_ma_btn = Button(
            text='+自訂',
            font_size=12,
            background_color=(0.2, 0.4, 0.3, 1)
        )
        custom_ma_btn.bind(on_press=self.on_add_custom_ma)
        ma_layout.add_widget(custom_ma_btn)
        
        main_layout.add_widget(ma_layout)
        
        # 疊加線控制
        overlay_layout = BoxLayout(size_hint_y=0.06, spacing=2, padding=[0, 5])
        
        self.overlay_btns = {}
        for key, label in [('vp_upper', 'VP上'), ('vp_lower', 'VP下'), 
                           ('poc', 'POC'), ('vwap', 'VWAP')]:
            btn = Button(
                text=label,
                font_size=12,
                background_color=(0.3, 0.3, 0.3, 1)
            )
            btn.bind(on_press=lambda x, k=key: self.on_toggle_overlay(k))
            overlay_layout.add_widget(btn)
            self.overlay_btns[key] = btn
        
        main_layout.add_widget(overlay_layout)
        
        # 指標選擇
        indicator_layout = BoxLayout(size_hint_y=0.06, spacing=5)
        
        indicator_label = Label(
            text='指標:',
            size_hint_x=0.2,
            font_size=14
        )
        indicator_layout.add_widget(indicator_label)
        
        self.indicator_spinner = Spinner(
            text='KD',
            values=['KD', 'MACD', 'RSI', 'MFI'],
            size_hint_x=0.4
        )
        self.indicator_spinner.bind(text=self.on_indicator_change)
        indicator_layout.add_widget(self.indicator_spinner)
        
        # 加入自選按鈕
        add_watch_btn = Button(
            text='⭐ 加入自選',
            size_hint_x=0.4,
            background_color=(0.8, 0.6, 0.2, 1)
        )
        add_watch_btn.bind(on_press=self.on_add_to_watchlist)
        indicator_layout.add_widget(add_watch_btn)
        
        main_layout.add_widget(indicator_layout)
        
        # 股票詳情
        self.detail_scroll = ScrollView(size_hint_y=0.18)
        self.detail_layout = BoxLayout(orientation='vertical', size_hint_y=None)
        self.detail_layout.bind(minimum_height=self.detail_layout.setter('height'))
        self.detail_scroll.add_widget(self.detail_layout)
        main_layout.add_widget(self.detail_scroll)
        
        self.add_widget(main_layout)
    
    def on_search(self, *args):
        """執行搜尋"""
        query = self.search_input.text.strip()
        if not query:
            return
        
        self.info_label.text = f'搜尋中: {query}...'
        
        # 模擬搜尋 (實際應從資料庫查詢)
        Clock.schedule_once(lambda dt: self._do_search(query), 0.1)
    
    def _do_search(self, query):
        """執行搜尋邏輯"""
        try:
            # TODO: 從 Supabase 查詢股票資料
            # 目前使用模擬資料
            
            self.current_stock = query
            self.info_label.text = f'{query} - 模擬數據'
            
            # 載入 K 線數據
            self._load_chart_data(query)
            
        except Exception as e:
            self.info_label.text = f'搜尋失敗: {str(e)}'
    
    def _load_chart_data(self, stock_code):
        """載入 K 線數據"""
        # TODO: 從資料庫載入真實數據
        # 目前使用模擬數據
        
        self.chart_data = ChartData()
        
        # 模擬數據
        import random
        dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        
        base_price = 100
        for i in range(100):
            dates.append(f'2024-{(i//30)+1:02d}-{(i%30)+1:02d}')
            o = base_price + random.uniform(-2, 2)
            c = o + random.uniform(-5, 5)
            h = max(o, c) + random.uniform(0, 2)
            l = min(o, c) - random.uniform(0, 2)
            
            opens.append(o)
            closes.append(c)
            highs.append(h)
            lows.append(l)
            volumes.append(random.randint(100000, 1000000))
            
            base_price = c
        
        self.chart_data.dates = dates
        self.chart_data.opens = opens
        self.chart_data.closes = closes
        self.chart_data.highs = highs
        self.chart_data.lows = lows
        self.chart_data.volumes = volumes
        
        # 計算均線
        for period in [3, 5, 10, 20, 60, 120, 200]:
            ma_values = []
            for i in range(len(closes)):
                if i < period - 1:
                    ma_values.append(0)
                else:
                    ma_values.append(sum(closes[i-period+1:i+1]) / period)
            self.chart_data.set_ma(period, ma_values)
        
        # 建立渲染器
        self.chart_renderer = ChartRenderer(self.chart_widget)
        self.chart_renderer.set_data(self.chart_data)
        
        # 設定預設開啟的均線
        self.chart_renderer.ma_enabled[20] = True
        self.chart_renderer.ma_enabled[60] = True
        
        # 更新按鈕狀態
        self._update_ma_btn_states()
        
        # 繪製
        self.chart_widget.set_renderer(self.chart_renderer)
    
    def _update_ma_btn_states(self):
        """更新均線按鈕狀態"""
        if not self.chart_renderer:
            return
        
        for period, btn in self.ma_btns.items():
            if self.chart_renderer.ma_enabled.get(period, False):
                btn.background_color = (0.075, 0.925, 0.357, 1)
            else:
                btn.background_color = (0.3, 0.3, 0.3, 1)
    
    def _update_overlay_btn_states(self):
        """更新疊加線按鈕狀態"""
        if not self.chart_renderer:
            return
        
        for key, btn in self.overlay_btns.items():
            if self.chart_renderer.overlay_enabled.get(key, False):
                btn.background_color = (0.075, 0.925, 0.357, 1)
            else:
                btn.background_color = (0.3, 0.3, 0.3, 1)
    
    def on_period_change(self, period):
        """切換週期"""
        self.current_period = period
        
        # 更新按鈕狀態
        for p, btn in self.period_btns.items():
            if p == period:
                btn.background_color = (0.075, 0.925, 0.357, 1)
            else:
                btn.background_color = (0.2, 0.4, 0.3, 1)
        
        # 重新載入數據
        if self.current_stock:
            self._load_chart_data(self.current_stock)
    
    def on_toggle_ma(self, period):
        """切換均線顯示"""
        if self.chart_renderer:
            self.chart_renderer.toggle_ma(period)
            self._update_ma_btn_states()
            self.chart_widget.redraw()
    
    def on_toggle_overlay(self, key):
        """切換疊加線顯示"""
        if self.chart_renderer:
            self.chart_renderer.toggle_overlay(key)
            self._update_overlay_btn_states()
            self.chart_widget.redraw()
    
    def on_indicator_change(self, spinner, text):
        """切換技術指標"""
        if self.chart_renderer:
            self.chart_renderer.current_indicator = text
            self.chart_widget.redraw()
    
    def on_add_custom_ma(self, *args):
        """新增自訂均線"""
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        period_input = TextInput(
            hint_text='輸入天數 (1-500)',
            multiline=False,
            input_filter='int'
        )
        content.add_widget(period_input)
        
        # 快速選擇
        quick_select = BoxLayout(spacing=5, size_hint_y=None, height=30)
        for p in [3, 20, 60, 120, 200]:
            btn = Button(text=str(p), font_size=11, background_color=(0.3, 0.3, 0.3, 1))
            btn.bind(on_press=lambda x, val=str(p): setattr(period_input, 'text', val))
            quick_select.add_widget(btn)
        content.add_widget(quick_select)
        
        colors = BoxLayout(spacing=5)
        color_btns = []
        for color, rgba in [('藍', (0.2, 0.6, 1, 1)), ('黃', (1, 0.84, 0, 1)),
                            ('紫', (0.6, 0.4, 1, 1)), ('橙', (1, 0.5, 0, 1))]:
            btn = Button(text=color, background_color=rgba)
            colors.add_widget(btn)
            color_btns.append((btn, rgba))
        content.add_widget(colors)
        
        popup = Popup(title='新增自訂均線', content=content, size_hint=(0.8, 0.4))
        popup.open()
    
    def on_add_to_watchlist(self, *args):
        """加入自選股"""
        if not self.current_stock:
            return
        
        # TODO: 實際加入自選股邏輯
        self.info_label.text = f'已將 {self.current_stock} 加入自選'
    
    def load_stock(self, stock_code: str, stock_name: str = ""):
        """外部呼叫: 載入指定股票"""
        self.search_input.text = stock_code
        self.current_stock = stock_code
        self.info_label.text = f'{stock_code} {stock_name}'
        self._load_chart_data(stock_code)

    def on_crosshair_move(self, data):
        """當查價線移動時更新資訊"""
        if not data:
            return
            
        # 格式化顯示資訊
        date = data['date']
        o = data['open']
        h = data['high']
        l = data['low']
        c = data['close']
        v = data['volume']
        
        # 漲跌顏色
        color = 'ff3a3a' if c >= o else '3aba59'  # 紅/綠
        
        info_text = (
            f'[color=cccccc]{date}[/color]  '
            f'開:{o:.2f}  高:{h:.2f}  低:{l:.2f}  '
            f'收:[color={color}]{c:.2f}[/color]  '
            f'量:{v:,}'
        )
        
        # 更新標籤 (需支援 markup)
        self.info_label.markup = True
        self.info_label.text = info_text
