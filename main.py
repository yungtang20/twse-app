"""
台股分析 App - v1.2.3 (UI/UX 升級版)
- 專業商業風格 UI
- 深藍灰色主題
- 卡片式佈局
- 插件系統整合
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
from kivy.uix.widget import Widget
from kivy.graphics import Color, Rectangle, RoundedRectangle, Line
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

# Plugin Manager
try:
    from src.plugin_engine import PluginManager
    plugin_manager = PluginManager(os.path.join(os.path.dirname(__file__), 'data'))
except ImportError:
    plugin_manager = None

# 載入本地股票清單 (修復股名顯示)
STOCKS_MAP = {}
try:
    with open(os.path.join(os.path.dirname(__file__), 'data', 'stocks.json'), 'r', encoding='utf-8') as f:
        import json
        STOCKS_MAP = json.load(f)
except Exception as e:
    print(f"Error loading stocks.json: {e}")

def get_stock_name(code):
    """獲取股票名稱 (優先查本地，失敗查 Supabase)"""
    if code in STOCKS_MAP:
        return STOCKS_MAP[code]
    if supabase:
        info = supabase.get_stock_info(code)
        if info:
            return info.get('name', code)
    return code

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
                name = info.get('name', code) if info else code
                
                lines = []
                # 最新一筆資料 (模擬即時股價)
                latest = data[0] if data else {}
                date_str = latest.get('date', '')[:10] if latest.get('date') else ''
                close = latest.get('close', 0) or 0
                open_p = latest.get('open', close) or close
                high = latest.get('high', close) or close
                low = latest.get('low', close) or close
                volume = latest.get('volume', 0) or 0
                
                # === 即時股價區 ===
                lines.append(f'=== {date_str} {name} ({code}) ===')
                lines.append(f'目前股價: {close:,.2f}')
                lines.append(f'開盤: {open_p:,.2f}  最高: {high:,.2f}  最低: {low:,.2f}')
                lines.append(f'成交量: {volume//1000:,} 張')
                lines.append('=' * 32)
                lines.append('')
                
                # === 近期走勢區 (完整 5 行格式) ===
                lines.append(f'【{name} {code}】近期走勢:')
                lines.append('═' * 32)
                
                for i, row in enumerate(data[:5]):
                    date = row.get('date', '')[:10]
                    r_close = row.get('close', 0) or 0
                    r_volume = row.get('volume', 0) or 0
                    
                    # 指標欄位
                    r_mfi = row.get('mfi14') or row.get('mfi') or 0
                    r_svi = row.get('svi') or 0
                    r_vwap = row.get('vwap20') or row.get('vwap') or 0
                    r_poc = row.get('vp_poc') or row.get('poc') or 0
                    r_ma3 = row.get('ma3') or 0
                    r_ma20 = row.get('ma20') or 0
                    r_ma60 = row.get('ma60') or 0
                    r_ma120 = row.get('ma120') or 0
                    r_ma200 = row.get('ma200') or 0
                    r_score = row.get('smart_score') or 0
                    
                    # 計算漲跌幅
                    change = 0
                    if i + 1 < len(data):
                        prev_close = data[i + 1].get('close', 0) or 0
                        if prev_close > 0:
                            change = ((r_close - prev_close) / prev_close) * 100
                    
                    # 量比計算
                    vol_ratio = 1.0
                    if i + 1 < len(data):
                        prev_vol = data[i + 1].get('volume', 0) or 0
                        if prev_vol > 0:
                            vol_ratio = r_volume / prev_vol
                    
                    # 止盈止損計算 (簡單版: 止盈+10%, 止損-3%)
                    take_profit = r_close * 1.10 if r_close else 0
                    stop_loss = r_poc * 0.97 if r_poc else r_close * 0.97
                    
                    # 箭頭與符號
                    arrow = '▲' if change >= 0 else '▼'
                    sign = '+' if change >= 0 else ''
                    mfi_arrow = '↑' if r_mfi > 50 else '↓'
                    vwap_arrow = '↑' if r_close > r_vwap else '↓' if r_vwap else ''
                    
                    # 建立訊號列表
                    signals = []
                    if change > 0 and vol_ratio > 1:
                        signals.append('價漲量增')
                    if r_score >= 4:
                        signals.append('主力進場')
                    if r_ma3 > r_ma20 > r_ma60:
                        signals.append('多頭排列')
                    if r_close > r_poc and r_poc > 0:
                        signals.append('POC支撐')
                    signal_str = ','.join(signals) if signals else '觀望'
                    
                    # Line 1: 日期/成交量/MFI/SVI
                    svi_str = f'(SVI:{sign}{r_svi:.1f}%)' if r_svi else ''
                    lines.append(f'{date} {name}({code})')
                    lines.append(f'成交量:{r_volume//1000:,}張({vol_ratio:.1f}x) MFI:{r_mfi:.1f}{mfi_arrow} {svi_str}')
                    
                    # Line 2: 收盤價/漲跌幅
                    lines.append(f'收盤:{r_close:,.2f}({sign}{change:.2f}%) {arrow}')
                    
                    # Line 3: 止盈/VWAP/POC/止損
                    if r_vwap or r_poc:
                        lines.append(f'止盈:{take_profit:,.2f} VWAP:{r_vwap:,.2f}{vwap_arrow} POC:{r_poc:,.2f} 止損:{stop_loss:,.2f}')
                    
                    # Line 4: 訊號
                    lines.append(f'訊號{r_score}/6:[{signal_str}]')
                    
                    # Line 5: MA均線
                    if r_ma20 or r_ma60:
                        lines.append(f'MA3:{r_ma3:,.2f} MA20:{r_ma20:,.2f} MA60:{r_ma60:,.2f} MA120:{r_ma120:,.2f}')
                    
                    lines.append('─' * 32)
                
                lines.append('═' * 32)
                
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
        from kivy.uix.spinner import Spinner
        
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
        
        # 設定卡片 (下拉選單 + 參數)
        settings_card = BoxLayout(orientation='vertical', size_hint_y=0.28, padding=dp(12), spacing=dp(8))
        with settings_card.canvas.before:
            Color(*COLORS['card'])
            settings_card.rect = RoundedRectangle(pos=settings_card.pos, size=settings_card.size, radius=[dp(12)])
        settings_card.bind(
            pos=lambda i, v: setattr(settings_card.rect, 'pos', v),
            size=lambda i, v: setattr(settings_card.rect, 'size', v)
        )
        
        # 下拉選單行
        spinner_row = BoxLayout(spacing=dp(10), size_hint_y=0.4)
        spinner_row.add_widget(Label(
            text='策略:',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            color=COLORS['text'],
            size_hint_x=0.2
        ))
        
        # 自訂 Spinner 選項類別 (修復中文字體)
        from kivy.uix.spinner import SpinnerOption
        class ChineseSpinnerOption(SpinnerOption):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.font_name = DEFAULT_FONT
                self.font_size = sp(14)
                self.background_normal = ''
                self.background_color = COLORS['card']
                self.color = COLORS['text']
        
        # 從插件系統動態載入掃描選項
        scan_options = self._load_plugin_options()
        self.strategy_spinner = Spinner(
            text=scan_options[0] if scan_options else '聰明錢 (6分制)',
            values=scan_options,
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            size_hint_x=0.8,
            background_normal='',
            background_color=COLORS['button'],
            color=COLORS['text'],
            option_cls=ChineseSpinnerOption  # 套用自訂選項類別
        )
        spinner_row.add_widget(self.strategy_spinner)
        settings_card.add_widget(spinner_row)
        
        # 參數行
        params_row = BoxLayout(spacing=dp(10), size_hint_y=0.3)
        
        params_row.add_widget(Label(
            text='量≥',
            font_name=DEFAULT_FONT,
            font_size=sp(12),
            color=COLORS['text_secondary'],
            size_hint_x=0.15
        ))
        self.volume_input = TextInput(
            text='500',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            multiline=False,
            input_filter='int',
            size_hint_x=0.25,
            background_color=COLORS['input'],
            foreground_color=COLORS['text'],
            padding=[dp(8), dp(6)]
        )
        params_row.add_widget(self.volume_input)
        
        params_row.add_widget(Label(
            text='檔數:',
            font_name=DEFAULT_FONT,
            font_size=sp(12),
            color=COLORS['text_secondary'],
            size_hint_x=0.2
        ))
        self.limit_input = TextInput(
            text='20',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            multiline=False,
            input_filter='int',
            size_hint_x=0.2,
            background_color=COLORS['input'],
            foreground_color=COLORS['text'],
            padding=[dp(8), dp(6)]
        )
        params_row.add_widget(self.limit_input)
        
        scan_btn = Button(
            text='掃描',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            size_hint_x=0.2,
            background_normal='',
            background_color=COLORS['accent'],
            color=COLORS['text'],
            bold=True
        )
        scan_btn.bind(on_press=self.execute_scan)
        params_row.add_widget(scan_btn)
        settings_card.add_widget(params_row)
        
        layout.add_widget(settings_card)
        
        # 結果卡片
        result_card = BoxLayout(orientation='vertical', size_hint_y=0.64, padding=dp(16))
        with result_card.canvas.before:
            Color(*COLORS['card'])
            result_card.rect = RoundedRectangle(pos=result_card.pos, size=result_card.size, radius=[dp(12)])
        result_card.bind(
            pos=lambda i, v: setattr(result_card.rect, 'pos', v),
            size=lambda i, v: setattr(result_card.rect, 'size', v)
        )
        
        scroll = ScrollView()
        self.result_label = Label(
            text='選擇策略並點擊「掃描」',
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
    
    def _load_plugin_options(self):
        """從插件系統動態載入掃描選項"""
        default_options = [
            '聰明錢 (6分制)',
            '月KD交叉',
            '均線多頭',
            'VP 突破',
            'MFI 超賣',
            '三重篩選',
            'KD 日線'
        ]
        
        if plugin_manager is None:
            return default_options
        
        try:
            plugins = plugin_manager.get_enabled_plugins()
            if plugins:
                return [p.get('name', p.get('id', 'Unknown')) for p in plugins]
        except Exception as e:
            print(f"[ScanScreen] 載入插件失敗: {e}")
        
        return default_options
    
    def execute_scan(self, instance):
        """統一掃描入口 - 根據下拉選單選擇執行對應策略"""
        strategy = self.strategy_spinner.text
        try:
            min_vol = int(self.volume_input.text) if self.volume_input.text else 500
            limit = int(self.limit_input.text) if self.limit_input.text else 20
        except ValueError:
            min_vol, limit = 500, 20
        
        self.result_label.text = f'掃描 {strategy} 中...'
        self.result_label.color = COLORS['text_secondary']
        
        if not supabase:
            self.result_label.text = '無法連接雲端'
            self.result_label.color = COLORS['error']
            return
        
        # 根據策略執行對應掃描
        if '聰明錢' in strategy:
            Clock.schedule_once(lambda dt: self._run_smart_money_scan(min_vol, limit), 0.1)
        elif 'KD交叉' in strategy or '月KD' in strategy:
            Clock.schedule_once(lambda dt: self._run_kd_monthly_scan(limit), 0.1)
        elif '均線' in strategy:
            Clock.schedule_once(lambda dt: self._run_ma_scan(limit), 0.1)
        elif 'VP' in strategy:
            Clock.schedule_once(lambda dt: self._run_vp_scan(limit), 0.1)
        elif 'MFI' in strategy:
            Clock.schedule_once(lambda dt: self._run_mfi_scan(limit), 0.1)
        elif '三重' in strategy:
            Clock.schedule_once(lambda dt: self._run_triple_filter_scan(limit), 0.1)
        elif 'KD 日線' in strategy:
            Clock.schedule_once(lambda dt: self._run_kd_scan(limit), 0.1)
        else:
            self.result_label.text = '未知策略'
            self.result_label.color = COLORS['warning']
    
    def scan_smart_money(self, instance):
        self.result_label.text = '掃描聰明錢訊號中...'
        self.result_label.color = COLORS['text_secondary']
        if supabase:
            Clock.schedule_once(lambda dt: self._run_smart_money_scan(500, 10), 0.1)
        else:
            self.result_label.text = '無法連接雲端'
            self.result_label.color = COLORS['error']
    
    def _format_scan_result_item(self, index, row, name):
        """格式化單筆掃描結果 (顯示一格式)"""
        code = row.get('code', '')
        date = row.get('date', '')[:10] if row.get('date') else ''
        close = row.get('close', 0) or 0
        vol = row.get('volume', 0) or 0
        score = row.get('smart_score', 0)
        
        # 計算漲跌幅與顏色
        prev_close = row.get('close_prev') or close
        chg = (close - prev_close) / prev_close * 100 if prev_close else 0
        color_hex = "ff5252" if chg > 0 else "00e676" if chg < 0 else "ffffff"
        arrow = "▲" if chg > 0 else "▼" if chg < 0 else ""
        
        # 輔助數據
        mfi = row.get('MFI', 0) or 0
        svi = row.get('SVI', 0) or 0
        ma3 = row.get('MA3', 0) or 0
        ma20 = row.get('MA20', 0) or 0
        ma60 = row.get('MA60', 0) or 0
        ma120 = row.get('MA120', 0) or 0
        ma200 = row.get('MA200', 0) or 0
        
        # 訊號列表 (模擬)
        signals = []
        if chg > 0 and vol > 1000: signals.append("價漲量增")
        if score >= 5: signals.append("主力進場")
        if ma3 > ma20 > ma60: signals.append("多頭排列")
        if svi > 20: signals.append("籌碼鎖定")
        signal_str = ",".join(signals)
        
        return f"""{index}. {name} ({code})        Score:{score}
   {date} {name}({code}) 成交量:{vol:,}張 MFI:{mfi:.1f} (SVI:{svi:+.1f}%)
   收盤價:[color={color_hex}]{close:.2f}({chg:+.2f}%)[/color] {arrow}
   止盈:{close*1.1:.2f}   VWAP:{row.get('VWAP',0):.2f}   POC:{row.get('POC',0):.2f}   止損:{close*0.9:.2f}
   訊號{len(signals)}/4:[{signal_str}]
   MA3:{ma3:.2f} MA20:{ma20:.2f} MA60:{ma60:.2f} MA120:{ma120:.2f} MA200:{ma200:.2f}
"""

    def _run_smart_money_scan(self, min_vol=500, limit=10):
        try:
            data = supabase.scan_smart_money(min_volume=min_vol, limit=limit)
            if data:
                # 取得股票名稱 (優先查本地)
                codes = [row.get('code', '') for row in data]
                names = {c: get_stock_name(c) for c in codes}
                
                lines = [f'【聰明錢掃描結果】(6分制) 顯示 {len(data)} 檔', '═' * 40, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, code)
                    lines.append(self._format_scan_result_item(i, row, name))
                    lines.append('─' * 40)
                
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
    
    def _run_kd_scan(self, limit=10):
        try:
            data = supabase.scan_kd_golden(limit=limit)
            if data:
                codes = [row.get('code', '') for row in data]
                names = {c: get_stock_name(c) for c in codes}
                
                lines = ['【KD黃金交叉結果】', '═' * 40, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, code)
                    lines.append(self._format_scan_result_item(i, row, name))
                    lines.append('─' * 40)
                
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
    
    def _run_ma_scan(self, limit=10):
        try:
            data = supabase.scan_ma_rising(limit=limit)
            if data:
                codes = [row.get('code', '') for row in data]
                names = {c: get_stock_name(c) for c in codes}
                
                lines = ['【均線多頭結果】', '═' * 40, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, code)
                    lines.append(self._format_scan_result_item(i, row, name))
                    lines.append('─' * 40)
                
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
    
    def _run_vp_scan(self, limit=10):
        try:
            data = supabase.scan_vp_breakout(limit=limit)
            if data:
                codes = [row.get('code', '') for row in data]
                names = supabase.get_stock_names(codes)
                
                lines = [f'【VP突破結果】顯示 {len(data)} 檔', '═' * 28, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, '')
                    date = row.get('date', '')[:10] if row.get('date') else ''
                    close = row.get('close', 0) or 0
                    vp_high = row.get('vp_high', 0) or 0
                    vol = row.get('volume', 0) or 0
                    
                    lines.append(f'{i}. {name}({code})')
                    lines.append(f'   {date} 收盤:${close:,.2f}')
                    lines.append(f'   VP上界:{vp_high:.0f} 量:{vol//1000:,}張')
                    lines.append('─' * 28)
                
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']
    
    def _run_mfi_scan(self, limit=10):
        """MFI 超賣掃描"""
        try:
            data = supabase.scan_mfi(mode='oversold', limit=limit)
            if data:
                codes = [row.get('code', '') for row in data]
                names = supabase.get_stock_names(codes)
                
                lines = [f'【MFI超賣結果】(<20) 顯示 {len(data)} 檔', '═' * 28, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, '')
                    date = row.get('date', '')[:10] if row.get('date') else ''
                    close = row.get('close', 0) or 0
                    mfi = row.get('mfi', 0)
                    status = row.get('mfi_status', '')
                    
                    lines.append(f'{i}. {name}({code})')
                    lines.append(f'   {date} 收盤:${close:,.2f}')
                    lines.append(f'   MFI:{mfi:.1f} ({status})')
                    lines.append('─' * 28)
                
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合MFI超賣條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']
    
    def _run_triple_filter_scan(self, limit=10):
        """三重篩選掃描"""
        try:
            data = supabase.scan_triple_filter(limit=limit)
            if data:
                codes = [row.get('code', '') for row in data]
                names = supabase.get_stock_names(codes)
                
                lines = [f'【三重篩選結果】顯示 {len(data)} 檔', '═' * 28, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, '')
                    date = row.get('date', '')[:10] if row.get('date') else ''
                    close = row.get('close', 0) or 0
                    vol_ratio = row.get('vol_ratio', 0)
                    trend = row.get('trend', '')
                    
                    lines.append(f'{i}. {name}({code})')
                    lines.append(f'   {date} 收盤:${close:,.2f}')
                    lines.append(f'   量比:{vol_ratio:.1f}x 趨勢:{trend}')
                    lines.append('─' * 28)
                
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合三重篩選條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']
    
    def _run_kd_monthly_scan(self, limit=10):
        """月KD交叉掃描"""
        try:
            data = supabase.scan_kd_monthly(limit=limit)
            if data:
                codes = [row.get('code', '') for row in data]
                names = supabase.get_stock_names(codes)
                
                lines = [f'【月KD交叉結果】顯示 {len(data)} 檔', '═' * 28, '']
                for i, row in enumerate(data, 1):
                    code = row.get('code', '')
                    name = names.get(code, '')
                    date = row.get('date', '')[:10] if row.get('date') else ''
                    close = row.get('close', 0) or 0
                    k = row.get('k_monthly', 0)
                    d = row.get('d_monthly', 0)
                    cross = row.get('cross_type', '')
                    
                    lines.append(f'{i}. {name}({code})')
                    lines.append(f'   {date} 收盤:${close:,.2f}')
                    lines.append(f'   月K:{k:.1f} 月D:{d:.1f} {cross}')
                    lines.append('─' * 28)
                
                self.result_label.text = '\n'.join(lines)
                self.result_label.color = COLORS['text']
            else:
                self.result_label.text = '沒有符合月KD交叉條件的股票'
                self.result_label.color = COLORS['warning']
        except Exception as e:
            self.result_label.text = f'掃描錯誤: {str(e)}'
            self.result_label.color = COLORS['error']


# ==================== K 線圖頁面 ====================
class ChartScreen(Screen):
    """K 線圖畫面 - OHLC 蠟燭圖 + MA 均線 + 成交量副圖"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.stock_code = ''
        self.stock_name = ''
        self.period = 'day'  # day, week, month
        self.data = []
        self.ma_enabled = {'ma3': True, 'ma20': True, 'ma60': False, 'ma120': False, 'ma200': False}
        
        layout = BoxLayout(orientation='vertical', padding=dp(8), spacing=dp(8))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        # 標題列 + 股票代碼輸入
        header = BoxLayout(size_hint_y=0.08, spacing=dp(8))
        self.code_input = TextInput(
            text='2330',
            hint_text='股票代碼',
            font_name=DEFAULT_FONT,
            font_size=sp(14),
            size_hint_x=0.25,
            multiline=False,
            background_color=COLORS['card'],
            foreground_color=COLORS['text']
        )
        header.add_widget(self.code_input)
        
        self.title_label = Label(
            text='請輸入股票代碼',
            font_name=DEFAULT_FONT,
            font_size=sp(18),
            color=COLORS['text'],
            bold=True,
            size_hint_x=0.45
        )
        header.add_widget(self.title_label)
        
        # 頂部控制列 (週期 + MA 開關)
        control_bar = BoxLayout(size_hint_y=None, height=dp(40), spacing=dp(5))
        
        # 週期切換
        for period, label in [('day', '日'), ('week', '週'), ('month', '月')]:
            btn = ToggleButton(
                text=label,
                group='period',
                state='down' if period == self.period else 'normal',
                font_name=DEFAULT_FONT,
                font_size=sp(14),
                size_hint_x=None,
                width=dp(40),
                background_normal='',
                background_color=COLORS['card'],
                background_down=COLORS['accent'],
                color=COLORS['text']
            )
            btn.period = period
            btn.bind(on_press=self.on_period_change)
            control_bar.add_widget(btn)
            
        # MA 開關 (使用 ToggleButton)
        ma_colors = {'ma3': (0.3, 0.6, 1, 1), 'ma20': (1, 0.8, 0, 1), 'ma60': (0.8, 0.3, 0.8, 1), 
                     'ma120': (1, 0.5, 0, 1), 'ma200': (0.5, 0.5, 0.5, 1)}
        for ma, color in ma_colors.items():
            btn = ToggleButton(
                text=ma.upper(),
                font_name=DEFAULT_FONT,
                font_size=sp(10),
                size_hint_x=None,
                width=dp(50),
                background_normal='',
                background_color=COLORS['card'],
                background_down=color,
                color=COLORS['text']
            )
            btn.ma_name = ma
            btn.bind(on_press=self.toggle_ma)
            control_bar.add_widget(btn)
            
        layout.add_widget(control_bar)
        
        # K 線主圖
        chart_card = BoxLayout(orientation='vertical', size_hint_y=0.55)
        with chart_card.canvas.before:
            Color(*COLORS['card'])
            chart_card.rect = RoundedRectangle(pos=chart_card.pos, size=chart_card.size, radius=[dp(12)])
        chart_card.bind(pos=lambda i,v: setattr(chart_card.rect, 'pos', v), size=lambda i,v: setattr(chart_card.rect, 'size', v))
        
        self.chart_widget = Widget()
        self.chart_widget.bind(size=self.draw_chart, pos=self.draw_chart)
        chart_card.add_widget(self.chart_widget)
        layout.add_widget(chart_card)
        
        # 成交量副圖
        vol_card = BoxLayout(orientation='vertical', size_hint_y=0.15)
        with vol_card.canvas.before:
            Color(*COLORS['card'])
            vol_card.rect = RoundedRectangle(pos=vol_card.pos, size=vol_card.size, radius=[dp(12)])
        vol_card.bind(pos=lambda i,v: setattr(vol_card.rect, 'pos', v), size=lambda i,v: setattr(vol_card.rect, 'size', v))
        
        self.volume_widget = Widget()
        self.volume_widget.bind(size=self.draw_volume, pos=self.draw_volume)
        vol_card.add_widget(self.volume_widget)
        layout.add_widget(vol_card)
        
        # 指標副圖 (含下拉選單)
        ind_card = BoxLayout(orientation='vertical', size_hint_y=0.2)
        with ind_card.canvas.before:
            Color(*COLORS['card'])
            ind_card.rect = RoundedRectangle(pos=ind_card.pos, size=ind_card.size, radius=[dp(12)])
        ind_card.bind(pos=lambda i,v: setattr(ind_card.rect, 'pos', v), size=lambda i,v: setattr(ind_card.rect, 'size', v))
        
        # 指標選擇器
        ind_spinner = Spinner(
            text='KD 指標',
            values=('KD 指標', 'MACD', 'RSI', 'MFI'),
            font_name=DEFAULT_FONT,
            font_size=sp(12),
            size_hint_y=None,
            height=dp(30),
            background_normal='',
            background_color=COLORS['button'],
            option_cls=ChineseSpinnerOption
        )
        ind_spinner.bind(text=self.on_indicator_change)
        ind_card.add_widget(ind_spinner)
        
        self.indicator_widget = Widget()
        self.indicator_widget.bind(size=self.draw_indicator, pos=self.draw_indicator)
        ind_card.add_widget(self.indicator_widget)
        layout.add_widget(ind_card)
        
        self.add_widget(layout)
    
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
    
    def on_period_change(self, instance):
        self.period = instance.period
        self.load_chart(None)
    
    def on_indicator_change(self, instance, text):
        self.current_indicator = text
        self.draw_indicator(None, None)

    def toggle_ma(self, instance):
        ma = instance.ma_name
        self.ma_enabled[ma] = instance.state == 'down'
        self.draw_chart(None, None)

    def draw_indicator(self, instance, value):
        self.indicator_widget.canvas.clear()
        if not self.df or self.df.empty:
            return
            
        with self.indicator_widget.canvas:
            # 繪製邊框
            Color(*COLORS['divider'])
            Line(rectangle=(self.indicator_widget.x, self.indicator_widget.y, self.indicator_widget.width, self.indicator_widget.height), width=1)
            
            # 顯示指標名稱
            Color(*COLORS['text_secondary'])
            
        # TODO: 實作具體的指標繪製邏輯 (KD, MACD 等)
        # 這部分需要計算指標數據並繪製線條
        pass
    
    def load_chart(self, instance):
        code = self.code_input.text.strip()
        if not code or not supabase:
            return
        
        self.stock_code = code
        info = supabase.get_stock_info(code)
        self.stock_name = info.get('name', code) if info else code
        self.title_label.text = f'{self.stock_name} ({code})'
        
        # 取得資料
        limit = {'day': 60, 'week': 52, 'month': 24}.get(self.period, 60)
        self.data = supabase.get_stock_data(code, limit=limit) or []
        self.data = list(reversed(self.data))  # 時間順序
        
        self.draw_chart(None, None)
        self.draw_volume(None, None)
    
    def draw_chart(self, instance, value):
        """繪製 K 線蠟燭圖"""
        self.chart_widget.canvas.clear()
        if not self.data:
            return
        
        widget = self.chart_widget
        x, y = widget.pos
        w, h = widget.size
        
        if w <= 0 or h <= 0:
            return
        
        # 計算價格範圍
        highs = [d.get('high', 0) or d.get('close', 0) for d in self.data]
        lows = [d.get('low', 0) or d.get('close', 0) for d in self.data]
        max_price = max(highs) if highs else 1
        min_price = min(lows) if lows else 0
        price_range = max_price - min_price or 1
        
        # 繪製設定
        n = len(self.data)
        candle_width = max(w / n * 0.8, 2)
        spacing = w / n
        
        with widget.canvas:
            # 繪製蠟燭
            for i, d in enumerate(self.data):
                open_p = d.get('open', 0) or d.get('close', 0)
                close_p = d.get('close', 0) or 0
                high_p = d.get('high', 0) or close_p
                low_p = d.get('low', 0) or close_p
                
                cx = x + i * spacing + spacing / 2
                is_up = close_p >= open_p
                
                # 顏色
                if is_up:
                    Color(*COLORS['up'])
                else:
                    Color(*COLORS['down'])
                
                # 上下影線
                y_high = y + (high_p - min_price) / price_range * h
                y_low = y + (low_p - min_price) / price_range * h
                Line(points=[cx, y_low, cx, y_high], width=1)
                
                # 實體
                y_open = y + (open_p - min_price) / price_range * h
                y_close = y + (close_p - min_price) / price_range * h
                body_top = max(y_open, y_close)
                body_bottom = min(y_open, y_close)
                body_height = max(body_top - body_bottom, 1)
                Rectangle(pos=(cx - candle_width/2, body_bottom), size=(candle_width, body_height))
            
            # 繪製 MA 均線
            ma_colors = {'ma3': (0.3, 0.6, 1, 1), 'ma20': (1, 0.8, 0, 1), 'ma60': (0.8, 0.3, 0.8, 1),
                         'ma120': (1, 0.5, 0, 1), 'ma200': (0.5, 0.5, 0.5, 1)}
            for ma_name, color in ma_colors.items():
                if not self.ma_enabled.get(ma_name):
                    continue
                period = int(ma_name.replace('ma', ''))
                if len(self.data) < period:
                    continue
                
                Color(*color)
                points = []
                for i in range(period - 1, len(self.data)):
                    vals = [self.data[j].get('close', 0) or 0 for j in range(i - period + 1, i + 1)]
                    ma_val = sum(vals) / period
                    px = x + i * spacing + spacing / 2
                    py = y + (ma_val - min_price) / price_range * h
                    points.extend([px, py])
                if len(points) >= 4:
                    Line(points=points, width=1.2)
    
    def draw_volume(self, instance, value):
        """繪製成交量副圖"""
        self.volume_widget.canvas.clear()
        if not self.data:
            return
        
        widget = self.volume_widget
        x, y = widget.pos
        w, h = widget.size
        
        if w <= 0 or h <= 0:
            return
        
        vols = [d.get('volume', 0) or 0 for d in self.data]
        max_vol = max(vols) if vols else 1
        
        n = len(self.data)
        bar_width = max(w / n * 0.8, 2)
        spacing = w / n
        
        with widget.canvas:
            for i, d in enumerate(self.data):
                vol = d.get('volume', 0) or 0
                open_p = d.get('open', 0) or d.get('close', 0)
                close_p = d.get('close', 0) or 0
                is_up = close_p >= open_p
                
                Color(*COLORS['up'] if is_up else COLORS['down'])
                
                cx = x + i * spacing + spacing / 2
                bar_h = vol / max_vol * h * 0.9
                Rectangle(pos=(cx - bar_width/2, y), size=(bar_width, bar_h))


# ==================== AI 助手頁面 ====================
class AIScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=dp(16), spacing=dp(10))
        
        with layout.canvas.before:
            Color(*COLORS['bg'])
            self.bg = Rectangle(pos=layout.pos, size=layout.size)
        layout.bind(pos=self._update_bg, size=self._update_bg)
        
        # 標題
        title_box = BoxLayout(size_hint_y=None, height=dp(50))
        title_box.add_widget(Label(
            text='🤖 AI 助手',
            font_name=DEFAULT_FONT,
            font_size=sp(20),
            color=COLORS['text'],
            bold=True,
            halign='left'
        ))
        layout.add_widget(title_box)
        
        # 對話歷史區
        chat_card = BoxLayout(orientation='vertical', size_hint_y=0.7)
        with chat_card.canvas.before:
            Color(*COLORS['card'])
            chat_card.rect = RoundedRectangle(pos=chat_card.pos, size=chat_card.size, radius=[dp(12)])
        chat_card.bind(pos=lambda i,v: setattr(chat_card.rect, 'pos', v), size=lambda i,v: setattr(chat_card.rect, 'size', v))
        
        scroll = ScrollView()
        self.chat_history = Label(
            text='🤖: 您好！我是您的股市 AI 助手。\n您可以問我：「2330 走勢如何？」或「幫我分析聯發科」',
            font_name=DEFAULT_FONT,
            font_size=sp(15),
            color=COLORS['text'],
            halign='left',
            valign='top',
            size_hint_y=None,
            markup=True,
            padding=[dp(10), dp(10)]
        )
        self.chat_history.bind(texture_size=self._update_label_size)
        scroll.add_widget(self.chat_history)
        chat_card.add_widget(scroll)
        layout.add_widget(chat_card)
        
        # 輸入區
        input_row = BoxLayout(size_hint_y=None, height=dp(50), spacing=dp(10))
        self.msg_input = TextInput(
            hint_text='輸入問題...',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            multiline=False,
            size_hint_x=0.7,
            background_color=COLORS['input'],
            foreground_color=COLORS['text'],
            padding=[dp(10), dp(10)]
        )
        input_row.add_widget(self.msg_input)
        
        send_btn = Button(
            text='送出',
            font_name=DEFAULT_FONT,
            font_size=sp(16),
            size_hint_x=0.2,
            background_normal='',
            background_color=COLORS['accent'],
            color=COLORS['text']
        )
        send_btn.bind(on_press=self.send_message)
        input_row.add_widget(send_btn)
        
        # 語音按鈕 (模擬)
        mic_btn = Button(
            text='🎤',
            font_name=DEFAULT_FONT,
            font_size=sp(20),
            size_hint_x=0.1,
            background_normal='',
            background_color=COLORS['button'],
            color=COLORS['text']
        )
        input_row.add_widget(mic_btn)
        layout.add_widget(input_row)
        
        # 快速提問區
        quick_row = BoxLayout(size_hint_y=None, height=dp(40), spacing=dp(5))
        for text in ['生成插件', '分析股票', '解釋指標', '新聞']:
            btn = Button(
                text=text,
                font_name=DEFAULT_FONT,
                font_size=sp(12),
                background_normal='',
                background_color=COLORS['button'],
                color=COLORS['text']
            )
            btn.bind(on_press=self.quick_ask)
            quick_row.add_widget(btn)
        layout.add_widget(quick_row)
        
        self.add_widget(layout)
        
    def _update_bg(self, instance, value):
        self.bg.pos = instance.pos
        self.bg.size = instance.size
        
    def _update_label_size(self, instance, value):
        instance.height = value[1]
        instance.text_size = (instance.width - dp(20), None)
        
    def send_message(self, instance):
        msg = self.msg_input.text.strip()
        if not msg: return
        
        self.chat_history.text += f"\n\n👤: {msg}"
        self.msg_input.text = ''
        
        # 模擬 AI 回應
        Clock.schedule_once(lambda dt: self.ai_reply(msg), 0.5)
        
    def ai_reply(self, msg):
        reply = "🤖: 我還在學習中，目前只能回答簡單的股票問題。"
        if '2330' in msg or '台積電' in msg:
            reply = "🤖: 台積電 (2330) 近期表現強勢，外資持續買超，技術面呈現多頭排列。"
        elif '分析' in msg:
            reply = "🤖: 請提供股票代碼，我將為您進行技術面與籌碼面分析。"
            
        self.chat_history.text += f"\n\n{reply}"
        
    def quick_ask(self, instance):
        self.msg_input.text = instance.text
        self.send_message(None)
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
            text='版本: 1.0.10',
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
        self.sm.add_widget(ChartScreen(name='chart'))
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
            ('K線', 'chart'),
            ('自選', 'watchlist'),
            ('AI', 'ai_chat'),
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
