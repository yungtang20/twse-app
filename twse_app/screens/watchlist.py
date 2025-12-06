"""
自選股畫面
"""
from kivy.uix.screenmanager import Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.scrollview import ScrollView
from kivy.uix.gridlayout import GridLayout
from kivy.uix.popup import Popup
from kivy.uix.textinput import TextInput
from kivy.app import App
from kivy.clock import Clock

try:
    from src.supabase_client import SupabaseClient
    from config import SUPABASE_URL, SUPABASE_KEY
except ImportError:
    SUPABASE_URL = ""
    SUPABASE_KEY = ""

class WatchlistScreen(Screen):
    """自選股"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.supabase = None
        if SUPABASE_URL and SUPABASE_KEY:
            self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        main_layout = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # 頂部按鈕
        header = BoxLayout(size_hint_y=0.08, spacing=10)
        header.add_widget(Label(text='⭐ 自選股', font_size=20, bold=True))
        
        btn_add = Button(text='➕ 新增', size_hint_x=0.3, background_color=(0.2, 0.4, 0.3, 1))
        btn_add.bind(on_press=self.show_add_popup)
        header.add_widget(btn_add)
        
        main_layout.add_widget(header)
        
        # 股票列表
        scroll = ScrollView(size_hint_y=0.92)
        self.watchlist = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=10,
            padding=5
        )
        self.watchlist.bind(minimum_height=self.watchlist.setter('height'))
        
        # 示例數據 (實際應從 DB 讀取)
        self.stocks = [
            {'code': '2330', 'name': '台積電', 'price': 1050, 'change': 25, 'pct': 2.4},
            {'code': '2454', 'name': '聯發科', 'price': 1280, 'change': 15, 'pct': 1.2},
            {'code': '2317', 'name': '鴻海', 'price': 205, 'change': -2, 'pct': -0.9},
        ]
        
        self.refresh_watchlist()
        
        scroll.add_widget(self.watchlist)
        main_layout.add_widget(scroll)
        
        self.add_widget(main_layout)
    
    def refresh_watchlist(self):
        """刷新自選股列表"""
        self.watchlist.clear_widgets()
        
        # TODO: 從 Supabase 讀取自選股
        # if self.supabase:
        #     self.stocks = self.supabase.get_watchlist()
        
        for stock in self.stocks:
            card = Button(
                size_hint_y=None,
                height=100,
                background_color=(0.1, 0.2, 0.15, 1),
                background_normal=''
            )
            card.bind(on_press=lambda x, c=stock['code'], n=stock['name']: self.goto_query(c, n))
            
            # 使用 BoxLayout 作為內容容器 (Kivy Button 不直接支援複雜佈局，需用自定義 Widget 或簡單文字)
            # 這裡簡化為文字顯示，若要複雜佈局需自定義 Behavior
            
            price_color = 'ff3a3a' if stock['change'] > 0 else '3aba59'
            sign = '+' if stock['change'] > 0 else ''
            
            card.markup = True
            card.text = (
                f"[b][size=18]{stock['code']} {stock['name']}[/size][/b]\n"
                f"[color={price_color}]{stock['price']}  {sign}{stock['change']} ({sign}{stock['pct']}%) [/color]\n"
                f"[color=aaaaaa]MFI: 65.2↑  量: 5,230[/color]"
            )
            card.halign = 'left'
            card.valign = 'middle'
            card.padding = (20, 10)
            card.bind(size=card.setter('text_size'))
            
            # 刪除按鈕 (獨立於卡片)
            row = BoxLayout(size_hint_y=None, height=100, spacing=5)
            row.add_widget(card)
            
            btn_del = Button(
                text='🗑️', 
                size_hint_x=0.15, 
                background_color=(0.5, 0.2, 0.2, 1)
            )
            btn_del.bind(on_press=lambda x, c=stock['code']: self.delete_stock(c))
            row.add_widget(btn_del)
            
            self.watchlist.add_widget(row)
    
    def goto_query(self, code, name):
        """跳轉到查詢頁面"""
        app = App.get_running_app()
        if app:
            app.navigate_to_query(code, name)
    
    def show_add_popup(self, instance):
        """顯示新增股票彈窗"""
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        content.add_widget(Label(text='輸入股票代碼:'))
        
        code_input = TextInput(multiline=False, hint_text='例如: 2330')
        content.add_widget(code_input)
        
        btn_layout = BoxLayout(spacing=10)
        btn_cancel = Button(text='取消', background_color=(0.4, 0.4, 0.4, 1))
        btn_ok = Button(text='確定', background_color=(0.075, 0.925, 0.357, 1))
        
        popup = Popup(
            title='新增自選股',
            content=content,
            size_hint=(0.8, 0.4)
        )
        
        btn_cancel.bind(on_press=popup.dismiss)
        btn_ok.bind(on_press=lambda x: self.add_stock(code_input.text, popup))
        
        btn_layout.add_widget(btn_cancel)
        btn_layout.add_widget(btn_ok)
        content.add_widget(btn_layout)
        
        popup.open()
    
    def add_stock(self, code, popup):
        """新增股票"""
        if code:
            # TODO: 實際新增邏輯 (寫入 DB)
            self.stocks.append({
                'code': code, 
                'name': '新增股票', 
                'price': 100, 
                'change': 0, 
                'pct': 0
            })
            self.refresh_watchlist()
            popup.dismiss()
    
    def delete_stock(self, code):
        """刪除股票"""
        self.stocks = [s for s in self.stocks if s['code'] != code]
        self.refresh_watchlist()
