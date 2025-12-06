"""
掃描畫面 - 策略選擇 (整合插件系統)
"""
from kivy.uix.screenmanager import Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.scrollview import ScrollView
from kivy.uix.gridlayout import GridLayout
from kivy.uix.textinput import TextInput
from kivy.uix.popup import Popup
from kivy.app import App
from kivy.clock import Clock

# 匯入後端
from src.config import SUPABASE_URL, SUPABASE_KEY
from src.supabase_client import SupabaseClient
from src.plugin_engine import PluginEngine

class ScanScreen(Screen):
    """掃描"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 初始化 Supabase Client
        self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        # 初始化插件引擎
        self.plugin_engine = PluginEngine()
        
        main_layout = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # 篩選條件
        filter_box = BoxLayout(orientation='vertical', size_hint_y=0.2, padding=10, spacing=5)
        filter_box.add_widget(Label(text='📊 篩選條件', font_size=18, size_hint_y=0.3, bold=True))
        
        filter_grid = GridLayout(cols=2, spacing=5, size_hint_y=0.7)
        filter_grid.add_widget(Label(text='成交量 ≥ (張):'))
        self.vol_input = TextInput(text='500', multiline=False)
        filter_grid.add_widget(self.vol_input)
        
        filter_grid.add_widget(Label(text='顯示筆數:'))
        self.limit_input = TextInput(text='20', multiline=False)
        filter_grid.add_widget(self.limit_input)
        
        filter_box.add_widget(filter_grid)
        main_layout.add_widget(filter_box)
        
        # 策略列表
        strategies_label = Label(
            text='🔍 掃描策略',
            font_size=18,
            size_hint_y=0.05,
            bold=True
        )
        main_layout.add_widget(strategies_label)
        
        scroll = ScrollView(size_hint_y=0.75)
        self.strategies_list = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=10,
            padding=5
        )
        self.strategies_list.bind(minimum_height=self.strategies_list.setter('height'))
        
        scroll.add_widget(self.strategies_list)
        main_layout.add_widget(scroll)
        
        self.add_widget(main_layout)
        
        # 載入策略
        self.load_strategies()
    
    def load_strategies(self):
        """載入所有策略 (內建 + 用戶自定義)"""
        self.strategies_list.clear_widgets()
        plugins = self.plugin_engine.get_all_plugins()
        
        for plugin in plugins:
            card = BoxLayout(
                orientation='vertical',
                size_hint_y=None,
                height=80,
                padding=10,
                spacing=3
            )
            
            # 標題行
            title_row = BoxLayout(orientation='horizontal')
            title_row.add_widget(Label(
                text=f"{plugin.icon} {plugin.name}",
                font_size=16,
                bold=True,
                size_hint_x=0.8,
                halign='left'
            ))
            
            # 類型標籤
            type_text = "內建" if plugin.is_builtin else "自訂"
            type_color = (0.5, 0.5, 0.5, 1) if plugin.is_builtin else (0.2, 0.6, 1, 1)
            title_row.add_widget(Label(
                text=type_text,
                font_size=10,
                size_hint_x=0.2,
                color=type_color
            ))
            card.add_widget(title_row)
            
            # 描述
            card.add_widget(Label(
                text=plugin.description,
                font_size=12,
                size_hint_y=0.5,
                halign='left',
                color=(0.7, 0.7, 0.7, 1)
            ))
            
            btn = Button(
                text='執行',
                size_hint=(0.3, 1),
                pos_hint={'right': 1},
                background_color=(0.075, 0.925, 0.357, 1)
            )
            btn.bind(on_press=lambda x, p=plugin: self.on_scan_strategy(p))
            
            card_container = BoxLayout(orientation='horizontal')
            card_container.add_widget(card)
            card_container.add_widget(btn)
            
            self.strategies_list.add_widget(card_container)
    
    def on_scan_strategy(self, plugin):
        """執行掃描策略"""
        # 顯示載入中
        popup = Popup(
            title='掃描中...',
            content=Label(text=f'正在執行 {plugin.name}'),
            size_hint=(0.6, 0.3),
            auto_dismiss=False
        )
        popup.open()
        
        # 使用 Clock 延遲執行，讓 Popup 有機會顯示
        Clock.schedule_once(lambda dt: self._execute_scan(plugin, popup), 0.1)

    def _execute_scan(self, plugin, popup):
        try:
            # 取得參數
            min_vol_thousands = int(self.vol_input.text or 500)
            min_vol = min_vol_thousands * 1000
            limit = int(self.limit_input.text or 20)
            
            # 取得最新指標資料
            # 注意: 實際應用應從本地 DB 或 Supabase 獲取完整數據
            # 這裡簡化為從 Supabase 獲取
            indicators_data = self.supabase.fetch_latest_indicators(min_volume=min_vol)
            
            if not indicators_data:
                popup.dismiss()
                self.show_error('無法取得指標資料，請檢查網路連線')
                return
            
            # 執行插件
            results = self.plugin_engine.execute_plugin(plugin.id, indicators_data)
            
            # 限制數量
            results = results[:limit]
            
            popup.dismiss()
            
            # 顯示結果
            self.show_results(plugin.name, results)
            
        except Exception as e:
            popup.dismiss()
            self.show_error(f'掃描發生錯誤: {str(e)}')
    
    def show_results(self, title, results):
        """顯示掃描結果"""
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # 標題
        content.add_widget(Label(
            text=f'{title}\n找到 {len(results)} 檔',
            size_hint_y=0.15,
            font_size=16
        ))
        
        # 結果列表
        scroll = ScrollView(size_hint_y=0.75)
        results_list = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=5
        )
        results_list.bind(minimum_height=results_list.setter('height'))
        
        for item in results:
            # item 可能是 (code, score, data) 或 (code, data)
            if len(item) == 3:
                code, score, data = item
                display_text = f"{code} {data.get('name', '')}  分數: {score}"
            else:
                code, data = item
                display_text = f"{code} {data.get('name', '')}  收盤: {data.get('close', 0)}"
            
            # 建立可點擊的項目
            item_btn = Button(
                text=display_text,
                size_hint_y=None,
                height=60,
                halign='left',
                background_color=(0.2, 0.3, 0.25, 1)
            )
            item_btn.bind(on_press=lambda x, c=code, n=data.get('name', ''): self.goto_query(c, n))
            
            results_list.add_widget(item_btn)
        
        scroll.add_widget(results_list)
        content.add_widget(scroll)
        
        # 關閉按鈕
        btn_close = Button(text='關閉', size_hint_y=0.1)
        content.add_widget(btn_close)
        
        self.results_popup = Popup(
            title=title,
            content=content,
            size_hint=(0.9, 0.8)
        )
        btn_close.bind(on_press=self.results_popup.dismiss)
        self.results_popup.open()
    
    def goto_query(self, code, name):
        """跳轉到查詢頁面"""
        if self.results_popup:
            self.results_popup.dismiss()
            
        app = App.get_running_app()
        if app:
            app.navigate_to_query(code, name)
    
    def show_error(self, message):
        """顯示錯誤訊息"""
        popup = Popup(
            title='錯誤',
            content=Label(text=message),
            size_hint=(0.7, 0.3)
        )
        popup.open()
