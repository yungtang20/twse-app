"""
首頁畫面 - 系統狀態與快速操作
"""
from kivy.uix.screenmanager import Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.scrollview import ScrollView
from kivy.uix.gridlayout import GridLayout

class HomeScreen(Screen):
    """首頁"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 主容器
        main_layout = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # 系統狀態卡片
        status_card = BoxLayout(orientation='vertical', size_hint_y=0.3, padding=10, spacing=5)
        status_card.canvas.before.clear()
        
        status_card.add_widget(Label(
            text='📊 系統狀態', 
            font_size=18, 
            size_hint_y=0.3,
            bold=True
        ))
        
        status_info = GridLayout(cols=2, spacing=5, size_hint_y=0.7)
        status_info.add_widget(Label(text='最新資料:', halign='left'))
        status_info.add_widget(Label(text='2025-12-04', halign='right'))
        
        status_info.add_widget(Label(text='股票總數:', halign='left'))
        status_info.add_widget(Label(text='1921 檔', halign='right'))
        
        status_info.add_widget(Label(text='資料範圍:', halign='left'))
        status_info.add_widget(Label(text='450 天', halign='right'))
        
        status_card.add_widget(status_info)
        main_layout.add_widget(status_card)
        
        # 快速操作
        quick_actions = BoxLayout(orientation='vertical', size_hint_y=0.25, padding=10, spacing=5)
        quick_actions.add_widget(Label(
            text='⚡ 快速操作', 
            font_size=18,
            size_hint_y=0.3,
            bold=True
        ))
        
        actions_grid = GridLayout(cols=2, spacing=10, size_hint_y=0.7)
        
        btn_update = Button(text='📥 更新資料')
        btn_update.bind(on_press=self.on_update_data)
        actions_grid.add_widget(btn_update)
        
        btn_calc = Button(text='🧮 計算指標')
        btn_calc.bind(on_press=self.on_calc_indicators)
        actions_grid.add_widget(btn_calc)
        
        quick_actions.add_widget(actions_grid)
        main_layout.add_widget(quick_actions)
        
        # 最近掃描結果
        recent_scan = BoxLayout(orientation='vertical', size_hint_y=0.45, padding=10, spacing=5)
        recent_scan.add_widget(Label(
            text='🔍 最近掃描',
            font_size=18,
            size_hint_y=0.15,
            bold=True
        ))
        
        scroll = ScrollView(size_hint_y=0.85)
        scan_list = BoxLayout(orientation='vertical', size_hint_y=None, spacing=5)
        scan_list.bind(minimum_height=scan_list.setter('height'))
        
        # 示例掃描結果
        for i in range(3):
            item = Label(
                text=f'2330 台積電 Score:5\n1050 ▲+25 (+2.4%)',
                size_hint_y=None,
                height=60,
                halign='left',
                valign='top'
            )
            item.bind(size=item.setter('text_size'))
            scan_list.add_widget(item)
        
        scroll.add_widget(scan_list)
        recent_scan.add_widget(scroll)
        main_layout.add_widget(recent_scan)
        
        self.add_widget(main_layout)
    
    def on_update_data(self, instance):
        """更新資料"""
        print("執行更新資料...")
    
    def on_calc_indicators(self, instance):
        """計算指標"""
        print("執行計算指標...")
