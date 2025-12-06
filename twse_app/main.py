"""
台股分析 App - Kivy Main Entry Point
5 分頁導航: 查詢/掃描/自選/AI助手/設定
"""
from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, SlideTransition
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.core.window import Window
from kivy.graphics import Color, Rectangle
from kivy.properties import BooleanProperty, StringProperty
from kivy.utils import get_color_from_hex

# 匯入畫面
from screens.query import QueryScreen
from screens.scan import ScanScreen
from screens.watchlist import WatchlistScreen
from screens.ai_chat import AIChatScreen
from screens.settings import SettingsScreen

# 設定視窗大小 (開發時模擬手機)
Window.size = (360, 640)


# 主題顏色
THEME = {
    'dark': {
        'bg': '#102216',
        'primary': '#13ec5b',
        'secondary': '#1a3d2a',
        'surface': '#15291d',
        'text': '#e0e0e0',
        'text_secondary': '#71717a',
    },
    'light': {
        'bg': '#f6f8f6',
        'primary': '#13ec5b',
        'secondary': '#d0e8d8',
        'surface': '#ffffff',
        'text': '#1a1a1a',
        'text_secondary': '#71717a',
    }
}


class NavButton(Button):
    """導航按鈕"""
    
    is_active = BooleanProperty(False)
    
    def __init__(self, icon: str, label: str, screen_name: str, **kwargs):
        super().__init__(**kwargs)
        self.screen_name = screen_name
        self.icon = icon
        self.label = label
        self.text = f'{icon}\n{label}'
        self.font_size = 14
        self.halign = 'center'
        self.valign = 'middle'
        self.background_normal = ''
        self.background_color = (0.063, 0.133, 0.086, 1)  # #102216
        self.color = (0.443, 0.443, 0.478, 1)  # #71717a
    
    def set_active(self, active: bool):
        self.is_active = active
        if active:
            self.color = (0.075, 0.925, 0.357, 1)  # #13ec5b
            self.bold = True
        else:
            self.color = (0.443, 0.443, 0.478, 1)
            self.bold = False


class TWSEApp(App):
    """主 App"""
    
    is_cloud_mode = BooleanProperty(True)
    is_dark_theme = BooleanProperty(True)
    current_theme = StringProperty('dark')
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.nav_buttons = {}
        self.sm = None
    
    def build(self):
        # 建立 ScreenManager
        self.sm = ScreenManager(transition=SlideTransition())
        self.sm.add_widget(QueryScreen(name='query'))
        self.sm.add_widget(ScanScreen(name='scan'))
        self.sm.add_widget(WatchlistScreen(name='watchlist'))
        self.sm.add_widget(AIChatScreen(name='ai_chat'))
        self.sm.add_widget(SettingsScreen(name='settings'))
        
        # 主容器
        root = BoxLayout(orientation='vertical')
        
        # 設定背景色
        with root.canvas.before:
            Color(*get_color_from_hex(THEME['dark']['bg']))
            self.bg_rect = Rectangle(pos=root.pos, size=root.size)
        root.bind(pos=self._update_bg, size=self._update_bg)
        
        # 頂部標題列
        header = BoxLayout(size_hint_y=0.07, padding=[10, 5])
        
        with header.canvas.before:
            Color(*get_color_from_hex(THEME['dark']['surface']))
            header.rect = Rectangle(pos=header.pos, size=header.size)
        header.bind(
            pos=lambda i, v: setattr(header.rect, 'pos', v),
            size=lambda i, v: setattr(header.rect, 'size', v)
        )
        
        self.title_label = Label(
            text='📊 台股分析',
            font_size=18,
            bold=True,
            color=get_color_from_hex('#13ec5b'),
            size_hint_x=0.5
        )
        header.add_widget(self.title_label)
        
        # 雲端/本地切換
        self.mode_btn = Button(
            text='☁️ 雲端',
            font_size=12,
            size_hint_x=0.25,
            background_color=get_color_from_hex('#1a3d2a'),
            color=get_color_from_hex('#e0e0e0')
        )
        self.mode_btn.bind(on_press=self.toggle_mode)
        header.add_widget(self.mode_btn)
        
        # 主題切換
        self.theme_btn = Button(
            text='🌙',
            font_size=16,
            size_hint_x=0.15,
            background_color=get_color_from_hex('#1a3d2a')
        )
        self.theme_btn.bind(on_press=self.toggle_theme)
        header.add_widget(self.theme_btn)
        
        root.add_widget(header)
        
        # 畫面區域
        root.add_widget(self.sm)
        
        # 底部導航列
        nav = BoxLayout(size_hint_y=0.09, spacing=2, padding=[5, 5])
        
        with nav.canvas.before:
            Color(*get_color_from_hex(THEME['dark']['surface']))
            nav.rect = Rectangle(pos=nav.pos, size=nav.size)
        nav.bind(
            pos=lambda i, v: setattr(nav.rect, 'pos', v),
            size=lambda i, v: setattr(nav.rect, 'size', v)
        )
        
        # 5 個導航按鈕
        nav_config = [
            ('📊', '查詢', 'query'),
            ('📈', '掃描', 'scan'),
            ('⭐', '自選', 'watchlist'),
            ('🤖', 'AI助手', 'ai_chat'),
            ('⚙️', '設定', 'settings'),
        ]
        
        for icon, label, screen_name in nav_config:
            btn = NavButton(icon, label, screen_name)
            btn.bind(on_press=self.on_nav_press)
            nav.add_widget(btn)
            self.nav_buttons[screen_name] = btn
        
        # 設定預設選中
        self.nav_buttons['query'].set_active(True)
        
        root.add_widget(nav)
        
        return root
    
    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size
    
    def on_nav_press(self, instance):
        """導航按鈕點擊"""
        screen_name = instance.screen_name
        
        # 更新按鈕狀態
        for name, btn in self.nav_buttons.items():
            btn.set_active(name == screen_name)
        
        # 切換畫面
        self.sm.current = screen_name
    
    def toggle_mode(self, instance):
        """切換雲端/本地模式"""
        self.is_cloud_mode = not self.is_cloud_mode
        if self.is_cloud_mode:
            self.mode_btn.text = '☁️ 雲端'
        else:
            self.mode_btn.text = '💾 本地'
        print(f"切換至: {'雲端' if self.is_cloud_mode else '本地'}模式")
    
    def toggle_theme(self, instance):
        """切換深淺色主題"""
        self.is_dark_theme = not self.is_dark_theme
        if self.is_dark_theme:
            self.current_theme = 'dark'
            self.theme_btn.text = '🌙'
        else:
            self.current_theme = 'light'
            self.theme_btn.text = '☀️'
        
        # TODO: 更新所有畫面的主題顏色
        print(f"切換至: {'深色' if self.is_dark_theme else '淺色'}主題")
    
    def navigate_to_query(self, stock_code: str, stock_name: str = ""):
        """導航到查詢頁並載入股票"""
        self.sm.current = 'query'
        
        # 更新導航按鈕狀態
        for name, btn in self.nav_buttons.items():
            btn.set_active(name == 'query')
        
        # 載入股票
        query_screen = self.sm.get_screen('query')
        query_screen.load_stock(stock_code, stock_name)


if __name__ == '__main__':
    TWSEApp().run()
