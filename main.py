"""
台股分析 App - 最小化版本
確保可以在 Android 上正常啟動
"""
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.scrollview import ScrollView
from kivy.uix.gridlayout import GridLayout
from kivy.graphics import Color, Rectangle
from kivy.clock import Clock


class TWSEApp(App):
    """主 App"""
    
    def build(self):
        # 主容器
        root = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # 設定深色背景
        with root.canvas.before:
            Color(0.063, 0.133, 0.086, 1)  # #102216
            self.bg_rect = Rectangle(pos=root.pos, size=root.size)
        root.bind(pos=self._update_bg, size=self._update_bg)
        
        # 標題
        title = Label(
            text='📊 台股分析 App',
            font_size=24,
            size_hint_y=0.1,
            color=(0.075, 0.925, 0.357, 1)  # #13ec5b
        )
        root.add_widget(title)
        
        # 狀態標籤
        self.status_label = Label(
            text='App 啟動成功！',
            font_size=18,
            size_hint_y=0.1,
            color=(0.9, 0.9, 0.9, 1)
        )
        root.add_widget(self.status_label)
        
        # 測試按鈕區域
        button_box = BoxLayout(orientation='vertical', size_hint_y=0.3, spacing=10)
        
        test_btn = Button(
            text='測試按鈕',
            font_size=16,
            background_color=(0.1, 0.4, 0.2, 1)
        )
        test_btn.bind(on_press=self.on_test_press)
        button_box.add_widget(test_btn)
        
        root.add_widget(button_box)
        
        # 結果區域
        self.result_label = Label(
            text='點擊按鈕測試功能',
            font_size=14,
            size_hint_y=0.5,
            color=(0.7, 0.7, 0.7, 1)
        )
        root.add_widget(self.result_label)
        
        return root
    
    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size
    
    def on_test_press(self, instance):
        self.result_label.text = '按鈕點擊成功！\n\nApp 運行正常 ✓'
        self.status_label.text = '測試通過！'


if __name__ == '__main__':
    TWSEApp().run()
