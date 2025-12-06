"""
設定畫面 - 完整版
支援 TWSE/TPEX 分開顯示、排程設定、主題切換
"""
from kivy.uix.screenmanager import Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.switch import Switch
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.uix.gridlayout import GridLayout
from kivy.uix.progressbar import ProgressBar
from kivy.graphics import Color, Rectangle, RoundedRectangle
from kivy.clock import Clock
from kivy.properties import StringProperty, BooleanProperty
import threading

try:
    from src.supabase_client import SupabaseClient
    from config import SUPABASE_URL, SUPABASE_KEY
except ImportError:
    SUPABASE_URL = ""
    SUPABASE_KEY = ""


class SettingsScreen(Screen):
    """設定"""
    
    # 資料狀態
    twse_cloud_date = StringProperty("載入中...")
    tpex_cloud_date = StringProperty("載入中...")
    twse_local_date = StringProperty("載入中...")
    tpex_local_date = StringProperty("載入中...")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.supabase = None
        if SUPABASE_URL and SUPABASE_KEY:
            self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        self._build_ui()
        
        # 載入狀態
        Clock.schedule_once(lambda dt: self._load_data_status(), 0.5)
    
    def _build_ui(self):
        main_layout = BoxLayout(orientation='vertical', padding=5, spacing=5)
        
        # 背景
        with main_layout.canvas.before:
            Color(0.063, 0.133, 0.086, 1)
            self.bg_rect = Rectangle(pos=main_layout.pos, size=main_layout.size)
        main_layout.bind(
            pos=lambda i, v: setattr(self.bg_rect, 'pos', v),
            size=lambda i, v: setattr(self.bg_rect, 'size', v)
        )
        
        scroll = ScrollView()
        content = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            spacing=10,
            padding=5
        )
        content.bind(minimum_height=content.setter('height'))
        
        # ===== 系統狀態 =====
        content.add_widget(self._create_section_header('📊 系統狀態'))
        status_layout = BoxLayout(orientation='vertical', size_hint_y=None, height=130, spacing=5)
        
        # 資料庫路徑 (可選擇)
        db_row = BoxLayout(size_hint_y=None, height=35, spacing=5)
        db_row.add_widget(Label(text='資料庫:', size_hint_x=0.25, font_size=13))
        self.db_path_label = Label(text='/sdcard/.../stock.db', size_hint_x=0.5, font_size=11, halign='left')
        self.db_path_label.bind(size=self.db_path_label.setter('text_size'))
        db_row.add_widget(self.db_path_label)
        btn_select_db = Button(text='選擇', size_hint_x=0.25, background_color=(0.2, 0.4, 0.3, 1))
        btn_select_db.bind(on_press=self._on_select_db_path)
        db_row.add_widget(btn_select_db)
        status_layout.add_widget(db_row)
        
        # 其他狀態
        status_grid = GridLayout(cols=2, spacing=5, size_hint_y=None, height=80)
        status_grid.add_widget(Label(text='股票總數:', halign='left', font_size=13))
        self.stock_count_label = Label(text='載入中...', halign='left', font_size=13)
        status_grid.add_widget(self.stock_count_label)
        status_grid.add_widget(Label(text='資料範圍:', halign='left', font_size=13))
        self.date_range_label = Label(text='載入中...', halign='left', font_size=13)
        status_grid.add_widget(self.date_range_label)
        status_layout.add_widget(status_grid)
        
        content.add_widget(status_layout)
        
        # ===== API Key 設定 =====
        content.add_widget(self._create_section_header('🔑 API Key 設定'))
        key_layout = BoxLayout(orientation='vertical', size_hint_y=None, height=140, spacing=5)
        
        # FinMind Token
        fm_row = BoxLayout(size_hint_y=None, height=35, spacing=5)
        fm_row.add_widget(Label(text='FinMind:', size_hint_x=0.2, font_size=13))
        self.finmind_input = TextInput(hint_text='Token...', password=True, multiline=False, size_hint_x=0.4, font_size=13)
        fm_row.add_widget(self.finmind_input)
        fm_save = Button(text='儲存', size_hint_x=0.15, background_color=(0.2, 0.4, 0.3, 1))
        fm_row.add_widget(fm_save)
        self.finmind_status = Label(text='✅ 可用', size_hint_x=0.25, font_size=12, color=(0.2, 0.8, 0.4, 1))
        fm_row.add_widget(self.finmind_status)
        key_layout.add_widget(fm_row)
        
        # Gemini API Key
        gm_row = BoxLayout(size_hint_y=None, height=35, spacing=5)
        gm_row.add_widget(Label(text='Gemini:', size_hint_x=0.2, font_size=13))
        self.gemini_input = TextInput(hint_text='API Key...', password=True, multiline=False, size_hint_x=0.4, font_size=13)
        gm_row.add_widget(self.gemini_input)
        gm_save = Button(text='儲存', size_hint_x=0.15, background_color=(0.2, 0.4, 0.3, 1))
        gm_save.bind(on_press=self._save_gemini_key)
        gm_row.add_widget(gm_save)
        self.gemini_status = Label(text='✅ 可用', size_hint_x=0.25, font_size=12, color=(0.2, 0.8, 0.4, 1))
        gm_row.add_widget(self.gemini_status)
        key_layout.add_widget(gm_row)
        
        content.add_widget(key_layout)
        
        # ===== Supabase 雲端 =====
        # ===== Supabase 雲端 =====
        supa_header = BoxLayout(size_hint_y=None, height=35)
        title_label = Label(text='☁️ Supabase 雲端', font_size=16, bold=True, halign='left', size_hint_x=0.6, color=(0.075, 0.925, 0.357, 1))
        title_label.bind(size=title_label.setter('text_size'))
        supa_header.add_widget(title_label)
        self.supabase_status = Label(text='✅ 連線正常', font_size=13, halign='right', size_hint_x=0.4, color=(0.2, 0.8, 0.4, 1))
        self.supabase_status.bind(size=self.supabase_status.setter('text_size'))
        supa_header.add_widget(self.supabase_status)
        content.add_widget(supa_header)
        cloud_layout = BoxLayout(orientation='vertical', size_hint_y=None, height=150, spacing=5)
        
        # 按鈕
        cloud_btns = BoxLayout(size_hint_y=None, height=40, spacing=5)
        btn_cloud_update = Button(text='更新三表', background_color=(0.075, 0.925, 0.357, 1))
        btn_cloud_update.bind(on_press=self._on_cloud_update)
        cloud_btns.add_widget(btn_cloud_update)
        btn_download = Button(text='下載到本地', background_color=(0.2, 0.4, 0.3, 1))
        cloud_btns.add_widget(btn_download)
        cloud_layout.add_widget(cloud_btns)
        
        # 資料狀態
        cloud_status = BoxLayout(orientation='vertical', size_hint_y=None, height=70, spacing=2)
        self.twse_cloud_label = Label(text='├ 上市(TWSE): ✅ 2025-12-06 (最新)', font_size=12, halign='left', color=(0.2, 0.8, 0.4, 1))
        self.twse_cloud_label.bind(size=self.twse_cloud_label.setter('text_size'))
        cloud_status.add_widget(self.twse_cloud_label)
        self.tpex_cloud_label = Label(text='├ 上櫃(TPEX): ⚠️ 2025-12-05 (落後 1 天)', font_size=12, halign='left', color=(0.9, 0.7, 0.2, 1))
        self.tpex_cloud_label.bind(size=self.tpex_cloud_label.setter('text_size'))
        cloud_status.add_widget(self.tpex_cloud_label)
        self.cloud_indicator_label = Label(text='└ 指標計算: ✅ 完成', font_size=12, halign='left', color=(0.2, 0.8, 0.4, 1))
        self.cloud_indicator_label.bind(size=self.cloud_indicator_label.setter('text_size'))
        cloud_status.add_widget(self.cloud_indicator_label)
        cloud_layout.add_widget(cloud_status)
        
        # 排程
        schedule_row = BoxLayout(size_hint_y=None, height=30)
        schedule_row.add_widget(Label(text='⏰ 自動排程: 每日 16:00', font_size=13, color=(0.6, 0.8, 0.7, 1)))
        cloud_layout.add_widget(schedule_row)
        
        content.add_widget(cloud_layout)
        
        # ===== 本地操作 =====
        content.add_widget(self._create_section_header('💾 本地操作'))
        local_layout = BoxLayout(orientation='vertical', size_hint_y=None, height=180, spacing=5)
        
        # 按鈕
        local_btns = BoxLayout(size_hint_y=None, height=40, spacing=5)
        btn_local_update = Button(text='更新三表', background_color=(0.075, 0.925, 0.357, 1))
        btn_local_update.bind(on_press=self._on_local_update)
        local_btns.add_widget(btn_local_update)
        btn_upload = Button(text='上傳到雲端', background_color=(0.2, 0.4, 0.3, 1))
        local_btns.add_widget(btn_upload)
        local_layout.add_widget(local_btns)
        
        # 資料狀態
        local_status = BoxLayout(orientation='vertical', size_hint_y=None, height=70, spacing=2)
        self.twse_local_label = Label(text='├ 上市(TWSE): ✅ 2025-12-06 (最新)', font_size=12, halign='left', color=(0.2, 0.8, 0.4, 1))
        self.twse_local_label.bind(size=self.twse_local_label.setter('text_size'))
        local_status.add_widget(self.twse_local_label)
        self.tpex_local_label = Label(text='├ 上櫃(TPEX): ⚠️ 2025-12-05 (落後 1 天)', font_size=12, halign='left', color=(0.9, 0.7, 0.2, 1))
        self.tpex_local_label.bind(size=self.tpex_local_label.setter('text_size'))
        local_status.add_widget(self.tpex_local_label)
        self.local_indicator_label = Label(text='└ 指標計算: ✅ 完成', font_size=12, halign='left', color=(0.2, 0.8, 0.4, 1))
        self.local_indicator_label.bind(size=self.local_indicator_label.setter('text_size'))
        local_status.add_widget(self.local_indicator_label)
        local_layout.add_widget(local_status)
        
        # 本地排程
        local_schedule_row = BoxLayout(size_hint_y=None, height=30, spacing=5)
        local_schedule_row.add_widget(Label(text='⏰ 本地排程:', font_size=12, size_hint_x=0.35))
        self.local_schedule_switch = Switch(active=False, size_hint_x=0.2)
        local_schedule_row.add_widget(self.local_schedule_switch)
        local_schedule_row.add_widget(Label(text='每日 16:00', font_size=12, size_hint_x=0.45, color=(0.6, 0.8, 0.7, 1)))
        local_layout.add_widget(local_schedule_row)
        
        content.add_widget(local_layout)
        
        # ===== 資料檢查 =====
        check_header = BoxLayout(size_hint_y=None, height=35)
        check_title = Label(text='🔧 資料檢查', font_size=16, bold=True, halign='left', size_hint_x=0.4, color=(0.075, 0.925, 0.357, 1))
        check_title.bind(size=check_title.setter('text_size'))
        check_header.add_widget(check_title)
        self.last_check_label = Label(text='最後: 2025-12-05 15:30', font_size=12, halign='right', size_hint_x=0.6, color=(0.6, 0.8, 0.7, 1))
        self.last_check_label.bind(size=self.last_check_label.setter('text_size'))
        check_header.add_widget(self.last_check_label)
        content.add_widget(check_header)
        
        check_container = BoxLayout(orientation='vertical', size_hint_y=None, height=115, spacing=5)
        
        # 按鈕
        check_layout = GridLayout(cols=2, size_hint_y=None, height=80, spacing=5)
        btn_check_missing = Button(text='檢查數據缺失', background_color=(0.2, 0.4, 0.3, 1))
        btn_check_missing.bind(on_press=self._on_check_missing)
        check_layout.add_widget(btn_check_missing)
        btn_clean = Button(text='清理下市股票', background_color=(0.2, 0.4, 0.3, 1))
        check_layout.add_widget(btn_clean)
        btn_verify = Button(text='驗證一致性並補漏', background_color=(0.2, 0.4, 0.3, 1))
        check_layout.add_widget(btn_verify)
        btn_calc = Button(text='計算技術指標', background_color=(0.2, 0.4, 0.3, 1))
        check_layout.add_widget(btn_calc)
        check_container.add_widget(check_layout)
        
        # 進度條
        self.check_progress = ProgressBar(max=100, value=0, size_hint_y=None, height=20)
        check_container.add_widget(self.check_progress)
        
        content.add_widget(check_container)
        
        # ===== 主題切換 =====
        content.add_widget(self._create_section_header('🎨 外觀'))
        theme_layout = BoxLayout(size_hint_y=None, height=50, spacing=10)
        theme_layout.add_widget(Label(text='深色主題', font_size=14))
        self.theme_switch = Switch(active=True)
        theme_layout.add_widget(self.theme_switch)
        content.add_widget(theme_layout)
        
        # ===== 關於 =====
        content.add_widget(self._create_section_header('ℹ️ 關於'))
        about_grid = GridLayout(cols=2, size_hint_y=None, height=60, spacing=5)
        about_grid.add_widget(Label(text='版本:', font_size=13))
        about_grid.add_widget(Label(text='v2.0.0', font_size=13))
        about_grid.add_widget(Label(text='資料來源:', font_size=13))
        about_grid.add_widget(Label(text='FinMind / TWSE / TPEX', font_size=13))
        content.add_widget(about_grid)
        
        scroll.add_widget(content)
        main_layout.add_widget(scroll)
        self.add_widget(main_layout)
    
    def _create_section_header(self, title: str):
        """建立區塊標題"""
        header = Label(
            text=title,
            font_size=16,
            bold=True,
            size_hint_y=None,
            height=35,
            halign='left',
            color=(0.075, 0.925, 0.357, 1)
        )
        header.bind(size=header.setter('text_size'))
        return header
    
    def _load_data_status(self):
        """載入資料狀態"""
        # TODO: 從 Supabase 和本地資料庫查詢實際日期
        self.twse_cloud_label.text = '2025-12-06'
        self.tpex_cloud_label.text = '2025-12-05 ⚠️'
        self.twse_local_label.text = '2025-12-06'
        self.tpex_local_label.text = '2025-12-05 ⚠️'
        self.stock_count_label.text = '1897 檔'
    
    def _on_cloud_update(self, *args):
        """雲端更新"""
        self.cloud_progress.value = 0
        # TODO: 呼叫 Supabase 更新邏輯
        Clock.schedule_interval(self._update_cloud_progress, 0.1)
    
    def _update_cloud_progress(self, dt):
        """更新雲端進度"""
        if self.cloud_progress.value < 100:
            self.cloud_progress.value += 2
        else:
            return False
    
    def _on_local_update(self, *args):
        """本地更新"""
        self.local_progress.value = 0
        # TODO: 呼叫本地更新邏輯
        Clock.schedule_interval(self._update_local_progress, 0.1)
    
    def _update_local_progress(self, dt):
        """更新本地進度"""
        if self.local_progress.value < 100:
            self.local_progress.value += 2
        else:
            return False
    
    def _save_gemini_key(self, *args):
        """儲存 Gemini API Key"""
        key = self.gemini_input.text.strip()
        if key:
            # TODO: 儲存到 config 或 secure storage
            # 更新 AI 助手
            app = self.manager.get_screen('ai_chat') if self.manager else None
            if app and hasattr(app, 'update_api_status'):
                app.update_api_status(key)
            print(f"Gemini API Key 已儲存")
    
    def _on_select_db_path(self, *args):
        """選擇資料庫路徑"""
        try:
            # Android 使用 file chooser
            from jnius import autoclass
            
            Intent = autoclass('android.content.Intent')
            intent = Intent(Intent.ACTION_OPEN_DOCUMENT)
            intent.addCategory(Intent.CATEGORY_OPENABLE)
            intent.setType('application/x-sqlite3')
            
            # TODO: 啟動 Activity 並處理結果
            
        except ImportError:
            # 非 Android 環境，使用 Kivy file chooser
            from kivy.uix.filechooser import FileChooserListView
            from kivy.uix.popup import Popup
            
            def select_path(instance):
                if instance.selection:
                    path = instance.selection[0]
                    self.db_path_label.text = path
                    # TODO: 儲存路徑到 config
                    print(f"選擇資料庫: {path}")
                popup.dismiss()
            
            chooser = FileChooserListView(
                path='.',
                filters=['*.db', '*.sqlite']
            )
            chooser.bind(on_submit=lambda x, y, z: select_path(x))
            
            popup = Popup(
                title='選擇資料庫檔案',
                content=chooser,
                size_hint=(0.9, 0.9)
            )
            popup.open()
    
    def _on_check_missing(self, *args):
        """檢查數據缺失"""
        self.check_progress.value = 0
        Clock.schedule_interval(self._update_check_progress, 0.1)
        
        # TODO: 呼叫實際的檢查邏輯
        import datetime
        self.last_check_label.text = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    
    def _update_check_progress(self, dt):
        """更新檢查進度"""
        if self.check_progress.value < 100:
            self.check_progress.value += 3
        else:
            return False

