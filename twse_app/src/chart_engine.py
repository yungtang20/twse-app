"""
Chart Engine - K 線圖渲染引擎
使用 Kivy Graphics 繪製 K 線圖和技術指標
"""
from kivy.graphics import Color, Line, Rectangle, Ellipse
from kivy.graphics.instructions import InstructionGroup
from kivy.uix.widget import Widget
from kivy.properties import ListProperty, NumericProperty, BooleanProperty, StringProperty
from typing import List, Dict, Tuple, Optional
from datetime import datetime


# 預設顏色配置 (深色主題)
COLORS = {
    'up': (0.847, 0.227, 0.227, 1),      # 紅色 - 上漲
    'down': (0.227, 0.729, 0.349, 1),    # 綠色 - 下跌
    'ma3': (1, 0.84, 0, 1),               # 金色
    'ma5': (1, 0.5, 0, 1),                # 橙色
    'ma10': (0.6, 0.4, 1, 1),             # 紫色
    'ma20': (0.2, 0.6, 1, 1),             # 藍色
    'ma60': (0, 0.8, 0.6, 1),             # 青色
    'ma120': (1, 0.4, 0.7, 1),            # 粉色
    'ma200': (1, 1, 1, 1),                # 白色
    'vp_upper': (1, 0.5, 0.5, 0.7),       # 淡紅
    'vp_lower': (0.5, 1, 0.5, 0.7),       # 淡綠
    'poc': (1, 1, 0, 0.8),                # 黃色
    'vwap': (0.4, 0.8, 1, 0.8),           # 天藍
    'stop_loss': (1, 0.2, 0.2, 0.8),      # 紅色
    'take_profit': (0.2, 1, 0.2, 0.8),    # 綠色
    'grid': (0.3, 0.3, 0.3, 0.5),         # 網格線
    'text': (0.8, 0.8, 0.8, 1),           # 文字
    'volume_up': (0.847, 0.227, 0.227, 0.8),
    'volume_down': (0.227, 0.729, 0.349, 0.8),
}


class ChartData:
    """K 線圖數據結構"""
    
    def __init__(self):
        self.dates: List[str] = []
        self.opens: List[float] = []
        self.highs: List[float] = []
        self.lows: List[float] = []
        self.closes: List[float] = []
        self.volumes: List[int] = []
        
        # 均線
        self.ma_data: Dict[int, List[float]] = {}
        
        # 技術指標
        self.indicators: Dict[str, List[float]] = {}
        
        # 疊加線
        self.overlays: Dict[str, float] = {}
    
    def load_from_list(self, data: List[Dict]):
        """從列表載入數據"""
        self.dates = [d.get('date', '') for d in data]
        self.opens = [float(d.get('open', 0)) for d in data]
        self.highs = [float(d.get('high', 0)) for d in data]
        self.lows = [float(d.get('low', 0)) for d in data]
        self.closes = [float(d.get('close', 0)) for d in data]
        self.volumes = [int(d.get('volume', 0)) for d in data]
    
    def set_ma(self, period: int, values: List[float]):
        """設定均線數據"""
        self.ma_data[period] = values
    
    def set_overlay(self, name: str, value: float):
        """設定疊加線 (水平線)"""
        self.overlays[name] = value
    
    def set_indicator(self, name: str, values: List[float]):
        """設定技術指標數據"""
        self.indicators[name] = values
    
    @property
    def length(self) -> int:
        return len(self.dates)
    
    @property
    def price_range(self) -> Tuple[float, float]:
        """價格範圍"""
        if not self.lows or not self.highs:
            return 0, 100
        return min(self.lows), max(self.highs)
    
    @property
    def volume_range(self) -> Tuple[int, int]:
        """成交量範圍"""
        if not self.volumes:
            return 0, 1000000
        return 0, max(self.volumes)


class ChartRenderer:
    """K 線圖渲染器"""
    
    def __init__(self, widget: Widget):
        self.widget = widget
        self.data: Optional[ChartData] = None
        
        # 顯示設定
        self.visible_count = 60          # 預設顯示 K 線數量
        self.scroll_offset = 0           # 捲動偏移
        
        # 均線開關
        self.ma_enabled: Dict[int, bool] = {
            3: False, 5: False, 10: False,
            20: True, 60: True, 120: False, 200: False
        }
        
        # 疊加線開關
        self.overlay_enabled: Dict[str, bool] = {
            'vp_upper': False,
            'vp_lower': False,
            'poc': False,
            'vwap': False,
            'stop_loss': False,
            'take_profit': False,
        }
        
        # 自訂均線
        self.custom_ma: List[Dict] = []
        
        # 圖表區域比例
        self.main_ratio = 0.6            # 主圖佔比
        self.volume_ratio = 0.15         # 成交量佔比
        self.indicator_ratio = 0.25      # 指標佔比
        
        # 當前選擇的指標
        self.current_indicator = 'KD'
    
    def set_data(self, data: ChartData):
        """設定圖表數據"""
        self.data = data
        self.scroll_offset = max(0, data.length - self.visible_count)
    
    def toggle_ma(self, period: int) -> bool:
        """切換均線顯示"""
        if period in self.ma_enabled:
            self.ma_enabled[period] = not self.ma_enabled[period]
            return self.ma_enabled[period]
        return False
    
    def toggle_overlay(self, name: str) -> bool:
        """切換疊加線顯示"""
        if name in self.overlay_enabled:
            self.overlay_enabled[name] = not self.overlay_enabled[name]
            return self.overlay_enabled[name]
        return False
    
    def add_custom_ma(self, period: int, color: Tuple, style: str = 'solid'):
        """新增自訂均線"""
        self.custom_ma.append({
            'period': period,
            'color': color,
            'style': style
        })
    
    def scroll(self, delta: int):
        """捲動圖表"""
        if not self.data:
            return
        
        new_offset = self.scroll_offset + delta
        max_offset = max(0, self.data.length - self.visible_count)
        self.scroll_offset = max(0, min(new_offset, max_offset))
    
    def zoom(self, factor: float):
        """縮放圖表"""
        new_count = int(self.visible_count * factor)
        self.visible_count = max(20, min(200, new_count))
    
    def render(self) -> InstructionGroup:
        """渲染完整圖表"""
        group = InstructionGroup()
        
        if not self.data or self.data.length == 0:
            return group
        
        # 計算區域
        w, h = self.widget.size
        x, y = self.widget.pos
        
        padding = 10
        chart_width = w - padding * 2
        chart_height = h - padding * 2
        
        main_height = chart_height * self.main_ratio
        volume_height = chart_height * self.volume_ratio
        indicator_height = chart_height * self.indicator_ratio
        
        main_y = y + indicator_height + volume_height + padding
        volume_y = y + indicator_height + padding
        indicator_y = y + padding
        
        # 渲染主圖
        group.add(self._render_main_chart(
            x + padding, main_y, chart_width, main_height
        ))
        
        # 渲染成交量
        group.add(self._render_volume_chart(
            x + padding, volume_y, chart_width, volume_height
        ))
        
        # 渲染指標
        group.add(self._render_indicator_chart(
            x + padding, indicator_y, chart_width, indicator_height
        ))
        
        # 渲染十字查價線
        if self.crosshair_x > 0:
            group.add(self._render_crosshair(
                x + padding, main_y, chart_width, main_height,
                x + padding, volume_y, volume_height,
                x + padding, indicator_y, indicator_height
            ))
        
        return group

    def on_touch(self, x: float, y: float) -> Optional[Dict]:
        """處理觸控事件，回傳選中的數據"""
        if not self.data:
            return None
            
        # 計算圖表區域
        wx, wy = self.widget.pos
        w, h = self.widget.size
        padding = 10
        chart_width = w - padding * 2
        chart_x = wx + padding
        
        # 檢查是否在圖表範圍內
        if not (chart_x <= x <= chart_x + chart_width):
            self.crosshair_x = -1
            return None
            
        # 計算選中的索引
        rel_x = x - chart_x
        bar_width = chart_width / self.visible_count
        idx_offset = int(rel_x / bar_width)
        
        start = self.scroll_offset
        idx = start + idx_offset
        
        if 0 <= idx < self.data.length:
            self.crosshair_x = x
            self.crosshair_idx = idx
            
            # 回傳數據
            return {
                'date': self.data.dates[idx],
                'open': self.data.opens[idx],
                'high': self.data.highs[idx],
                'low': self.data.lows[idx],
                'close': self.data.closes[idx],
                'volume': self.data.volumes[idx],
                'ma': {p: self.data.ma_data[p][idx] for p in self.ma_enabled if p in self.data.ma_data},
                'indicator': {k: v[idx] for k, v in self.data.indicators.items() if idx < len(v)}
            }
        
        self.crosshair_x = -1
        return None

    def _render_crosshair(self, x: float, main_y: float, width: float, main_height: float,
                          vol_x: float, vol_y: float, vol_height: float,
                          ind_x: float, ind_y: float, ind_height: float) -> InstructionGroup:
        """渲染十字查價線"""
        group = InstructionGroup()
        
        # 垂直線
        group.add(Color(0.5, 0.5, 0.5, 0.8))
        group.add(Line(points=[self.crosshair_x, ind_y, self.crosshair_x, main_y + main_height], width=1, dash_offset=4, dash_length=4))
        
        # 水平線 (跟隨收盤價)
        if self.crosshair_idx >= 0 and self.crosshair_idx < self.data.length:
            # 計算主圖 Y
            start = self.scroll_offset
            end = min(start + self.visible_count, self.data.length)
            visible_lows = [self.data.lows[i] for i in range(start, end)]
            visible_highs = [self.data.highs[i] for i in range(start, end)]
            
            if visible_lows and visible_highs:
                price_min = min(visible_lows) * 0.98
                price_max = max(visible_highs) * 1.02
                price_range = price_max - price_min
                
                c = self.data.closes[self.crosshair_idx]
                cy = main_y + (c - price_min) / price_range * main_height
                
                group.add(Line(points=[x, cy, x + width, cy], width=1, dash_offset=4, dash_length=4))
                
                # 顯示價格標籤
                group.add(Color(0.2, 0.2, 0.2, 0.9))
                group.add(Rectangle(pos=(x + width - 60, cy - 10), size=(60, 20)))
                # 注意: Kivy Graphics 無法直接繪製文字，需在 Widget 層處理 Label
        
        return group
    
    def _render_main_chart(self, x: float, y: float, 
                           width: float, height: float) -> InstructionGroup:
        """渲染主圖 (K 線 + 均線 + 疊加線)"""
        group = InstructionGroup()
        
        # 計算可見數據
        start = self.scroll_offset
        end = min(start + self.visible_count, self.data.length)
        visible_data = range(start, end)
        
        # 計算價格範圍
        visible_lows = [self.data.lows[i] for i in visible_data]
        visible_highs = [self.data.highs[i] for i in visible_data]
        
        if not visible_lows or not visible_highs:
            return group
        
        price_min = min(visible_lows) * 0.98
        price_max = max(visible_highs) * 1.02
        price_range = price_max - price_min
        
        if price_range == 0:
            return group
        
        # K 線寬度
        bar_width = width / self.visible_count
        candle_width = bar_width * 0.7
        
        # 繪製 K 線
        for i, idx in enumerate(visible_data):
            bx = x + i * bar_width + bar_width / 2
            
            o = self.data.opens[idx]
            h = self.data.highs[idx]
            l = self.data.lows[idx]
            c = self.data.closes[idx]
            
            # 計算 Y 座標
            open_y = y + (o - price_min) / price_range * height
            close_y = y + (c - price_min) / price_range * height
            high_y = y + (h - price_min) / price_range * height
            low_y = y + (l - price_min) / price_range * height
            
            # 顏色
            is_up = c >= o
            color = COLORS['up'] if is_up else COLORS['down']
            
            group.add(Color(*color))
            
            # 上下影線
            group.add(Line(points=[bx, low_y, bx, high_y], width=1))
            
            # K 線實體
            body_bottom = min(open_y, close_y)
            body_height = max(abs(close_y - open_y), 1)
            
            group.add(Rectangle(
                pos=(bx - candle_width / 2, body_bottom),
                size=(candle_width, body_height)
            ))
        
        # 繪製均線
        for period, enabled in self.ma_enabled.items():
            if enabled and period in self.data.ma_data:
                group.add(self._render_ma_line(
                    x, y, width, height,
                    self.data.ma_data[period],
                    start, end,
                    price_min, price_range,
                    COLORS.get(f'ma{period}', (1, 1, 1, 1))
                ))
        
        # 繪製疊加線
        for name, enabled in self.overlay_enabled.items():
            if enabled and name in self.data.overlays:
                value = self.data.overlays[name]
                if price_min <= value <= price_max:
                    vy = y + (value - price_min) / price_range * height
                    group.add(Color(*COLORS.get(name, (1, 1, 1, 0.5))))
                    group.add(Line(points=[x, vy, x + width, vy], width=1.5, dash_offset=4, dash_length=4))
        
        return group
    
    def _render_ma_line(self, x: float, y: float, width: float, height: float,
                        values: List[float], start: int, end: int,
                        price_min: float, price_range: float,
                        color: Tuple) -> InstructionGroup:
        """繪製均線"""
        group = InstructionGroup()
        group.add(Color(*color))
        
        bar_width = width / self.visible_count
        points = []
        
        for i, idx in enumerate(range(start, end)):
            if idx < len(values) and values[idx] > 0:
                mx = x + i * bar_width + bar_width / 2
                my = y + (values[idx] - price_min) / price_range * height
                points.extend([mx, my])
        
        if len(points) >= 4:
            group.add(Line(points=points, width=1.5))
        
        return group
    
    def _render_volume_chart(self, x: float, y: float,
                             width: float, height: float) -> InstructionGroup:
        """渲染成交量圖"""
        group = InstructionGroup()
        
        start = self.scroll_offset
        end = min(start + self.visible_count, self.data.length)
        
        # 計算最大成交量
        visible_volumes = [self.data.volumes[i] for i in range(start, end)]
        if not visible_volumes:
            return group
        
        max_vol = max(visible_volumes)
        if max_vol == 0:
            return group
        
        bar_width = width / self.visible_count
        vol_width = bar_width * 0.7
        
        for i, idx in enumerate(range(start, end)):
            bx = x + i * bar_width + bar_width / 2
            vol = self.data.volumes[idx]
            vol_height = (vol / max_vol) * height * 0.9
            
            # 顏色依漲跌
            is_up = self.data.closes[idx] >= self.data.opens[idx]
            color = COLORS['volume_up'] if is_up else COLORS['volume_down']
            
            group.add(Color(*color))
            group.add(Rectangle(
                pos=(bx - vol_width / 2, y),
                size=(vol_width, vol_height)
            ))
        
        return group
    
    def _render_indicator_chart(self, x: float, y: float,
                                width: float, height: float) -> InstructionGroup:
        """渲染指標圖 (KD/MACD/RSI/MFI)"""
        group = InstructionGroup()
        
        indicator = self.current_indicator
        
        if indicator == 'KD':
            group.add(self._render_kd(x, y, width, height))
        elif indicator == 'MACD':
            group.add(self._render_macd(x, y, width, height))
        elif indicator == 'RSI':
            group.add(self._render_rsi(x, y, width, height))
        elif indicator == 'MFI':
            group.add(self._render_mfi(x, y, width, height))
        
        return group
    
    def _render_kd(self, x: float, y: float,
                   width: float, height: float) -> InstructionGroup:
        """渲染 KD 指標"""
        group = InstructionGroup()
        
        k_values = self.data.indicators.get('K', [])
        d_values = self.data.indicators.get('D', [])
        
        if not k_values or not d_values:
            return group
        
        start = self.scroll_offset
        end = min(start + self.visible_count, len(k_values))
        bar_width = width / self.visible_count
        
        # K 線 (黃色)
        group.add(Color(1, 0.84, 0, 1))
        k_points = []
        for i, idx in enumerate(range(start, end)):
            if idx < len(k_values):
                kx = x + i * bar_width + bar_width / 2
                ky = y + (k_values[idx] / 100) * height
                k_points.extend([kx, ky])
        if len(k_points) >= 4:
            group.add(Line(points=k_points, width=1.5))
        
        # D 線 (藍色)
        group.add(Color(0.2, 0.6, 1, 1))
        d_points = []
        for i, idx in enumerate(range(start, end)):
            if idx < len(d_values):
                dx = x + i * bar_width + bar_width / 2
                dy = y + (d_values[idx] / 100) * height
                d_points.extend([dx, dy])
        if len(d_points) >= 4:
            group.add(Line(points=d_points, width=1.5))
        
        # 超買超賣線 (80/20)
        group.add(Color(0.5, 0.5, 0.5, 0.5))
        y80 = y + 0.8 * height
        y20 = y + 0.2 * height
        group.add(Line(points=[x, y80, x + width, y80], width=1, dash_offset=4, dash_length=4))
        group.add(Line(points=[x, y20, x + width, y20], width=1, dash_offset=4, dash_length=4))
        
        return group
    
    def _render_macd(self, x: float, y: float,
                     width: float, height: float) -> InstructionGroup:
        """渲染 MACD 指標"""
        group = InstructionGroup()
        # TODO: 實作 MACD 渲染
        return group
    
    def _render_rsi(self, x: float, y: float,
                    width: float, height: float) -> InstructionGroup:
        """渲染 RSI 指標"""
        group = InstructionGroup()
        # TODO: 實作 RSI 渲染
        return group
    
    def _render_mfi(self, x: float, y: float,
                    width: float, height: float) -> InstructionGroup:
        """渲染 MFI 指標"""
        group = InstructionGroup()
        
        mfi_values = self.data.indicators.get('MFI', [])
        if not mfi_values:
            return group
        
        start = self.scroll_offset
        end = min(start + self.visible_count, len(mfi_values))
        bar_width = width / self.visible_count
        
        # MFI 線 (紫色)
        group.add(Color(0.6, 0.4, 1, 1))
        points = []
        for i, idx in enumerate(range(start, end)):
            if idx < len(mfi_values):
                mx = x + i * bar_width + bar_width / 2
                my = y + (mfi_values[idx] / 100) * height
                points.extend([mx, my])
        if len(points) >= 4:
            group.add(Line(points=points, width=1.5))
        
        # 超買超賣線 (80/20)
        group.add(Color(0.5, 0.5, 0.5, 0.5))
        y80 = y + 0.8 * height
        y20 = y + 0.2 * height
        group.add(Line(points=[x, y80, x + width, y80], width=1, dash_offset=4, dash_length=4))
        group.add(Line(points=[x, y20, x + width, y20], width=1, dash_offset=4, dash_length=4))
        
        return group
