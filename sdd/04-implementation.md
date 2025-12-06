# 實現指南 (Implementation Guide)

## 1. 編碼規範

### 1.1 Python 風格指南
- 遵循 PEP 8
- 使用 4 空格縮進
- 行長度限制 100 字符
- 使用 snake_case 命名函數和變量
- 使用 PascalCase 命名類

### 1.2 文檔字符串 (Docstring)
```python
def calculate_wma(series, period):
    """計算加權移動平均 (Weighted Moving Average)
    
    公式: WMA = Σ(價格 × 權重) / Σ權重
    權重為線性遞增: [1, 2, 3, ..., period]
    
    Args:
        series (np.array): 價格序列
        period (int): 計算周期
    
    Returns:
        np.array: WMA 值序列，不足周期的位置為 NaN
    
    Example:
        >>> prices = np.array([10, 11, 12, 13, 14])
        >>> wma = calculate_wma(prices, 3)
        >>> wma[-1]  # 最新的 WMA 值
        13.0
    """
```

### 1.3 註釋規範
```python
# 好的註釋: 解釋「為什麼」
# Android 環境下使用 DELETE 模式避免 WAL 的 mmap 文件鎖定問題
if IS_ANDROID:
    conn.execute("PRAGMA journal_mode=DELETE;")

# 不好的註釋: 重複代碼
# 設置 journal 模式為 DELETE
if IS_ANDROID:
    conn.execute("PRAGMA journal_mode=DELETE;")
```

## 2. 關鍵實現模式

### 2.1 Singleton 模式 (DatabaseManager)
```python
class DatabaseManager:
    _instance = None
    
    def __new__(cls):
        """確保全局唯一實例"""
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
```

**使用場景**: 全局共享資源 (數據庫連接池)

### 2.2 上下文管理器模式 (數據庫連接)
```python
@contextmanager
def get_connection(self, timeout=30):
    """自動管理連接生命週期"""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=timeout)
        yield conn
    finally:
        if conn:
            conn.close()

# 使用方式
with db_manager.get_connection() as conn:
    cur = conn.cursor()
    cur.execute("SELECT * FROM stock_data")
```

**優點**: 自動釋放資源，避免洩漏

### 2.3 策略模式 (數據源管理)
```python
class DataSource(ABC):
    @abstractmethod
    def fetch_history(self, stock_code, ...):
        pass

class FinMindDataSource(DataSource):
    def fetch_history(self, stock_code, ...):
        # FinMind API 實現

class OfficialAPIDataSource(DataSource):
    def fetch_history(self, stock_code, ...):
        # Official API 實現

# 使用
manager = DataSourceManager()
data = manager.fetch_history("2330")  # 自動選擇最佳數據源
```

**優點**: 易於擴展新數據源

### 2.4 工廠模式 (指標計算)
```python
class IndicatorCalculator:
    """純靜態方法工廠"""
    
    @staticmethod
    def calculate_wma(series, period):
        # WMA 實現
    
    @staticmethod
    def calculate_rsi(df, period=14):
        # RSI 實現

# 使用 (無需實例化)
wma = IndicatorCalculator.calculate_wma(prices, 20)
rsi = IndicatorCalculator.calculate_rsi(df, 14)
```

**優點**: 無狀態，線程安全

## 3. 性能優化最佳實踐

### 3.1 使用 numpy 向量化
```python
# ❌ 慢: 使用 Python 循環
deltas = []
for i in range(1, len(prices)):
    deltas.append(prices[i] - prices[i-1])

# ✅ 快: 使用 numpy 向量化
deltas = np.diff(prices)
```

### 3.2 批量數據庫操作
```python
# ❌ 慢: 逐筆插入
for row in data:
    cur.execute("INSERT INTO stock_data VALUES (?,?,?)", row)
    conn.commit()

# ✅ 快: 批量插入
cur.executemany("INSERT INTO stock_data VALUES (?,?,?)", data)
conn.commit()
```

### 3.3 快取計算結果
```python
# 全域快取
GLOBAL_INDICATOR_CACHE = {
    "data": None,
    "timestamp": None,
    "cache_duration": 3600
}

def get_cached_indicators():
    """檢查快取是否有效"""
    if GLOBAL_INDICATOR_CACHE["data"] is None:
        return None
    
    cache_time = datetime.fromisoformat(GLOBAL_INDICATOR_CACHE["timestamp"])
    elapsed = (datetime.now() - cache_time).total_seconds()
    
    if elapsed > GLOBAL_INDICATOR_CACHE["cache_duration"]:
        return None  # 過期
    
    return GLOBAL_INDICATOR_CACHE["data"]
```

## 4. 錯誤處理最佳實踐

### 4.1 分層錯誤處理
```python
# 低層: 捕獲並記錄
def fetch_from_api(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"API 請求失敗: {url}, {e}")
        return None

# 中層: 失敗轉移
def fetch_history(stock_code):
    data = fetch_from_finmind(stock_code)
    if data is None:
        data = fetch_from_official(stock_code)
    return data

# 高層: 用戶提示
try:
    result = download_all_stocks()
except Exception as e:
    print_flush(f"❌ 下載失敗: {e}")
    logging.exception("下載異常")
```

### 4.2 參數驗證
```python
def calculate_wma(series, period):
    """加上輸入驗證"""
    if len(series) < period:
        logging.warning(f"數據不足: len={len(series)}, period={period}")
        return np.full(len(series), np.nan)
    
    if period <= 0:
        raise ValueError(f"period 必須 > 0, 當前值: {period}")
    
    # 正常計算...
```

### 4.3 SQL 注入防護
```python
# ❌ 危險: 字符串拼接
code = user_input
cur.execute(f"SELECT * FROM stock_data WHERE code='{code}'")

# ✅ 安全: 參數化查詢
cur.execute("SELECT * FROM stock_data WHERE code=?", (code,))
```

## 5. 並發與線程安全

### 5.1 進度追蹤器線程安全
```python
class ProgressTracker:
    _lock = threading.Lock()  # 類級別鎖
    
    def update_lines(self, *messages, force=False):
        with ProgressTracker._lock:  # 自動加鎖
            # 更新邏輯
            sys.stdout.write(...)
            sys.stdout.flush()
```

### 5.2 數據庫連接隔離
```python
# ✅ 每個線程使用獨立連接
def download_worker(stock_code):
    with db_manager.get_connection() as conn:  # 新連接
        # 處理數據...
        conn.commit()

# 使用線程池
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(download_worker, code) for code in codes]
```

## 6. 測試建議

### 6.1 單元測試示例
```python
import unittest

class TestIndicatorCalculator(unittest.TestCase):
    def test_wma_calculation(self):
        """測試 WMA 計算正確性"""
        prices = np.array([10, 11, 12, 13, 14])
        wma = IndicatorCalculator.calculate_wma(prices, 3)
        
        # 手動計算: (12*1 + 13*2 + 14*3) / (1+2+3) = 13.0
        expected = 13.0
        self.assertAlmostEqual(wma[-1], expected, places=2)
    
    def test_wma_insufficient_data(self):
        """測試數據不足情況"""
        prices = np.array([10, 11])
        wma = IndicatorCalculator.calculate_wma(prices, 3)
        
        # 應返回 NaN
        self.assertTrue(np.all(np.isnan(wma)))
```

### 6.2 集成測試示例
```python
def test_download_and_calculate():
    """測試完整流程"""
    # 1. 下載數據
    manager = DataSourceManager()
    df = manager.fetch_history("2330", start_date="2024-01-01")
    
    assert df is not None
    assert len(df) > 0
    
    # 2. 計算指標
    rsi = IndicatorCalculator.calculate_rsi(df)
    assert rsi is not None
    assert 0 <= rsi <= 100
```

## 7. 常見陷阱與解決方案

### 7.1 日期格式不一致
```python
# 問題: 混合使用不同日期格式
# 解決: 統一使用 YYYY-MM-DD 格式

def standardize_date(date_str):
    """統一日期格式"""
    # 處理民國年
    if len(date_str) == 7 and date_str.isdigit():
        return roc_to_western_date(date_str)
    
    # 處理 ISO 格式
    try:
        return pd.to_datetime(date_str).strftime("%Y-%m-%d")
    except:
        return date_str
```

### 7.2 DataFrame 索引錯誤
```python
# 問題: 忘記重置索引導致序號不連續
df = df[df['close'] > 0]  # 過濾後索引不連續

# 解決: 重置索引
df = df.reset_index(drop=True)
```

### 7.3 內存洩漏
```python
# 問題: 大型 DataFrame 未及時釋放
all_data = []
for code in codes:
    df = fetch_data(code)
    all_data.append(df)  # 累積在記憶體

# 解決: 分批處理
for code in codes:
    df = fetch_data(code)
    process_and_save(df)  # 處理後立即釋放
    del df  # 明確釋放
```

## 8. 代碼審查檢查清單

### 8.1 功能性
- [ ] 實現了所有需求功能
- [ ] 邊界情況已處理
- [ ] 錯誤處理完整

### 8.2 性能
- [ ] 使用向量化操作
- [ ] 避免不必要的循環
- [ ] 批量數據庫操作

### 8.3 可讀性
- [ ] 有意義的變量名
- [ ] 適當的註釋
- [ ] 清晰的函數職責

### 8.4 安全性
- [ ] SQL 參數化查詢
- [ ] 輸入驗證
- [ ] 敏感信息不硬編碼

### 8.5 可維護性
- [ ] 模塊化設計
- [ ] DRY 原則
- [ ] 易於擴展

## 9. Git 提交規範

```bash
# 提交格式
<type>(<scope>): <subject>

# 類型
feat: 新功能
fix: 修復
refactor: 重構
docs: 文檔
test: 測試
perf: 性能優化

# 示例
feat(indicator): 實現 WMA 指標計算
fix(database): 修復 Android WAL 模式問題
refactor(datasource): 優化失敗轉移邏輯
```

## 10. 下一步行動

1. **當前**: 完成數據源層實現 (TASK-004 ~ TASK-007)
2. **接著**: 實現技術指標層 (TASK-008 ~ TASK-010)
3. **然後**: 集成業務邏輯層 (TASK-011 ~ TASK-014)
4. **最後**: UI 層與測試 (TASK-015 ~ TASK-023)
