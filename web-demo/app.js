/**
 * 台灣股市分析系統 - 前端互動邏輯
 * 
 * 這是一個範本，展示基本的頁面切換與互動邏輯
 * 實際開發時會使用 React + Vite 框架
 */

// ========================================
// 全域狀態
// ========================================
const state = {
    currentPage: 'home',
    currentScan: null,
    currentRanking: { type: 'buy', entity: 'foreign' }
};

// ========================================
// 頁面切換
// ========================================
function switchPage(pageName) {
    // 隱藏所有頁面
    document.querySelectorAll('.page').forEach(page => {
        page.classList.remove('active');
    });
    
    // 顯示目標頁面
    const targetPage = document.getElementById(`page-${pageName}`);
    if (targetPage) {
        targetPage.classList.add('active');
    }
    
    // 更新導航狀態
    document.querySelectorAll('.nav-link, .nav-item').forEach(link => {
        link.classList.remove('active');
        if (link.dataset.page === pageName) {
            link.classList.add('active');
        }
    });
    
    state.currentPage = pageName;
}

// ========================================
// 股票搜尋
// ========================================
function searchStock() {
    const input = document.getElementById('stockSearch');
    const code = input.value.trim();
    
    // 驗證股票代碼 (4位數字)
    if (!/^\d{4}$/.test(code)) {
        alert('請輸入4位數股票代號');
        return;
    }
    
    viewStock(code);
    input.value = '';
}

function viewStock(code) {
    // 更新個股詳情頁內容
    document.getElementById('detail-code').textContent = code;
    
    // 模擬載入股票資料 (實際開發時 API 呼叫)
    const mockStocks = {
        '2330': { name: '台積電', price: 1095.00, change: 2.34 },
        '2317': { name: '鴻海', price: 189.50, change: -1.05 },
        '2454': { name: '聯發科', price: 1380.00, change: 1.47 },
        '3034': { name: '聯詠', price: 518.00, change: -0.58 }
    };
    
    const stock = mockStocks[code] || { name: '未知', price: 0, change: 0 };
    
    document.getElementById('detail-name').textContent = stock.name;
    document.getElementById('detail-price').textContent = stock.price.toFixed(2);
    document.getElementById('detail-price').className = 'price ' + (stock.change >= 0 ? 'up' : 'down');
    
    const changeText = `${stock.change >= 0 ? '+' : ''}${(stock.price * stock.change / 100).toFixed(2)} (${stock.change >= 0 ? '+' : ''}${stock.change.toFixed(2)}%)`;
    document.getElementById('detail-change').textContent = changeText;
    document.getElementById('detail-change').className = 'change ' + (stock.change >= 0 ? 'up' : 'down');
    
    // 切換到個股詳情頁
    switchPage('stock');
}

// ========================================
// 市場掃描
// ========================================
function loadScan(scanType) {
    console.log('載入掃描:', scanType);
    
    // 顯示掃描結果區
    const resultsArea = document.getElementById('scan-results');
    resultsArea.classList.remove('hidden');
    
    // 更新標題
    const scanTitles = {
        'vp': 'VP 掃描結果 (箱型壓力/支撐)',
        'vp-up': 'VP上 - 突破壓力位',
        'vp-down': 'VP下 - 跌破支撐位',
        'mfi': 'MFI 掃描結果 (資金流向)',
        'mfi-oversold': 'MFI 超賣區 (MFI < 20)',
        'ma': '均線掃描結果',
        'ma-bull': '多頭排列股票',
        'kd': 'KD 交叉訊號',
        'vsbc': 'VSBC 籌碼策略',
        'smart': '聰明錢 NVI 訊號',
        '2560': '2560 戰法'
    };
    
    document.getElementById('scan-title').textContent = scanTitles[scanType] || '掃描結果';
    
    // 模擬載入結果 (實際開發時 API 呼叫)
    const resultsTable = document.getElementById('results-table');
    resultsTable.innerHTML = `
        <div class="stock-list">
            <div class="stock-card" onclick="viewStock('2330')">
                <div class="stock-info">
                    <span class="stock-code">2330</span>
                    <span class="stock-name">台積電</span>
                </div>
                <div class="stock-price">
                    <span class="price up">1,095.00</span>
                    <span class="change up">+2.34%</span>
                </div>
            </div>
            <div class="stock-card" onclick="viewStock('2454')">
                <div class="stock-info">
                    <span class="stock-code">2454</span>
                    <span class="stock-name">聯發科</span>
                </div>
                <div class="stock-price">
                    <span class="price up">1,380.00</span>
                    <span class="change up">+1.47%</span>
                </div>
            </div>
            <div class="stock-card" onclick="viewStock('3034')">
                <div class="stock-info">
                    <span class="stock-code">3034</span>
                    <span class="stock-name">聯詠</span>
                </div>
                <div class="stock-price">
                    <span class="price down">518.00</span>
                    <span class="change down">-0.58%</span>
                </div>
            </div>
        </div>
    `;
    
    document.getElementById('scan-count').textContent = '3 檔';
    
    state.currentScan = scanType;
}

// ========================================
// 法人排行
// ========================================
function loadRanking(rankingType) {
    console.log('載入排行:', rankingType);
    
    // 切換到排行頁
    switchPage('ranking');
    
    // 解析類型 (如 'foreign-buy')
    const [entity, type] = rankingType.split('-');
    
    // 更新 UI 狀態
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.type === type);
    });
    
    document.querySelectorAll('.entity-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.entity === entity);
    });
    
    state.currentRanking = { type, entity };
}

// ========================================
// 事件監聽器
// ========================================
document.addEventListener('DOMContentLoaded', () => {
    // 導航連結點擊
    document.querySelectorAll('.nav-link, .nav-item').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const page = link.dataset.page;
            if (page) {
                switchPage(page);
            }
        });
    });
    
    // 搜尋框 Enter 鍵
    document.getElementById('stockSearch').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            searchStock();
        }
    });
    
    // 排行類型切換
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const type = btn.dataset.type;
            loadRanking(`${state.currentRanking.entity}-${type}`);
        });
    });
    
    // 法人類型切換
    document.querySelectorAll('.entity-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const entity = btn.dataset.entity;
            loadRanking(`${entity}-${state.currentRanking.type}`);
        });
    });
    
    // 初始化
    updateSystemStatus();
});

// ========================================
// 系統狀態更新
// ========================================
function updateSystemStatus() {
    // 模擬 API 呼叫取得系統狀態
    const now = new Date();
    document.getElementById('lastUpdate').textContent = 
        `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
}

// ========================================
// API 呼叫 (範本)
// ========================================
const API = {
    baseUrl: 'http://localhost:8000/api',
    
    async get(endpoint) {
        try {
            const response = await fetch(`${this.baseUrl}${endpoint}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (error) {
            console.error('API Error:', error);
            return null;
        }
    },
    
    // 取得股票清單
    async getStocks() {
        return this.get('/stocks');
    },
    
    // 取得單一股票資料
    async getStock(code) {
        return this.get(`/stocks/${code}`);
    },
    
    // 取得股票歷史 K 線
    async getHistory(code, limit = 60) {
        return this.get(`/stocks/${code}/history?limit=${limit}`);
    },
    
    // 執行掃描
    async scan(type) {
        return this.get(`/scan/${type}`);
    },
    
    // 取得排行榜
    async getRanking(type) {
        return this.get(`/ranking/${type}`);
    }
};

// ========================================
// 工具函數
// ========================================

/**
 * 格式化數字 (加入千分位)
 */
function formatNumber(num, decimals = 2) {
    return num.toLocaleString('zh-TW', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

/**
 * 格式化漲跌幅
 */
function formatChange(change) {
    const sign = change >= 0 ? '+' : '';
    return `${sign}${change.toFixed(2)}%`;
}

/**
 * 取得顏色類別
 */
function getColorClass(value) {
    if (value > 0) return 'up';
    if (value < 0) return 'down';
    return '';
}

/**
 * 防抖函數
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

console.log('📈 台灣股市分析系統 - 前端範本已載入');
