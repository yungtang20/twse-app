# 台灣股市分析系統 - 網頁應用

將原本的 Python CLI 程式 (`最終修正.py`) 轉換為響應式網頁應用。

## 專案結構

```
twse/
├── 最終修正.py          # 原始 Python 程式
├── taiwan_stock.db      # SQLite 資料庫
│
├── backend/             # 後端 (FastAPI)
│   ├── main.py          # 主程式入口
│   ├── requirements.txt # 依賴套件
│   ├── routers/         # API 路由
│   │   ├── stocks.py    # 股票 API
│   │   ├── scan.py      # 掃描 API
│   │   ├── ranking.py   # 排行 API
│   │   └── admin.py     # 管理 API
│   └── services/        # 服務模塊
│       └── db.py        # 資料庫服務
│
├── frontend/            # 前端 (React + Vite)
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   └── src/
│       ├── main.jsx     # 入口
│       ├── App.jsx      # 路由
│       ├── index.css    # 全域樣式
│       ├── components/  # 共用組件
│       ├── pages/       # 頁面組件
│       └── services/    # API 服務
│
└── web-demo/            # 範本 (純 HTML/CSS/JS)
```

## 快速開始

### 1. 後端啟動

```bash
cd backend
python -m venv venv
venv\Scripts\activate       # Windows
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

API 文件: http://localhost:8000/docs

### 2. 前端啟動

```bash
cd frontend
npm install
npm run dev
```

網頁: http://localhost:5173

## 功能對應

| 原 CLI 功能 | 網頁對應 |
|------------|---------|
| [1] 資料管理 | /admin (後台管理) |
| [2] 市場掃描 | /scan |
| [3] 法人排行 | /ranking |
| [4] 系統維護 | /admin |
| 個股查詢 | /stock/:code |

## API 端點

- `GET /api/stocks` - 股票清單
- `GET /api/stocks/{code}` - 個股詳情
- `GET /api/scan/{type}` - 市場掃描
- `GET /api/ranking/{entity}-{direction}` - 法人排行
- `GET /api/status` - 系統狀態

## 技術堆疊

- **後端**: FastAPI + SQLite (沿用原資料庫)
- **前端**: React 18 + Vite + React Router
- **樣式**: 深色主題 + 響應式設計
