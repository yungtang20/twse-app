# TWSE App
台股分析 Android App (Kivy)

## 專案結構
```
twse_app/
├── main.py                    # Kivy 入口
├── buildozer.spec             # Buildozer 設定
├── supabase_schema.sql        # 資料庫 Schema
├── src/                       # 核心邏輯
│   ├── supabase_client.py    # Supabase API 封裝
│   ├── indicator_calculator.py
│   └── data_fetcher.py
├── screens/                   # UI 畫面
│   ├── home.py
│   ├── scan.py
│   ├── watchlist.py
│   └── settings.py
├── widgets/                   # 自訂元件
└── .github/workflows/         # CI/CD
    └── build.yml
```

## 開發環境設定
1. 安裝依賴：`pip install kivy requests pandas`
2. 本地測試：`python main.py`
3. 建立 APK：`buildozer android debug`

## 雲端打包
推送至 GitHub 後自動觸發 Actions 編譯 APK。
