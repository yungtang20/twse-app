# APK 打包指南 (使用 Google Colab)

本指南說明如何使用 Google Colab 打包 TWSE 股市分析 APK，無需安裝 Android Studio。

## 架構說明

APK 支援兩種讀取模式（在「系統設定」中切換）：
- **本地模式** → 讀取手機內建 SQLite 資料庫（完全離線）
- **雲端模式** → 讀取 Supabase 雲端資料

---

## 手機版資料庫路徑

```
Android: /data/data/com.twse.app/databases/taiwan_stock.db
```

---

## 資料庫同步方式

### 方法 1：上傳到 Supabase Storage（推薦）

1. 在 PC 執行 `最終修正.py` 更新本地資料庫
2. 將 `taiwan_stock.db` 上傳到 Supabase Storage 的 `databases` bucket
3. APK 啟動時會自動檢查並下載最新資料庫

### 方法 2：手動複製

1. 將 `taiwan_stock.db` 複製到手機
2. 使用檔案管理器移動到 `/data/data/com.twse.app/databases/`
3. 需要 Root 權限或使用 ADB

---

## 前置準備

1. 在本機執行前端建置：
```bash
cd d:\twse\frontend
npm run build
```

2. 初始化 Capacitor Android 專案：
```bash
npx cap init "TWSE 股市分析" com.twse.app --web-dir dist
npx cap add android
npx cap sync
```

3. 將 `frontend/android` 資料夾壓縮成 `android.zip`

---

## Colab 打包步驟

在 Google Colab 中執行以下代碼：

```python
# 1. 安裝 Android SDK
!apt-get update
!apt-get install -y openjdk-17-jdk wget unzip

import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = os.environ['JAVA_HOME'] + '/bin:' + os.environ['PATH']

!wget -q https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip
!unzip -q commandlinetools-linux-11076708_latest.zip -d android-sdk
!mkdir -p android-sdk/cmdline-tools/latest
!mv android-sdk/cmdline-tools/* android-sdk/cmdline-tools/latest/ 2>/dev/null || true

os.environ['ANDROID_HOME'] = '/content/android-sdk'
os.environ['PATH'] = os.environ['ANDROID_HOME'] + '/cmdline-tools/latest/bin:' + os.environ['PATH']

# 2. 安裝 SDK 元件
!yes | sdkmanager --licenses
!sdkmanager "platform-tools" "platforms;android-34" "build-tools;34.0.0"

# 3. 上傳並解壓縮 android.zip
from google.colab import files
uploaded = files.upload()
!unzip -q android.zip -d /content/

# 4. 建置 APK
os.chdir('/content/android')
!chmod +x gradlew
!./gradlew assembleDebug

# APK 打包指南 (使用 Google Colab)

本指南說明如何使用 Google Colab 打包 TWSE 股市分析 APK，無需安裝 Android Studio。

## 架構說明

APK 支援兩種讀取模式（在「系統設定」中切換）：
- **本地模式** → 讀取手機內建 SQLite 資料庫（完全離線）
- **雲端模式** → 讀取 Supabase 雲端資料

---

## 手機版資料庫路徑

```
Android: /data/data/com.twse.app/databases/taiwan_stock.db
```

---

## 資料庫同步方式

### 方法 1：上傳到 Supabase Storage（推薦）

1. 在 PC 執行 `最終修正.py` 更新本地資料庫
2. 將 `taiwan_stock.db` 上傳到 Supabase Storage 的 `databases` bucket
3. APK 啟動時會自動檢查並下載最新資料庫

### 方法 2：手動複製

1. 將 `taiwan_stock.db` 複製到手機
2. 使用檔案管理器移動到 `/data/data/com.twse.app/databases/`
3. 需要 Root 權限或使用 ADB

---

## 前置準備

1. 在本機執行前端建置：
```bash
cd d:\twse\frontend
npm run build
```

2. 初始化 Capacitor Android 專案：
```bash
npx cap init "TWSE 股市分析" com.twse.app --web-dir dist
npx cap add android
npx cap sync
```

3. 將 `frontend/android` 資料夾壓縮成 `android.zip`

---

## Colab 打包步驟

在 Google Colab 中執行以下代碼：

```python
# 1. 安裝 Android SDK
!apt-get update
!apt-get install -y openjdk-17-jdk wget unzip

import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = os.environ['JAVA_HOME'] + '/bin:' + os.environ['PATH']

!wget -q https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip
!unzip -q commandlinetools-linux-11076708_latest.zip -d android-sdk
!mkdir -p android-sdk/cmdline-tools/latest
!mv android-sdk/cmdline-tools/* android-sdk/cmdline-tools/latest/ 2>/dev/null || true

os.environ['ANDROID_HOME'] = '/content/android-sdk'
os.environ['PATH'] = os.environ['ANDROID_HOME'] + '/cmdline-tools/latest/bin:' + os.environ['PATH']

# 2. 安裝 SDK 元件
!yes | sdkmanager --licenses
!sdkmanager "platform-tools" "platforms;android-34" "build-tools;34.0.0"

# 3. 上傳並解壓縮 android.zip
from google.colab import files
uploaded = files.upload()
!unzip -q android.zip -d /content/

# 4. 建置 APK
os.chdir('/content/android')
!chmod +x gradlew
!./gradlew assembleDebug

# 5. 下載 APK
files.download('/content/android/app/build/outputs/apk/debug/app-debug.apk')
```

---

## GitHub Actions 自動打包 (推薦)

本專案已整合 GitHub Actions，只需將程式碼推送到 GitHub，系統即會自動打包 APK。

1. **推送程式碼**：
   ```bash
   git add .
   git commit -m "Update app"
   git push origin main
   ```

2. **下載 APK**：
   - 前往 GitHub Repository 的 **Actions** 頁籤
   - 點選最新的 Workflow Run
   - 在 **Artifacts** 區塊下載 `app-debug`

---

## 手機版固定路徑注意事項

- Debug APK 僅供測試，正式發布需簽章
- 確保手機已開啟「允許未知來源安裝」
- 本地模式需先下載資料庫才能使用
