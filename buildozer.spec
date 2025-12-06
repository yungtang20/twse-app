[app]
title = 台股分析
package.name = twseapp
package.domain = tw.stock

# 原始碼目錄

source.dir = twse_app
source.include_exts = py,png,jpg,kv,atlas,ttf,sql,json

# 版本
version = 1.0.0

# 依賴項 (Python packages)
requirements = python3,kivy==2.3.0,requests,pillow,openssl

# Android 設定
android.permissions = INTERNET,WRITE_EXTERNAL_STORAGE,READ_EXTERNAL_STORAGE
android.api = 33
android.minapi = 21
android.ndk = 25b
android.archs = arm64-v8a
android.accept_sdk_license = True

# App 圖示與啟動畫面
# icon.filename = %(source.dir)s/data/icon.png
# presplash.filename = %(source.dir)s/data/presplash.png

# 方向
orientation = portrait

# 全螢幕
fullscreen = 0

# Android 主題
# android.theme = @android:style/Theme.NoTitleBar

[buildozer]
log_level = 2
warn_on_root = 1
