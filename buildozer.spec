[app]
title = 台股分析
package.name = twseapp
package.domain = tw.stock

source.dir = .
source.include_exts = py,png,jpg,kv,atlas,json

version = 1.0.1

# 最小化依賴
requirements = python3,kivy==2.3.0

# Android 設定
android.permissions = INTERNET
android.api = 33
android.minapi = 21
android.ndk = 25b
android.archs = arm64-v8a
android.accept_sdk_license = True

orientation = portrait
fullscreen = 0

[buildozer]
log_level = 2
warn_on_root = 1
