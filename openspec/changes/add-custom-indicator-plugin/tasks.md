# Tasks: Add Custom Indicator Plugin System + AI Assistant

## Phase 1: Plugin Engine Core
- [ ] 1.1 Create `plugin_engine.py` with `PluginEngine` class
- [ ] 1.2 Implement `SafeExecutor` with restricted namespace
- [ ] 1.3 Implement `PluginValidator` for syntax/security checks
- [ ] 1.4 Create `plugin_helpers.py` with safe utility functions

## Phase 2: Plugin Data Management
- [ ] 2.1 Create `default_plugins.json` with 7 built-in plugins
- [ ] 2.2 Implement plugin CRUD (create/read/update/delete)
- [ ] 2.3 Add Supabase sync for user plugins (`user_plugins` table)
- [ ] 2.4 Add plugin export/import (JSON file)

## Phase 3: AI Integration (Gemini API)
- [ ] 3.1 Create `ai_generator.py` with Gemini API integration
- [ ] 3.2 Design system prompt for code generation
- [ ] 3.3 Create `ai_chat.py` screen for AI assistant
- [ ] 3.4 Implement chat history and context management
- [ ] 3.5 Add Gemini API Key input in settings screen

## Phase 4: UI - Query Screen (含 K 線圖)
- [ ] 4.1 Create `query.py` with search input
- [ ] 4.2 Create `chart_engine.py` for K-line rendering
- [ ] 4.3 Implement candlestick chart with day/week/month toggle
- [ ] 4.4 Add MA overlay toggles (MA3/20/60/120/200)
- [ ] 4.5 Create custom MA dialog (period, color, line style)
- [ ] 4.6 Add overlay lines (VP, VWAP, POC, stop-loss, take-profit)
- [ ] 4.7 Add volume sub-chart
- [ ] 4.8 Add indicator sub-chart (KD/MACD/RSI/MFI + custom plugins)

## Phase 5: UI - Scan Screen
- [ ] 5.1 Refactor `scan.py` to load plugins dynamically
- [ ] 5.2 Add "+ New Custom Scan" button (AI 生成 / 手動編寫)
- [ ] 5.3 Implement long-press menu for edit/delete/export

## Phase 6: UI - Plugin Editor
- [ ] 6.1 Create `plugin_editor.py` screen
- [ ] 6.2 Add multi-line code input with scrolling
- [ ] 6.3 Add syntax check button
- [ ] 6.4 Add test run button (preview with 10 stocks)
- [ ] 6.5 Add save/cancel buttons

## Phase 7: UI - Other Screens
- [ ] 7.1 Refactor `watchlist.py` (自選股)
- [ ] 7.2 Update `settings.py` with API Key inputs
- [ ] 7.3 Update `main.py` with 5-tab navigation

## Phase 8: Styling
- [ ] 8.1 Apply dark theme from `設定頁面/code.html`
- [ ] 8.2 Use green accent (#13ec5b) for primary actions
- [ ] 8.3 Add Material Design icons

## Phase 9: Testing & Build
- [ ] 9.1 Test locally on Windows with `python main.py`
- [ ] 9.2 Test plugin security: reject `import os`, `open()`, `eval()`
- [ ] 9.3 Test AI generation with sample prompts
- [ ] 9.4 Test chart rendering with real stock data
- [ ] 9.5 Test plugin export/import
- [ ] 9.6 Push to GitHub and trigger APK build
- [ ] 9.7 Install APK and verify on Android device
