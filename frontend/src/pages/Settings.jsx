function Settings() {
    return (
        <div className="settings-page">
            <div className="page-header">
                <h1>設定</h1>
            </div>

            <div className="settings-section">
                <h2>系統資訊</h2>
                <div className="settings-item">
                    <span className="label">版本</span>
                    <span className="value">1.0.0</span>
                </div>
                <div className="settings-item">
                    <span className="label">技術堆疊</span>
                    <span className="value">React + FastAPI + SQLite</span>
                </div>
            </div>

            <div className="settings-section">
                <h2>資料管理</h2>
                <p className="hint">資料更新功能需透過後台管理介面執行</p>
            </div>

            <div className="settings-section">
                <h2>關於</h2>
                <p>台灣股市分析系統 - 技術指標掃描、法人買賣超排行、個股分析</p>
                <p className="hint">原始程式碼：最終修正.py (12,037 行)</p>
            </div>
        </div>
    )
}

export default Settings
