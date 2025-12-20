import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import StockCard from '../components/StockCard'
import './Home.css'

function Home() {
    const [status, setStatus] = useState(null)
    const [loading, setLoading] = useState(true)
    const navigate = useNavigate()

    useEffect(() => {
        loadStatus()
    }, [])

    const loadStatus = async () => {
        try {
            const data = await api.getStatus()
            setStatus(data)
        } catch (err) {
            console.error('載入系統狀態失敗:', err)
        } finally {
            setLoading(false)
        }
    }

    const quickScans = [
        { id: 'vp?direction=support', icon: '📈', name: 'VP上', desc: '突破壓力位' },
        { id: 'vp?direction=resistance', icon: '📉', name: 'VP下', desc: '跌破支撐位' },
        { id: 'mfi?condition=oversold', icon: '💰', name: 'MFI超賣', desc: 'MFI < 20' },
        { id: 'ma?pattern=bull', icon: '🐂', name: '多頭排列', desc: '均線上揚' },
    ]

    // 模擬熱門股票
    const hotStocks = [
        { code: '2330', name: '台積電', close: 1095.00, change_pct: 2.34 },
        { code: '2317', name: '鴻海', close: 189.50, change_pct: -1.05 },
        { code: '2454', name: '聯發科', close: 1380.00, change_pct: 1.47 },
    ]

    return (
        <div className="home">
            {/* 系統狀態卡片 */}
            <div className="card system-status">
                <h2 className="card-title">
                    <span className={`status-dot ${status ? 'online' : ''}`}></span>
                    系統狀態
                </h2>
                {loading ? (
                    <div className="loading">載入中...</div>
                ) : (
                    <div className="status-grid">
                        <div className="status-item">
                            <span className="status-label">資料庫</span>
                            <span className="status-value">{status?.db_path || 'N/A'}</span>
                        </div>
                        <div className="status-item">
                            <span className="status-label">最新更新</span>
                            <span className="status-value">{status?.latest_date || 'N/A'}</span>
                        </div>
                        <div className="status-item">
                            <span className="status-label">股票總數</span>
                            <span className="status-value">{status?.stock_count?.toLocaleString() || 0}</span>
                        </div>
                        <div className="status-item">
                            <span className="status-label">資料庫大小</span>
                            <span className="status-value">{status?.db_size_mb || 0} MB</span>
                        </div>
                    </div>
                )}
            </div>

            {/* 快速掃描 */}
            <h2 className="section-title">快速掃描</h2>
            <div className="quick-scan-grid">
                {quickScans.map((scan) => (
                    <button
                        key={scan.id}
                        className="scan-card"
                        onClick={() => navigate(`/scan/${scan.id}`)}
                    >
                        <div className="scan-icon">{scan.icon}</div>
                        <div className="scan-name">{scan.name}</div>
                        <div className="scan-desc">{scan.desc}</div>
                    </button>
                ))}
            </div>

            {/* 今日熱門 */}
            <h2 className="section-title">今日熱門</h2>
            <div className="stock-list">
                {hotStocks.map((stock) => (
                    <StockCard
                        key={stock.code}
                        stock={stock}
                        onClick={() => navigate(`/stock/${stock.code}`)}
                    />
                ))}
            </div>
        </div>
    )
}

export default Home
