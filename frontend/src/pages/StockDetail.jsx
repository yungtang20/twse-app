import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import './StockDetail.css'

function StockDetail() {
    const { code } = useParams()
    const navigate = useNavigate()
    const [stock, setStock] = useState(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)

    useEffect(() => {
        loadStock(code)
    }, [code])

    const loadStock = async (stockCode) => {
        setLoading(true)
        setError(null)
        try {
            const data = await api.getStock(stockCode)
            setStock(data)
        } catch (err) {
            console.error('載入股票失敗:', err)
            setError('找不到此股票')
        } finally {
            setLoading(false)
        }
    }

    const formatNumber = (num, decimals = 2) => {
        if (num === null || num === undefined) return '-'
        return num.toLocaleString('zh-TW', {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        })
    }

    const getColorClass = (value) => {
        if (!value) return ''
        return value > 0 ? 'up' : value < 0 ? 'down' : ''
    }

    if (loading) {
        return <div className="stock-detail loading">載入中...</div>
    }

    if (error) {
        return (
            <div className="stock-detail error">
                <p>{error}</p>
                <button onClick={() => navigate(-1)}>返回</button>
            </div>
        )
    }

    const changeValue = stock?.close * (stock?.change_pct || 0) / 100

    return (
        <div className="stock-detail">
            <button className="back-btn" onClick={() => navigate(-1)}>
                ← 返回
            </button>

            {/* 股票基本資訊 */}
            <div className="stock-header">
                <div className="stock-main-info">
                    <h1>{stock?.code}</h1>
                    <span className="stock-name">{stock?.name}</span>
                    <span className="stock-market">{stock?.market}</span>
                </div>
                <div className="stock-main-price">
                    <span className={`price ${getColorClass(stock?.change_pct)}`}>
                        {formatNumber(stock?.close)}
                    </span>
                    <span className={`change ${getColorClass(stock?.change_pct)}`}>
                        {stock?.change_pct >= 0 ? '+' : ''}{formatNumber(changeValue)}
                        ({stock?.change_pct >= 0 ? '+' : ''}{stock?.change_pct?.toFixed(2)}%)
                    </span>
                </div>
            </div>

            {/* K線圖區 (佔位) */}
            <div className="chart-container">
                <div className="chart-placeholder">
                    <p>📊 K線圖區域</p>
                    <p className="chart-hint">(開發中 - 將整合 TradingView Widget)</p>
                </div>
            </div>

            {/* 技術指標 */}
            <div className="indicators-grid">
                <div className="indicator-card">
                    <span className="ind-label">MA5</span>
                    <span className="ind-value">{formatNumber(stock?.ma5)}</span>
                </div>
                <div className="indicator-card">
                    <span className="ind-label">MA20</span>
                    <span className="ind-value">{formatNumber(stock?.ma20)}</span>
                </div>
                <div className="indicator-card">
                    <span className="ind-label">MA60</span>
                    <span className="ind-value">{formatNumber(stock?.ma60)}</span>
                </div>
                <div className="indicator-card">
                    <span className="ind-label">RSI</span>
                    <span className="ind-value">{formatNumber(stock?.rsi, 1)}</span>
                </div>
                <div className="indicator-card">
                    <span className="ind-label">MFI</span>
                    <span className="ind-value">{formatNumber(stock?.mfi, 1)}</span>
                </div>
                <div className="indicator-card">
                    <span className="ind-label">KD(K)</span>
                    <span className="ind-value">{formatNumber(stock?.k, 1)}</span>
                </div>
            </div>

            {/* 成交量資訊 */}
            <div className="volume-section">
                <h3>成交資訊</h3>
                <div className="volume-grid">
                    <div className="volume-item">
                        <span className="label">成交量</span>
                        <span className="value">{stock?.volume?.toLocaleString() || '-'} 張</span>
                    </div>
                    <div className="volume-item">
                        <span className="label">成交額</span>
                        <span className="value">{stock?.amount ? `${(stock.amount / 100000000).toFixed(2)} 億` : '-'}</span>
                    </div>
                </div>
            </div>

            {/* 法人買賣超 */}
            <div className="institutional-section">
                <h3>法人買賣超</h3>
                <div className="institutional-bars">
                    <div className="inst-row">
                        <span className="inst-name">外資</span>
                        <div className="inst-bar">
                            <div
                                className={`bar-fill ${stock?.foreign_buy >= 0 ? 'buy' : 'sell'}`}
                                style={{ width: '50%' }}
                            ></div>
                        </div>
                        <span className={`inst-value ${getColorClass(stock?.foreign_buy)}`}>
                            {stock?.foreign_buy >= 0 ? '+' : ''}{stock?.foreign_buy?.toLocaleString() || '-'}
                        </span>
                    </div>
                    <div className="inst-row">
                        <span className="inst-name">投信</span>
                        <div className="inst-bar">
                            <div
                                className={`bar-fill ${stock?.trust_buy >= 0 ? 'buy' : 'sell'}`}
                                style={{ width: '30%' }}
                            ></div>
                        </div>
                        <span className={`inst-value ${getColorClass(stock?.trust_buy)}`}>
                            {stock?.trust_buy >= 0 ? '+' : ''}{stock?.trust_buy?.toLocaleString() || '-'}
                        </span>
                    </div>
                    <div className="inst-row">
                        <span className="inst-name">自營商</span>
                        <div className="inst-bar">
                            <div
                                className={`bar-fill ${stock?.dealer_buy >= 0 ? 'buy' : 'sell'}`}
                                style={{ width: '20%' }}
                            ></div>
                        </div>
                        <span className={`inst-value ${getColorClass(stock?.dealer_buy)}`}>
                            {stock?.dealer_buy >= 0 ? '+' : ''}{stock?.dealer_buy?.toLocaleString() || '-'}
                        </span>
                    </div>
                </div>
            </div>

            {/* VP 價值區間 */}
            {stock?.vp_high && stock?.vp_low && (
                <div className="vp-section">
                    <h3>VP 價值區間</h3>
                    <div className="vp-grid">
                        <div className="vp-item">
                            <span className="label">壓力位 (VP High)</span>
                            <span className="value">{formatNumber(stock.vp_high)}</span>
                        </div>
                        <div className="vp-item">
                            <span className="label">POC</span>
                            <span className="value">{formatNumber(stock.vp_poc)}</span>
                        </div>
                        <div className="vp-item">
                            <span className="label">支撐位 (VP Low)</span>
                            <span className="value">{formatNumber(stock.vp_low)}</span>
                        </div>
                    </div>
                </div>
            )}
        </div>
    )
}

export default StockDetail
