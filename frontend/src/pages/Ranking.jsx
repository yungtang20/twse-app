import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import './Ranking.css'

const ENTITIES = [
    { id: 'foreign', name: '外資' },
    { id: 'trust', name: '投信' },
    { id: 'dealer', name: '自營商' },
]

const DIRECTIONS = [
    { id: 'buy', name: '買超' },
    { id: 'sell', name: '賣超' },
]

function Ranking() {
    const { entity = 'foreign', direction = 'buy' } = useParams()
    const navigate = useNavigate()
    const [results, setResults] = useState([])
    const [loading, setLoading] = useState(false)

    useEffect(() => {
        loadRanking(entity, direction)
    }, [entity, direction])

    const loadRanking = async (e, d) => {
        setLoading(true)
        try {
            const data = await api.ranking(e, d)
            setResults(data?.results || [])
        } catch (err) {
            console.error('載入排行失敗:', err)
            setResults([])
        } finally {
            setLoading(false)
        }
    }

    const handleEntityChange = (e) => {
        navigate(`/ranking/${e}/${direction}`)
    }

    const handleDirectionChange = (d) => {
        navigate(`/ranking/${entity}/${d}`)
    }

    const formatNumber = (num) => {
        if (!num) return '-'
        return num.toLocaleString('zh-TW', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    }

    const formatChange = (num) => {
        if (!num) return '-'
        const sign = num >= 0 ? '+' : ''
        return `${sign}${num.toFixed(2)}%`
    }

    const getColorClass = (value) => {
        if (!value) return ''
        return value > 0 ? 'up' : value < 0 ? 'down' : ''
    }

    return (
        <div className="ranking-page">
            <div className="page-header">
                <h1>法人買賣超排行</h1>
            </div>

            {/* 類型選擇 */}
            <div className="ranking-tabs">
                {DIRECTIONS.map((d) => (
                    <button
                        key={d.id}
                        className={`tab-btn ${direction === d.id ? 'active' : ''}`}
                        onClick={() => handleDirectionChange(d.id)}
                    >
                        {d.name}排行
                    </button>
                ))}
            </div>

            {/* 法人選擇 */}
            <div className="entity-tabs">
                {ENTITIES.map((e) => (
                    <button
                        key={e.id}
                        className={`entity-btn ${entity === e.id ? 'active' : ''}`}
                        onClick={() => handleEntityChange(e.id)}
                    >
                        {e.name}
                    </button>
                ))}
            </div>

            {/* 排行表格 */}
            <div className="ranking-table">
                {loading ? (
                    <div className="loading">載入中...</div>
                ) : (
                    <table>
                        <thead>
                            <tr>
                                <th>排名</th>
                                <th>代號</th>
                                <th>名稱</th>
                                <th>收盤價</th>
                                <th>漲跌%</th>
                                <th>{direction === 'buy' ? '買超' : '賣超'}張數</th>
                            </tr>
                        </thead>
                        <tbody>
                            {results.length === 0 ? (
                                <tr>
                                    <td colSpan="6" className="empty">無資料</td>
                                </tr>
                            ) : (
                                results.map((item) => (
                                    <tr
                                        key={item.code}
                                        onClick={() => navigate(`/stock/${item.code}`)}
                                        className="clickable"
                                    >
                                        <td>{item.rank}</td>
                                        <td className="code">{item.code}</td>
                                        <td>{item.name}</td>
                                        <td className={getColorClass(item.change_pct)}>
                                            {formatNumber(item.close)}
                                        </td>
                                        <td className={getColorClass(item.change_pct)}>
                                            {formatChange(item.change_pct)}
                                        </td>
                                        <td className={getColorClass(item.buy_sell)}>
                                            {item.buy_sell?.toLocaleString() || '-'}
                                        </td>
                                    </tr>
                                ))
                            )}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    )
}

export default Ranking
