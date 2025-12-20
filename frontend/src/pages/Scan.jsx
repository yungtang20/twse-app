import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../services/api'
import StockCard from '../components/StockCard'
import './Scan.css'

const SCAN_TYPES = [
    { id: 'vp', name: '📊 VP掃描', desc: '箱型壓力/支撐位分析', tags: ['技術面', '成交量'] },
    { id: 'mfi', name: '💰 MFI掃描', desc: '資金流向指標分析', tags: ['資金面', '超買超賣'] },
    { id: 'ma', name: '📈 均線掃描', desc: '多頭/空頭排列篩選', tags: ['趨勢', '均線'] },
    { id: 'kd-cross', name: '🔀 KD交叉', desc: '金叉/死叉訊號', tags: ['動能', 'KD'] },
    { id: 'vsbc', name: '🧮 VSBC策略', desc: '量價/箱型/籌碼綜合', tags: ['綜合', '籌碼'] },
    { id: 'smart-money', name: '🧠 聰明錢', desc: 'NVI主力籌碼追蹤', tags: ['主力', 'NVI'] },
]

function Scan() {
    const { type } = useParams()
    const [searchParams] = useSearchParams()
    const navigate = useNavigate()
    const [results, setResults] = useState([])
    const [loading, setLoading] = useState(false)
    const [scanInfo, setScanInfo] = useState(null)

    useEffect(() => {
        if (type) {
            runScan(type)
        }
    }, [type, searchParams])

    const runScan = async (scanType) => {
        setLoading(true)
        try {
            // 解析掃描類型和參數
            const [baseType] = scanType.split('?')
            const params = Object.fromEntries(searchParams.entries())

            const data = await api.scan(baseType, params)
            setResults(data?.results || [])
            setScanInfo(data)
        } catch (err) {
            console.error('掃描失敗:', err)
            setResults([])
        } finally {
            setLoading(false)
        }
    }

    if (!type) {
        // 顯示策略選擇頁面
        return (
            <div className="scan-page">
                <div className="page-header">
                    <h1>市場掃描</h1>
                    <p className="page-desc">選擇策略進行全市場掃描</p>
                </div>

                <div className="scan-strategy-grid">
                    {SCAN_TYPES.map((scan) => (
                        <div
                            key={scan.id}
                            className="strategy-card"
                            onClick={() => navigate(`/scan/${scan.id}`)}
                        >
                            <h3>{scan.name}</h3>
                            <p>{scan.desc}</p>
                            <div className="strategy-tags">
                                {scan.tags.map((tag) => (
                                    <span key={tag} className="tag">{tag}</span>
                                ))}
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        )
    }

    // 顯示掃描結果
    return (
        <div className="scan-page">
            <div className="page-header">
                <button className="back-btn" onClick={() => navigate('/scan')}>
                    ← 返回
                </button>
                <h1>{scanInfo?.scan_type || type} 掃描結果</h1>
                <span className="badge">{results.length} 檔</span>
            </div>

            {loading ? (
                <div className="loading">掃描中...</div>
            ) : (
                <div className="stock-list">
                    {results.length === 0 ? (
                        <div className="empty">無符合條件的股票</div>
                    ) : (
                        results.map((stock) => (
                            <StockCard
                                key={stock.code}
                                stock={stock}
                                onClick={() => navigate(`/stock/${stock.code}`)}
                            />
                        ))
                    )}
                </div>
            )}
        </div>
    )
}

export default Scan
