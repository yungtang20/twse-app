import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import { TechnicalChart } from '@/components/TechnicalChart'
import { useMobileView } from "@/context/MobileViewContext"
import './StockDetail.css'

function StockDetail() {
    const { code } = useParams()
    const navigate = useNavigate()
    const { isMobileView } = useMobileView()
    const [stock, setStock] = useState(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)
    const [hoverData, setHoverData] = useState(null)
    const [prevData, setPrevData] = useState(null)

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

    const handleHoverUpdate = (current, prev) => {
        setHoverData(current);
        setPrevData(prev);
    };

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

    const priceChange = hoverData && prevData ? (hoverData.close - prevData.close).toFixed(2) : '0';
    const priceChangePercent = hoverData && prevData ? ((hoverData.close - prevData.close) / prevData.close * 100).toFixed(2) : '0';
    const volChange = hoverData && prevData ? (hoverData.value - prevData.value) : 0;

    return (
        <div className={`bg-slate-900 min-h-screen p-3 ${isMobileView ? 'flex justify-center' : ''}`}>
            <div className={`w-full transition-all duration-300 ${isMobileView ? 'max-w-[375px]' : ''}`}>

                <div className="flex items-center gap-2 mb-2">
                    <button
                        className="bg-slate-800 text-slate-300 px-3 py-1 rounded hover:bg-slate-700 transition-colors text-sm"
                        onClick={() => navigate(-1)}
                    >
                        ← 返回
                    </button>
                </div>

                {/* 股票資訊欄 (與儀表板一致) */}
                <div className={`bg-slate-800 rounded px-3 py-2 mb-2 text-sm text-slate-300 flex ${isMobileView ? 'flex-col items-start gap-2' : 'flex-wrap gap-4 items-center'}`}>
                    <div className={`flex ${isMobileView ? 'flex-col gap-1 w-full' : 'gap-3 items-center flex-wrap'}`}>
                        <div className="flex justify-between items-center">
                            <span className="text-white font-bold text-lg">{stock?.name} ({stock?.code})</span>
                            <span className="text-slate-400 text-xs ml-2">{hoverData?.time || '-'}</span>
                        </div>

                        <div className={`flex ${isMobileView ? 'justify-between text-xs' : 'gap-3'}`}>
                            <span>收 <b className="text-white">{hoverData?.close?.toFixed(2) || '-'}</b></span>
                            <span className={Number(priceChange) > 0 ? 'text-red-400' : Number(priceChange) < 0 ? 'text-green-400' : 'text-slate-400'}>
                                {Number(priceChange) > 0 ? '▲' : Number(priceChange) < 0 ? '▼' : ''} {Math.abs(Number(priceChange))} ({priceChangePercent}%)
                            </span>
                            <span>量 <b className="text-yellow-400">{hoverData ? (hoverData.value / 1000).toFixed(0) : '-'}</b>張</span>
                        </div>

                        {isMobileView && (
                            <div className="flex justify-between text-xs text-slate-400 mt-1">
                                <span>開 {hoverData?.open?.toFixed(2)}</span>
                                <span>高 {hoverData?.high?.toFixed(2)}</span>
                                <span>低 {hoverData?.low?.toFixed(2)}</span>
                                {hoverData?.amount > 0 && <span>{(hoverData.amount / 100000000).toFixed(1)}億</span>}
                            </div>
                        )}

                        {!isMobileView && (
                            <>
                                <span>開 <b className="text-white">{hoverData?.open?.toFixed(2) || '-'}</b></span>
                                <span>高 <b className="text-red-400">{hoverData?.high?.toFixed(2) || '-'}</b></span>
                                <span>低 <b className="text-green-400">{hoverData?.low?.toFixed(2) || '-'}</b></span>
                                <span>收 <b className="text-white">{hoverData?.close?.toFixed(2) || '-'}</b><span className={Number(priceChange) > 0 ? 'text-red-400' : Number(priceChange) < 0 ? 'text-green-400' : 'text-slate-400'}>({Number(priceChange) > 0 ? '+' : ''}{priceChange})</span></span>
                                <span>量 <b className="text-yellow-400">{hoverData ? (hoverData.value / 1000).toFixed(0) : '-'}</b><span className={Number(volChange) > 0 ? 'text-red-400' : Number(volChange) < 0 ? 'text-green-400' : 'text-slate-400'}>({Number(volChange) > 0 ? '+' : ''}{hoverData && prevData ? (volChange / 1000).toFixed(0) : '0'})</span></span>
                                {hoverData?.amount > 0 && <span>額 <b className="text-cyan-400">{(hoverData.amount / 100000000).toFixed(2)}億</b></span>}
                            </>
                        )}
                    </div>
                </div>

                {/* K線圖區 */}
                <div className="chart-container">
                    <TechnicalChart code={code} name={stock?.name} onHoverData={handleHoverUpdate} />
                </div>
            </div>
        </div>
    )
}

export default StockDetail
