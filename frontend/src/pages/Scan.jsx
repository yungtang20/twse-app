import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMobileView } from "@/context/MobileViewContext";

export const Scan = () => {
    const navigate = useNavigate();
    const { isMobileView } = useMobileView();
    const [activeFilter, setActiveFilter] = useState('vp');
    const [scanResults, setScanResults] = useState([]);
    const [processLog, setProcessLog] = useState([]);
    const [loading, setLoading] = useState(false);
    const [minVol, setMinVol] = useState(500);
    const [minPrice, setMinPrice] = useState(50); // Default 50 (Lower Limit)

    // Pagination
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 24;

    const fetchScanResults = async () => {
        setLoading(true);
        setProcessLog([]); // Reset log
        try {
            let url = '';
            const params = `limit=100&min_vol=${minVol}${minPrice ? `&min_price=${minPrice}` : ''}`;
            switch (activeFilter) {
                case 'vp': url = `http://localhost:8000/api/scan/vp?direction=support&${params}`; break;
                case 'mfi': url = `http://localhost:8000/api/scan/mfi?condition=oversold&${params}`; break;
                case 'ma': url = `http://localhost:8000/api/scan/ma?pattern=bull&${params}`; break;
                case 'kd_month': url = `http://localhost:8000/api/scan/kd-cross?signal=golden&timeframe=month&${params}`; break;
                case 'vsbc': url = `http://localhost:8000/api/scan/vsbc?style=steady&${params}`; break;
                case 'smart_money': url = `http://localhost:8000/api/scan/smart-money?${params}`; break;
                case '2560': url = `http://localhost:8000/api/scan/2560?${params}`; break;
                case 'five_stage': url = `http://localhost:8000/api/scan/five-stage?${params}`; break;
                case 'institutional': url = `http://localhost:8000/api/scan/institutional-value?${params}`; break;
                case 'six_dim': url = `http://localhost:8000/api/scan/six-dim?${params}`; break;
                case 'patterns': url = `http://localhost:8000/api/scan/patterns?type=morning_star&${params}`; break;
                case 'pv_div': url = `http://localhost:8000/api/scan/pv-divergence?${params}`; break;
                case 'builtin': url = `http://localhost:8000/api/scan/builtin?${params}`; break;
                default:
                    setScanResults([]);
                    setLoading(false);
                    return;
            }

            const res = await fetch(url);
            const data = await res.json();

            if (data.success && data.data && data.data.results) {
                setScanResults(data.data.results);
                if (data.data.process_log) {
                    setProcessLog(data.data.process_log);
                }
                setCurrentPage(1);
            } else {
                setScanResults([]);
            }
        } catch (error) {
            console.error('Scan failed:', error);
            setScanResults([]);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchScanResults();
    }, [activeFilter, minVol, minPrice]);

    // Pagination Logic
    const indexOfLastItem = currentPage * itemsPerPage;
    const indexOfFirstItem = indexOfLastItem - itemsPerPage;
    const currentItems = scanResults.slice(indexOfFirstItem, indexOfLastItem);
    const totalPages = Math.ceil(scanResults.length / itemsPerPage);

    const handlePageChange = (pageNumber) => {
        setCurrentPage(pageNumber);
    };

    const filters = [
        { id: 'vp', name: '[1] VP箱型', desc: "尋找股價接近 VP (Volume Profile) 籌碼密集區的個股，作為支撐或壓力測試。" },
        { id: 'mfi', name: '[2] MFI資金', desc: "偵測 MFI 資金流向指標，尋找超賣 (<20) 或超買 (>80) 的反轉機會。" },
        { id: 'ma', name: '[3] 均線掃描', desc: "掃描均線排列型態，包括多頭排列 (MA5>20>60) 或回測均線支撐。" },
        { id: 'kd_month', name: '[4] 月KD交叉', desc: "篩選月層級 KD 指標黃金交叉 (K>D) 或死亡交叉的波段訊號。" },
        { id: 'vsbc', name: '[5] VSBC籌碼', desc: "基於 VSBC (Volume Spread Analysis) 策略，尋找量價異常或籌碼穩定的標的。" },
        { id: 'smart_money', name: '[6] 聰明錢', desc: "追蹤聰明錢流向，尋找縮量上漲或主力吸籌的跡象。" },
        { id: '2560', name: '[7] 2560戰法', desc: "執行 2560 戰法篩選，尋找 MA25 與 MA60 黃金交叉的波段起漲點。" },
        { id: 'five_stage', name: '[8] 五階篩選', desc: "綜合五階篩選模型：均線、動能、籌碼、趨勢、型態。" },
        { id: 'institutional', name: '[9] 機構價值', desc: "評估機構投資價值，尋找低估值或高成長潛力的個股。" },
        { id: 'six_dim', name: '[a] 六維共振', desc: "六維共振分析：結合量、價、均線、指標、籌碼、型態的多重確認。" },
        { id: 'patterns', name: '[b] K線型態', desc: "辨識 K 線型態，如晨星、吞噬、鎚頭等反轉訊號。" },
        { id: 'pv_div', name: '[c] 量價背離', desc: "偵測量價背離訊號，尋找價格創新高但成交量背離的轉折點。" },
    ];

    const activeDesc = filters.find(f => f.id === activeFilter)?.desc || "選擇一個策略開始掃描";

    // Helper Functions
    const fmtNum = (num) => num ? Math.round(num).toLocaleString() : '-';
    const fmtSheets = (num) => num ? Math.round(num / 1000).toLocaleString() : '0'; // Convert shares to sheets
    const fmtPrice = (num) => num ? num.toFixed(2) : '-';
    const getTrend = (curr, prev) => {
        if (!curr || !prev) return '';
        return curr > prev ? '↑' : curr < prev ? '↓' : '';
    };
    const getTrendColor = (curr, prev) => {
        if (!curr || !prev) return 'text-slate-400';
        return curr > prev ? 'text-red-400' : curr < prev ? 'text-green-400' : 'text-slate-400';
    };

    return (
        <div className={`bg-slate-900 min-h-screen p-4 text-slate-300 font-sans ${isMobileView ? 'max-w-md mx-auto border-x border-slate-700' : ''}`}>
            {/* Header */}
            <div className="flex justify-between items-center mb-4">
                <div>
                    <h1 className="text-xl font-bold text-white flex items-center gap-2">
                        <span className="text-blue-500">⚡</span> 市場掃描
                    </h1>
                </div>

            </div>

            {/* Filter & Control Bar */}
            <div className="flex flex-col md:flex-row gap-2 mb-2">
                <div className="flex-1">
                    <div className="relative">
                        <select
                            value={activeFilter}
                            onChange={(e) => setActiveFilter(e.target.value)}
                            className="w-full bg-slate-800 text-white border border-slate-700 rounded px-3 py-2 text-sm appearance-none focus:outline-none focus:border-blue-500 transition-colors cursor-pointer"
                        >
                            {filters.map(f => (
                                <option key={f.id} value={f.id}>{f.name}</option>
                            ))}
                        </select>
                        <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400 text-xs">▼</div>
                    </div>
                </div>
                <div className="w-full md:w-auto">
                    <div className="flex items-center gap-2 bg-slate-800 px-3 py-2 rounded border border-slate-700">
                        <span className="text-sm text-slate-500 whitespace-nowrap">量&gt;</span>
                        <input
                            type="number"
                            value={minVol}
                            onChange={(e) => setMinVol(Number(e.target.value))}
                            className="bg-transparent text-white w-full md:w-16 text-sm focus:outline-none text-right"
                            placeholder="500"
                        />
                        <span className="text-sm text-slate-500 whitespace-nowrap">張</span>
                    </div>
                </div>
                <div className="w-full md:w-auto">
                    <div className="flex items-center gap-2 bg-slate-800 px-3 py-2 rounded border border-slate-700">
                        <span className="text-sm text-slate-500 whitespace-nowrap">價&gt;</span>
                        <input
                            type="number"
                            value={minPrice}
                            onChange={(e) => setMinPrice(Number(e.target.value))}
                            className="bg-transparent text-white w-full md:w-12 text-sm focus:outline-none text-right"
                            placeholder="50"
                        />
                        <span className="text-sm text-slate-500 whitespace-nowrap">元</span>
                    </div>
                </div>
            </div>

            {/* Screening Process Description Panel */}
            <div className="bg-slate-800/80 border border-blue-500/30 rounded p-3 mb-4 text-sm text-slate-300 shadow-sm">
                <div className="flex flex-col gap-2">
                    <div className="flex items-center gap-2 border-b border-slate-700/50 pb-2">
                        <span className="text-blue-400 font-bold whitespace-nowrap">ℹ️ 篩選邏輯:</span>
                        <span className="text-white font-medium">{activeDesc}</span>
                    </div>

                    {processLog && processLog.length > 0 ? (
                        <div className="font-mono text-xs bg-slate-900/50 p-2 rounded border border-slate-700/50">
                            <div className="flex justify-between text-slate-400 mb-1 border-b border-slate-700/50 pb-1">
                                <span>篩選步驟</span>
                                <span>剩餘檔數</span>
                            </div>
                            {processLog.map((step, idx) => (
                                <div key={idx} className="flex justify-between items-center py-0.5 hover:bg-slate-800/50">
                                    <div className="flex items-center gap-2">
                                        <span className="text-green-500">✓</span>
                                        <span className="text-slate-300">{step.step}</span>
                                        <span className="text-slate-500 text-[10px]">({step.desc})</span>
                                    </div>
                                    <span className="text-white font-bold">{step.count} 檔</span>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div className="flex flex-wrap gap-2 text-xs text-slate-400">
                            <span className="bg-slate-900/50 px-1.5 py-0.5 rounded border border-slate-700">成交量 &gt; {minVol} 張</span>
                            <span className="bg-slate-900/50 px-1.5 py-0.5 rounded border border-slate-700">股價 &gt; {minPrice} 元</span>
                            {loading && <span className="text-yellow-500 animate-pulse">掃描運算中...</span>}
                            {!loading && scanResults.length > 0 && <span className="text-green-400">符合條件: {scanResults.length} 檔</span>}
                            {!loading && scanResults.length === 0 && <span className="text-red-400">無符合條件</span>}
                        </div>
                    )}
                </div>
            </div>

            {/* Results Display */}
            <div className="bg-slate-800/50 rounded-lg p-2 border border-slate-700/50 min-h-[400px]">
                {loading ? (
                    <div className="flex items-center justify-center h-full text-slate-500">
                        <div className="animate-pulse">掃描中...</div>
                    </div>
                ) : (
                    <>
                        {isMobileView ? (
                            /* Mobile List View */
                            <div className="flex flex-col gap-1">
                                {currentItems.map((stock) => (
                                    <div key={stock.code} onClick={() => navigate('/', { state: { code: stock.code } })} className="bg-slate-800 border border-slate-700 p-2 rounded flex items-center justify-between text-sm font-mono cursor-pointer hover:bg-slate-700">
                                        <div className="flex flex-col w-16 shrink-0">
                                            <span className="text-blue-400 font-bold">{stock.code}</span>
                                            <span className="text-white truncate">{stock.name}</span>
                                        </div>
                                        <div className="flex flex-col w-16 shrink-0 text-right">
                                            <span className={stock.change_pct >= 0 ? 'text-red-400' : 'text-green-400'}>{stock.close?.toFixed(2)}</span>
                                            <span className={stock.change_pct >= 0 ? 'text-red-400' : 'text-green-400'}>{stock.change_pct}%</span>
                                        </div>
                                        <div className="flex flex-col flex-1 text-right ml-2 overflow-hidden">
                                            <span className="text-slate-400">{(stock.volume / 1000).toFixed(0)}張</span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            /* Desktop Grid View - 6 Columns with Larger Font (text-sm) */
                            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 2xl:grid-cols-6 gap-2">
                                {currentItems.map((stock) => {
                                    const volRatio = (stock.volume && stock.vol_ma60) ? (stock.volume / stock.vol_ma60).toFixed(1) : '-';
                                    return (
                                        <div
                                            key={stock.code}
                                            onClick={() => navigate('/', { state: { code: stock.code } })}
                                            className="bg-slate-800 border border-slate-700 rounded p-2 hover:bg-slate-700/50 hover:border-blue-500/50 transition-all cursor-pointer group shadow-sm flex flex-col gap-1 h-full overflow-hidden"
                                        >
                                            {/* Header: Name/Code & Price */}
                                            <div className="flex justify-between items-start border-b border-slate-700/50 pb-1">
                                                <div className="flex items-baseline gap-1 min-w-0">
                                                    <span className="text-white font-bold text-lg group-hover:text-blue-400 transition-colors truncate tracking-tight" title={stock.name}>{stock.name}</span>
                                                    <span className="text-slate-500 text-xs font-mono">{stock.code}</span>
                                                </div>
                                                <div className="flex flex-col items-end shrink-0 ml-1">
                                                    <span className={`text-xl font-bold font-mono ${stock.close ? (stock.change_pct >= 0 ? 'text-red-400' : 'text-green-400') : 'text-slate-400'}`}>
                                                        {stock.close?.toFixed(2) || '-'}
                                                    </span>
                                                    <span className={`text-xs ${stock.change_pct >= 0 ? 'text-red-400' : 'text-green-400'}`}>
                                                        {stock.change_pct > 0 ? '+' : ''}{stock.change_pct}%
                                                    </span>
                                                </div>
                                            </div>

                                            {/* Volume Info */}
                                            <div className="text-sm text-slate-400 font-mono leading-none tracking-tight">
                                                <div className="flex justify-between items-center">
                                                    <span>量:<span className="text-slate-300">{fmtSheets(stock.volume)}</span>張 <span className="text-slate-500">({volRatio}x)</span></span>
                                                </div>
                                                <div className="flex justify-between items-center mt-0.5">
                                                    <span className="text-xs text-slate-500">均:{fmtSheets(stock.vol_ma5)}/{fmtSheets(stock.vol_ma60)}</span>
                                                </div>
                                            </div>

                                            {/* Price Indicators */}
                                            <div className="text-sm text-slate-400 font-mono leading-none border-t border-slate-700/30 pt-1 grid grid-cols-2 gap-x-1 tracking-tight">
                                                <span>VWAP:{fmtPrice(stock.vwap20)}<span className={getTrendColor(stock.vwap20, stock.vwap20_prev)}>{getTrend(stock.vwap20, stock.vwap20_prev)}</span></span>
                                                <span className="text-right">POC:{fmtPrice(stock.vp_poc)}</span>
                                                <span>MFI:{stock.mfi14 ? stock.mfi14.toFixed(0) : '-'}<span className={getTrendColor(stock.mfi14, stock.mfi14_prev)}>{getTrend(stock.mfi14, stock.mfi14_prev)}</span></span>
                                                <span className="text-right">RSI:{stock.rsi ? stock.rsi.toFixed(0) : '-'}</span>
                                            </div>

                                            {/* Ranges */}
                                            <div className="text-sm text-slate-400 font-mono leading-none border-t border-slate-700/30 pt-1 flex justify-between tracking-tight">
                                                <div>
                                                    <span className="text-slate-500 mr-0.5">VSBC</span>
                                                    <span>{stock.vsbc_upper ? Math.round(stock.vsbc_upper) : '-'}/{stock.vsbc_lower ? Math.round(stock.vsbc_lower) : '-'}</span>
                                                </div>
                                                <div>
                                                    <span className="text-slate-500 mr-0.5">VP</span>
                                                    <span>{stock.vp_upper ? Math.round(stock.vp_upper) : '-'}/{stock.vp_lower ? Math.round(stock.vp_lower) : '-'}</span>
                                                </div>
                                            </div>

                                            {/* MAs */}
                                            <div className="text-sm text-slate-400 font-mono leading-none border-t border-slate-700/30 pt-1 grid grid-cols-2 gap-x-1 tracking-tight">
                                                <span>MA20:{fmtPrice(stock.ma20)}<span className={getTrendColor(stock.ma20, stock.ma20_prev)}>{getTrend(stock.ma20, stock.ma20_prev)}</span></span>
                                                <span className="text-right">MA60:{fmtPrice(stock.ma60)}<span className={getTrendColor(stock.ma60, stock.ma60_prev)}>{getTrend(stock.ma60, stock.ma60_prev)}</span></span>
                                                <span>MA120:{fmtPrice(stock.ma120)}<span className={getTrendColor(stock.ma120, stock.ma120_prev)}>{getTrend(stock.ma120, stock.ma120_prev)}</span></span>
                                                <span className="text-right">MA200:{fmtPrice(stock.ma200)}<span className={getTrendColor(stock.ma200, stock.ma200_prev)}>{getTrend(stock.ma200, stock.ma200_prev)}</span></span>
                                            </div>

                                            {/* Signal */}
                                            <div className="text-sm text-yellow-500 font-mono leading-none border-t border-slate-700/30 pt-1 truncate tracking-tight">
                                                {stock.vsa_signal || '-'}
                                            </div>

                                            {/* Chips */}
                                            <div className="text-sm text-slate-400 font-mono leading-none border-t border-slate-700/30 pt-1">
                                                <div className="flex justify-between gap-0.5 text-xs">
                                                    <span className={stock.foreign_buy > 0 ? 'text-red-400' : stock.foreign_buy < 0 ? 'text-green-400' : ''}>外:{fmtSheets(stock.foreign_buy)}</span>
                                                    <span className={stock.trust_buy > 0 ? 'text-red-400' : stock.trust_buy < 0 ? 'text-green-400' : ''}>投:{fmtSheets(stock.trust_buy)}</span>
                                                    <span className={stock.dealer_buy > 0 ? 'text-red-400' : stock.dealer_buy < 0 ? 'text-green-400' : ''}>自:{fmtSheets(stock.dealer_buy)}</span>
                                                </div>
                                                <div className="flex justify-between mt-0.5">
                                                    <span>大戶:{stock.major_holders_pct ? stock.major_holders_pct.toFixed(1) : '-'}%</span>
                                                    <span>集保:{fmtNum(stock.total_shareholders)}</span>
                                                </div>
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                    </>
                )}
                {/* Pagination */}
                {scanResults.length > 0 && (
                    <div className="mt-4 pt-2 border-t border-slate-700/50 flex flex-col sm:flex-row justify-between items-center gap-4 text-sm text-slate-500">
                        <span>顯示 {indexOfFirstItem + 1}-{Math.min(indexOfLastItem, scanResults.length)} 筆，共 {scanResults.length} 筆</span>
                        <div className="flex gap-1">
                            <button onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 1} className="px-2 py-1 bg-slate-700 rounded hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">上一頁</button>
                            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                                let p = currentPage - 2 + i;
                                if (currentPage < 3) p = i + 1;
                                if (p > 0 && p <= totalPages) {
                                    return <button key={p} onClick={() => handlePageChange(p)} className={`px-2 py-1 rounded transition-colors ${currentPage === p ? 'bg-blue-600 text-white' : 'bg-slate-700 hover:bg-slate-600'}`}>{p}</button>;
                                }
                                return null;
                            })}
                            <button onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage === totalPages} className="px-2 py-1 bg-slate-700 rounded hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">下一頁</button>
                        </div>
                    </div >
                )}
            </div >
        </div >
    );
};
