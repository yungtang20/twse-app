}, [activeFilter, minVol, minPrice, vpDirection, tolerance, maPattern]);

const handleSort = (key) => {
    let direction = 'desc';
    if (sortConfig.key === key && sortConfig.direction === 'desc') {
        direction = 'asc';
    } else if (sortConfig.key === key && sortConfig.direction === 'asc') {
        direction = null; // Reset
        key = null;
    }
    setSortConfig({ key, direction });
};

const sortedResults = useMemo(() => {
    let sortableItems = [...scanResults];
    if (sortConfig.key !== null) {
        sortableItems.sort((a, b) => {
            let aValue, bValue;

            // Helper to get value based on key
            switch (sortConfig.key) {
                case 'name': aValue = a.code; bValue = b.code; break;
                case 'close': aValue = a.close; bValue = b.close; break;
                case 'change_pct': aValue = a.change_pct; bValue = b.change_pct; break;
                case 'volume': aValue = a.volume; bValue = b.volume; break;
                case 'vol_ratio':
                    aValue = (a.volume && a.vol_ma60) ? (a.volume / a.vol_ma60) : -1;
                    bValue = (b.volume && b.vol_ma60) ? (b.volume / b.vol_ma60) : -1;
                    break;
                case 'vp_high': aValue = a.vp_high || 0; bValue = b.vp_high || 0; break;
                case 'vp_low': aValue = a.vp_low || 0; bValue = b.vp_low || 0; break;
                case 'vp_poc': aValue = a.vp_poc || 0; bValue = b.vp_poc || 0; break;
                case 'vwap': aValue = a.vwap || 0; bValue = b.vwap || 0; break;
                case 'mfi': aValue = a.mfi || 0; bValue = b.mfi || 0; break;
                case 'distance':
                    const getDist = (item) => {
                        if (activeFilter !== 'vp') return -999;
                        if (vpDirection === 'support' && item.vp_low) {
                            return (item.close - item.vp_low) / item.vp_low;
                        } else if (vpDirection === 'resistance' && item.vp_high) {
                            return (item.vp_high - item.close) / item.vp_high;
                        }
                        return -999;
                    };
                    aValue = getDist(a);
                    bValue = getDist(b);
                    break;
                default: aValue = a[sortConfig.key]; bValue = b[sortConfig.key];
            }

            if (aValue < bValue) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aValue > bValue) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
    }
    return sortableItems;
}, [scanResults, sortConfig, activeFilter, vpDirection]);

// Pagination Logic
const indexOfLastItem = currentPage * itemsPerPage;
const indexOfFirstItem = indexOfLastItem - itemsPerPage;
const currentItems = sortedResults.slice(indexOfFirstItem, indexOfLastItem);
const totalPages = Math.ceil(sortedResults.length / itemsPerPage);
const fmtPrice = (num) => num ? num.toFixed(2) : '-';

const toggleColumn = (key) => {
    setVisibleColumns(prev => ({ ...prev, [key]: !prev[key] }));
};

// Filter definitions - aligned with Python menu
const filters = [
    { id: 'vp', name: '[1] VP掃描', desc: "尋找股價接近 VP 下緣支撐或上緣壓力的個股 (容忍度 15%)。" },
    { id: 'mfi', name: '[2] MFI掃描', desc: "偵測 MFI 資金流向，由小→大 (流入開始, <30) 或由大→小 (流出結束, >70)。" },
    { id: 'ma', name: '[3] 均線掃描', desc: "子選項: [1]低於MA200 [2]低於MA20 [3]均線多頭 (四線上揚+乖離0-10%)。" },
    { id: 'kd_month', name: '[4] 月KD交叉', desc: "月KD金叉 (K_prev<=D_prev && K>D && K<80) 且 NVI>PVI 籌碼多頭。" },
    { id: 'vsbc', name: '[5] VSBC籌碼', desc: "VSBC 多方行為 (股價站上POC, MA20>MA60, 股價>MA20)。" },
    { id: 'smart_money', name: '[6] 聰明錢', desc: "量增>1.1x + 價>MA200 + MFI<80 + 籌碼評分>=4。" },
    { id: '2560', name: '[7] 2560戰法', desc: "股價>25MA向上 + 均量金叉 + 陽線收漲 + 乖離<10%。" },
    { id: 'five_stage', name: '[8] 五階篩選', desc: "RS強度 + 均線多頭 + NVI籌碼 + RSI強勢 + 收紅五階篩選。" },
    { id: 'institutional', name: '[9] 機構價值', desc: "趨勢向上 (MA20>MA60) + 未過熱 (RSI<60) + 高流動性。" },
    { id: 'six_dim', name: '[a] 六維共振', desc: "MACD/KDJ/RSI/LWR/BBI/MTM 至少5項符合。" },
    { id: 'patterns', name: '[b] K線型態', desc: "掃描晨星 (底部反轉) 或黃昏之星 (頂部反轉) 型態。" },
    { id: 'pv_div', name: '[c] 量價背離', desc: "3日量價背離訊號 (價漲量縮 / 價跌量增)。" }
];

const activeDesc = filters.find(f => f.id === activeFilter)?.desc || "選擇一個策略開始掃描";

const fmtSheets = (num) => num ? Math.round(num / 1000).toLocaleString() : '0';

const handlePageChange = (page) => {
    setCurrentPage(page);
};

// Table-Driven Method for Controls Rendering (Rule #1)
const renderControls = () => {
    const controlsMap = {
        vp: (
            <div className="flex flex-wrap items-center gap-2 bg-slate-800 rounded border border-slate-700 px-2 py-1 min-h-[36px]">
                <div className="flex items-center gap-1">
                    <span className="text-[10px] text-slate-400 whitespace-nowrap">容忍度</span>
                    <select
                        value={tolerance}
                        onChange={(e) => setTolerance(parseFloat(e.target.value))}
                        className="bg-slate-900 text-white text-[10px] border border-slate-600 rounded px-1 py-0.5 focus:outline-none cursor-pointer"
                    >
                        <option value={0.02}>2%</option>
                        <option value={0.05}>5%</option>
                        <option value={0.10}>10%</option>
                        <option value={0.15}>15%</option>
                    </select>
                </div>
                <div className="w-px h-3 bg-slate-700 mx-1 hidden sm:block"></div>
                <div className="flex bg-slate-900 rounded p-0.5">
                    <button onClick={() => setVpDirection('support')} className={`px-2 py-0.5 text-[10px] rounded transition-colors ${vpDirection === 'support' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>支撐</button>
                    <button onClick={() => setVpDirection('resistance')} className={`px-2 py-0.5 text-[10px] rounded transition-colors ${vpDirection === 'resistance' ? 'bg-red-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>壓力</button>
                </div>
            </div>
        ),
        ma: (
            <div className="flex bg-slate-900 rounded border border-slate-700 p-0.5 h-9 items-center">
                <button onClick={() => setMaPattern('below_ma200')} className={`px-2 py-1 text-[10px] rounded transition-colors ${maPattern === 'below_ma200' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>低於年線</button>
                <button onClick={() => setMaPattern('below_ma20')} className={`px-2 py-1 text-[10px] rounded transition-colors ${maPattern === 'below_ma20' ? 'bg-purple-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>低於月線</button>
                <button onClick={() => setMaPattern('bull')} className={`px-2 py-1 text-[10px] rounded transition-colors ${maPattern === 'bull' ? 'bg-red-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>均線多頭</button>
            </div>
        ),
        patterns: (
            <div className="flex bg-slate-900 rounded border border-slate-700 p-0.5 h-9 items-center">
                <button onClick={() => setPatternType('morning_star')} className={`px-2 py-1 text-[10px] rounded transition-colors ${patternType === 'morning_star' ? 'bg-red-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>晨星 (多)</button>
                <button onClick={() => setPatternType('evening_star')} className={`px-2 py-1 text-[10px] rounded transition-colors ${patternType === 'evening_star' ? 'bg-green-600 text-white' : 'text-slate-400 hover:text-slate-200'}`}>夜星 (空)</button>
            </div>
        )
    };

    return controlsMap[activeFilter] || null;
};

return (
    <div className="h-screen w-screen overflow-hidden flex flex-col pb-10 bg-slate-900 text-slate-300">
        {/* Header - Compact */}
        <div className="shrink-0 px-3 py-2 border-b border-slate-800 flex justify-between items-center bg-slate-900 z-10">
            <h1 className="text-2xl font-bold text-white flex items-center gap-2">
                <span className="text-yellow-400">⚡</span>
                市場掃描 v1.2 (Strict)
            </h1>
            {dataDate && <span className="text-xs text-slate-500">{dataDate}</span>}
        </div>

        {/* Controls - Compact */}
        <div className="shrink-0 p-2 bg-slate-800/50 border-b border-slate-700">
            <div className="flex flex-col gap-2">
                <div className="flex gap-2 flex-wrap items-center">
                    <div className="relative flex-1 min-w-[150px] h-9">
                        <select
                            value={activeFilter}
                            onChange={(e) => setActiveFilter(e.target.value)}
                            className="w-full h-full bg-slate-800 text-white border border-slate-700 rounded px-2 text-xs appearance-none focus:outline-none focus:border-blue-500"
                        >
                            {filters.map(f => <option key={f.id} value={f.id}>{f.name}</option>)}
                        </select>
                        <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400 text-[10px]">▼</div>
                    </div>

                    {/* Contextual Controls */}
                    {renderControls()}
                </div>

                <div className="text-[10px] text-slate-400 truncate px-1">
                    {activeDesc}
                </div>
            </div>
        </div>

        {/* Results List - Table View */}
        <div className="flex-1 overflow-auto p-2">
            {
                loading ? (
                    <div className="flex flex-col items-center justify-center h-40 text-slate-500 gap-2" >
                        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
                        <div className="text-xs">掃描運算中...</div>
                        <div className="text-[10px] opacity-70 max-w-[200px] truncate text-center">
                            {processLog[processLog.length - 1]}
                        </div>
                    </div>
                ) : scanResults.length > 0 ? (
                    <div className="bg-slate-800 rounded border border-slate-700 overflow-hidden">
                        <div className="overflow-x-auto">
                            <table className="w-full text-xs border-collapse whitespace-nowrap text-left">
                                <thead>
                                    <tr className="bg-slate-900 text-slate-400 border-b border-slate-700">
                                        <th className="p-2 font-bold text-slate-300 sticky left-0 z-20 bg-slate-900 shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)] min-w-[80px]">股票</th>
                                        <th className="p-2 font-bold text-right cursor-pointer hover:text-white min-w-[70px]" onClick={() => handleSort('close')}>現價<span className="ml-1 text-[10px]">{sortConfig.key === 'close' ? (sortConfig.direction === 'desc' ? '▼' : '▲') : '⇅'}</span></th>
                                        <th className="p-2 font-bold text-right cursor-pointer hover:text-white min-w-[70px]" onClick={() => handleSort('volume')}>成交量<span className="ml-1 text-[10px]">{sortConfig.key === 'volume' ? (sortConfig.direction === 'desc' ? '▼' : '▲') : '⇅'}</span></th>

                                        {/* Dynamic Columns */}
                                        {activeFilter === 'vp' && (
                                            <>
                                                <th className="p-2 font-bold text-right min-w-[60px]">VP{vpDirection === 'support' ? '支撐' : '壓力'}</th>
                                                <th className="p-2 font-bold text-right cursor-pointer hover:text-white min-w-[60px]" onClick={() => handleSort('distance')}>距離%<span className="ml-1 text-[10px]">{sortConfig.key === 'distance' ? (sortConfig.direction === 'desc' ? '▼' : '▲') : '⇅'}</span></th>
                                            </>
                                        )}
                                        {activeFilter === 'mfi' && <th className="p-2 font-bold text-right cursor-pointer hover:text-white min-w-[60px]" onClick={() => handleSort('mfi')}>MFI<span className="ml-1 text-[10px]">{sortConfig.key === 'mfi' ? (sortConfig.direction === 'desc' ? '▼' : '▲') : '⇅'}</span></th>}
                                        {activeFilter === 'ma' && <th className="p-2 font-bold text-right min-w-[60px]">乖離率</th>}
                                    </tr>
                                </thead>
                                <tbody>
                                    {currentItems.map((stock) => {
                                        // Calculate dynamic values for display
                                        let displayValue = null;
                                        let displayLabel = '';
                                        let displayClass = '';

                                        if (activeFilter === 'vp') {
                                            const target = vpDirection === 'support' ? stock.vp_low : stock.vp_high;
                                            const dist = vpDirection === 'support'
                                                ? (stock.close - target) / target
                                                : (target - stock.close) / target;
                                            displayLabel = target?.toFixed(2);
                                            displayValue = (dist * 100).toFixed(1) + '%';
                                            displayClass = 'text-yellow-400';
                                        } else if (activeFilter === 'mfi') {
                                            displayValue = stock.mfi?.toFixed(1);
                                            displayClass = stock.mfi < 30 ? 'text-green-400' : 'text-red-400';
                                        }

                                        return (
                                            <tr key={stock.code} onClick={() => navigate(`/dashboard?code=${stock.code}`)} className="border-b border-slate-700/50 hover:bg-slate-700/50 cursor-pointer transition-colors">
                                                <td className="p-2 sticky left-0 z-10 bg-slate-800 shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)]">
                                                    <div className="font-bold text-blue-400">{stock.code}</div>
                                                    <div className="text-[10px] text-slate-400">{stock.name}</div>
                                                </td>
                                                <td className={`p-2 text-right font-mono ${stock.change_pct > 0 ? 'text-red-400' : stock.change_pct < 0 ? 'text-green-400' : 'text-slate-300'}`}>
                                                    {stock.close}
                                                </td>
                                                <td className="p-2 text-right font-mono text-slate-300">
                                                    {fmtSheets(stock.volume)}
                                                </td>

                                                {/* Dynamic Cells */}
                                                {activeFilter === 'vp' && (
                                                    <>
                                                        <td className="p-2 text-right font-mono text-slate-400">{displayLabel}</td>
                                                        <td className={`p-2 text-right font-mono ${displayClass}`}>{displayValue}</td>
                                                    </>
                                                )}
                                                {activeFilter === 'mfi' && (
                                                    <td className={`p-2 text-right font-mono ${displayClass}`}>{displayValue}</td>
                                                )}
                                                {activeFilter === 'ma' && (
                                                    <td className="p-2 text-right font-mono text-slate-400">-</td>
                                                )}
                                                {/* Default empty cells for other filters to maintain structure if needed, or just omit */}
                                                {!['vp', 'mfi', 'ma'].includes(activeFilter) && (
                                                    <td className="p-2 text-right text-slate-500 text-[10px]">符合</td>
                                                )}
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    </div>
                ) : (
                    <div className="flex flex-col items-center justify-center h-40 text-slate-500 gap-2">
                        <div className="text-2xl">🔍</div>
                        <div className="text-xs">無符合條件個股</div>
                    </div>
                )}
        </div >

        {/* Pagination - Compact */}
        {
            scanResults.length > 0 && (
                <div className="shrink-0 p-2 border-t border-slate-800 bg-slate-900 flex justify-between items-center">
                    <button onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 1} className="px-3 py-1 bg-slate-800 rounded text-xs disabled:opacity-30">上一頁</button>
                    <span className="text-xs text-slate-400">{currentPage} / {totalPages}</span>
                    <button onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage === totalPages} className="px-3 py-1 bg-slate-800 rounded text-xs disabled:opacity-30">下一頁</button>
                </div>
            )
        }
    </div >
);
};
