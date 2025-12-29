import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMobileView } from "@/context/MobileViewContext";

export const Rankings = () => {
    const navigate = useNavigate();
    const { isMobileView } = useMobileView();
    const [sortType, setSortType] = useState('buy');

    const [filterForeign, setFilterForeign] = useState('');
    const [filterTrust, setFilterTrust] = useState('');

    const [sortColumn, setSortColumn] = useState(null);
    const [sortDirection, setSortDirection] = useState('desc');

    const [rankings, setRankings] = useState([]);
    const [loading, setLoading] = useState(false);
    const [page, setPage] = useState(1);
    const [totalPages, setTotalPages] = useState(1);

    // Column Widths - Optimized for "One Page" view
    const colWidths = [
        '12%', // Stock
        '10%', // Price
        '8%',  // Volume
        '15%', // Foreign Streak
        '15%', // Trust Streak
        '10%', // Foreign Shares
        '10%', // Foreign Pct
        '10%', // Trust Shares
        '10%'  // Trust Pct
    ];

    const fetchRankings = async () => {
        setLoading(true);
        try {
            const factor = sortType === 'buy' ? 1 : -1;
            const fStreak = filterForeign ? parseInt(filterForeign) * factor : 0;
            const tStreak = filterTrust ? parseInt(filterTrust) * factor : 0;

            const params = {
                type: 'foreign',
                sort: sortType,
                limit: 30,
                page: page,
                min_foreign_streak: fStreak,
                min_trust_streak: tStreak,
                min_dealer_streak: 0
            };

            if (sortColumn) {
                params.sort_by = sortColumn;
                params.direction = sortDirection;
            }

            const queryParams = new URLSearchParams(params);
            const res = await fetch(`http://localhost:8000/api/rankings/institutional?${queryParams}`);
            const data = await res.json();
            if (data.success) {
                setRankings(data.data);
                setTotalPages(data.total_pages);
            } else {
                setRankings([]);
                setTotalPages(1);
            }
        } catch (error) {
            console.error('Fetch rankings failed:', error);
            setRankings([]);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchRankings();
    }, [sortType, filterForeign, filterTrust, sortColumn, sortDirection, page]);

    const handleSort = (column) => {
        if (sortColumn === column) {
            setSortDirection(prev => prev === 'desc' ? 'asc' : 'desc');
        } else {
            setSortColumn(column);
            setSortDirection('desc');
        }
    };

    const SortIcon = ({ column }) => {
        if (sortColumn !== column) return <span className="text-slate-600 ml-0.5">⇅</span>;
        return sortDirection === 'desc' ? <span className="text-cyan-400 ml-0.5">▼</span> : <span className="text-cyan-400 ml-0.5">▲</span>;
    };

    // Formatters
    const fmtSheets = (num) => {
        if (!num) return '-';
        const val = Math.round(num / 1000);
        const sign = val > 0 ? '+' : '';
        return sign + val.toLocaleString();
    };

    const fmtAmount = (shares, price) => {
        if (!shares || !price) return '-';
        const val = (shares * price) / 100000000; // 億
        const sign = val > 0 ? '+' : '';
        return sign + val.toFixed(1);
    };

    const fmtVolume = (num) => {
        if (!num) return '-';
        const val = Math.round(num / 1000);
        if (val >= 10000) {
            return (val / 10000).toFixed(1) + '萬';
        }
        return val.toLocaleString();
    };

    const fmtCumulative = (num) => {
        if (!num) return '-';
        const sign = num > 0 ? '+' : '';
        const val = Math.round(num / 1000);
        return sign + val.toLocaleString();
    };

    const StreakBadge = ({ value }) => {
        if (!value || value === 0) return <span className="text-slate-500">-</span>;
        const isBuy = value > 0;
        return (
            <span className={`font-bold ${isBuy ? 'text-red-400' : 'text-green-400'}`}>
                {isBuy ? `+${value}` : value}
            </span>
        );
    };

    const getColor = (val) => val > 0 ? 'text-red-400' : val < 0 ? 'text-green-400' : 'text-slate-500';

    return (
        <div className={`bg-slate-900 min-h-screen p-4 text-slate-300 font-sans ${isMobileView ? 'max-w-md mx-auto' : ''}`}>
            {/* Header */}
            <div className="bg-gradient-to-r from-teal-600 to-teal-700 text-white px-4 py-2 mb-4 rounded font-bold text-center text-lg">
                📊 法人買賣超統計
            </div>

            {/* Controls */}
            <div className="flex gap-2 flex-wrap items-center mb-4 bg-slate-800/50 p-2 rounded border border-slate-700">
                <button onClick={() => setSortType('buy')}
                    className={`px-3 py-1 text-sm font-bold rounded ${sortType === 'buy' ? 'bg-red-500/20 text-red-400 border border-red-500/50' : 'bg-slate-800 text-slate-400 border border-slate-600'}`}>
                    買超排行
                </button>
                <button onClick={() => setSortType('sell')}
                    className={`px-3 py-1 text-sm font-bold rounded ${sortType === 'sell' ? 'bg-green-500/20 text-green-400 border border-green-500/50' : 'bg-slate-800 text-slate-400 border border-slate-600'}`}>
                    賣超排行
                </button>

                <span className="text-xs text-slate-400">連買/賣：外資</span>
                <input type="number" value={filterForeign} onChange={(e) => setFilterForeign(e.target.value)}
                    className="bg-slate-800 border border-slate-600 rounded px-1 py-0.5 w-8 text-center text-white text-xs" />
                <span className="text-xs text-slate-400">天</span>

                <span className="text-xs text-slate-400 ml-2">投信</span>
                <input type="number" value={filterTrust} onChange={(e) => setFilterTrust(e.target.value)}
                    className="bg-slate-800 border border-slate-600 rounded px-1 py-0.5 w-8 text-center text-white text-xs" />
                <span className="text-xs text-slate-400">天</span>
            </div>

            {/* Table */}
            <style>{`
                #rankings-table-container,
                #rankings-table-container * {
                    box-sizing: border-box;
                }
                #rankings-table {
                    width: 100% !important;
                    table-layout: fixed !important;
                }
                #rankings-table tbody {
                    display: table-row-group !important;
                }
                #rankings-table tr {
                    display: table-row !important;
                }
                #rankings-table th,
                #rankings-table td {
                    display: table-cell !important;
                    padding: 4px 2px !important;
                    font-size: 11px !important;
                    vertical-align: middle !important;
                }
            `}</style>
            <div id="rankings-table-container" className="bg-slate-800/50 border border-slate-700 rounded overflow-x-auto rankings-fix-container">
                {loading ? (
                    <div className="flex items-center justify-center h-64 text-slate-500">載入中...</div>
                ) : (
                    <table id="rankings-table" className="w-full text-xs border-collapse table-fixed">
                        <colgroup>
                            {colWidths.map((w, i) => <col key={i} style={{ width: w }} />)}
                        </colgroup>
                        <thead style={{ display: 'table-header-group' }}>
                            <tr className="bg-slate-800" style={{ display: 'table-row' }}>
                                <th rowSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-slate-300 sticky left-0 bg-slate-800 z-10 relative" style={{ display: 'table-cell', width: colWidths[0] }}>
                                    股票
                                </th>
                                <th rowSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-slate-300 cursor-pointer hover:text-white relative" onClick={() => handleSort('close')} style={{ display: 'table-cell', width: colWidths[1] }}>
                                    現價%<SortIcon column="close" />
                                </th>
                                <th rowSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-slate-300 cursor-pointer hover:text-white relative" onClick={() => handleSort('volume')} style={{ display: 'table-cell', width: colWidths[2] }}>
                                    成交量<SortIcon column="volume" />
                                </th>
                                <th colSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-slate-300 cursor-pointer hover:text-white" onClick={() => handleSort('streak')} style={{ display: 'table-cell' }}>
                                    連買連賣<SortIcon column="streak" />
                                </th>
                                <th colSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-orange-400 bg-orange-500/10 cursor-pointer hover:text-orange-300 relative" onClick={() => handleSort('foreign_holding')} style={{ display: 'table-cell' }}>
                                    外資持股<SortIcon column="foreign_holding" />
                                </th>
                                <th colSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-yellow-400 bg-yellow-500/10 cursor-pointer hover:text-yellow-300 relative" onClick={() => handleSort('trust_holding')} style={{ display: 'table-cell' }}>
                                    投信累計<SortIcon column="trust_holding" />
                                </th>
                            </tr>
                            <tr className="bg-slate-800/80 text-slate-400" style={{ display: 'table-row' }}>
                                <th className="border border-slate-700 px-1 py-0.5 text-red-400 relative" style={{ display: 'table-cell', width: colWidths[3] }}>
                                    <div className="flex flex-col items-center">
                                        <span className="cursor-pointer hover:text-red-300" onClick={() => handleSort('foreign_streak')}>外資</span>
                                        <div className="flex gap-1 mt-0.5">
                                            <span onClick={() => handleSort('foreign_cumulative')} className="text-[9px] bg-slate-700 px-1 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計張數排序">張</span>
                                            <span onClick={() => handleSort('foreign_cumulative_amount')} className="text-[9px] bg-slate-700 px-1 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計金額排序">金</span>
                                        </div>
                                    </div>
                                </th>
                                <th className="border border-slate-700 px-1 py-0.5 text-yellow-400 relative" style={{ display: 'table-cell', width: colWidths[4] }}>
                                    <div className="flex flex-col items-center">
                                        <span className="cursor-pointer hover:text-yellow-300" onClick={() => handleSort('trust_streak')}>投信</span>
                                        <div className="flex gap-1 mt-0.5">
                                            <span onClick={() => handleSort('trust_cumulative')} className="text-[9px] bg-slate-700 px-1 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計張數排序">張</span>
                                            <span onClick={() => handleSort('trust_cumulative_amount')} className="text-[9px] bg-slate-700 px-1 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計金額排序">金</span>
                                        </div>
                                    </div>
                                </th>
                                <th className="border border-slate-700 px-1 py-0.5 text-orange-400 relative" style={{ display: 'table-cell', width: colWidths[5] }}>張數</th>
                                <th className="border border-slate-700 px-1 py-0.5 text-orange-400 relative" style={{ display: 'table-cell', width: colWidths[6] }}>比率</th>
                                <th className="border border-slate-700 px-1 py-0.5 text-yellow-400 relative" style={{ display: 'table-cell', width: colWidths[7] }}>張數</th>
                                <th className="border border-slate-700 px-1 py-0.5 text-yellow-400 relative" style={{ display: 'table-cell', width: colWidths[8] }}>比率</th>
                            </tr>
                        </thead>
                        <tbody style={{ display: 'table-row-group' }}>
                            {rankings.map((stock) => (
                                <React.Fragment key={stock.code}>
                                    <tr onClick={() => navigate('/', { state: { code: stock.code } })}
                                        className="hover:bg-slate-700/50 cursor-pointer border-t border-slate-700"
                                        style={{ display: 'table-row' }}>
                                        <td rowSpan="2" className="border border-slate-700 px-2 py-1 font-bold text-slate-200 sticky left-0 bg-slate-800 z-10 overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            <div className="flex flex-col">
                                                <span className="text-sm text-white">{stock.name}</span>
                                                <span className="text-xs text-slate-400">{stock.code}</span>
                                            </div>
                                        </td>
                                        <td rowSpan="2" className={`border border-slate-700 px-2 py-1 text-right font-mono font-bold overflow-hidden text-ellipsis whitespace-nowrap ${getColor(stock.change_pct)}`} style={{ display: 'table-cell' }}>
                                            <div className="flex flex-col">
                                                <span>{stock.close}</span>
                                                <span className="text-xs">{stock.change_pct > 0 ? '+' : ''}{stock.change_pct}%</span>
                                            </div>
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-right font-mono text-slate-300 overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            {fmtVolume(stock.volume)}
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-center overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            <div className="flex flex-col items-center justify-center gap-0.5">
                                                <StreakBadge value={stock.foreign_streak} />
                                                <div className={`text-[10px] ${getColor(stock.foreign_cumulative)}`}>
                                                    {fmtCumulative(stock.foreign_cumulative)}
                                                    <span className="text-slate-500 ml-0.5">({stock.foreign_cumulative_pct}%)</span>
                                                </div>
                                                <div className={`text-[10px] ${getColor(stock.foreign_cumulative)}`}>
                                                    {fmtAmount(stock.foreign_cumulative, stock.close)}億
                                                </div>
                                            </div>
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-center overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            <div className="flex flex-col items-center justify-center gap-0.5">
                                                <StreakBadge value={stock.trust_streak} />
                                                <div className={`text-[10px] ${getColor(stock.trust_cumulative)}`}>
                                                    {fmtCumulative(stock.trust_cumulative)}
                                                    <span className="text-slate-500 ml-0.5">({stock.trust_cumulative_pct}%)</span>
                                                </div>
                                                <div className={`text-[10px] ${getColor(stock.trust_cumulative)}`}>
                                                    {fmtAmount(stock.trust_cumulative, stock.close)}億
                                                </div>
                                            </div>
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-right font-mono text-orange-300 overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            {fmtCumulative(stock.foreign_holding_shares)}
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-right font-mono text-orange-300 overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            {stock.foreign_holding_pct}%
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-right font-mono text-yellow-300 overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            {fmtCumulative(stock.trust_holding_shares)}
                                        </td>
                                        <td rowSpan="2" className="border border-slate-700 px-1 py-1 text-right font-mono text-yellow-300 overflow-hidden text-ellipsis whitespace-nowrap" style={{ display: 'table-cell' }}>
                                            {stock.trust_holding_pct}%
                                        </td>
                                    </tr>
                                </React.Fragment>
                            ))}

                            {rankings.length === 0 && !loading && (
                                <tr>
                                    <td colSpan="9" className="text-center py-8 text-slate-500" style={{ display: 'table-cell' }}>
                                        無符合條件的資料
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                )}
            </div>

            {/* Pagination Controls */}
            {!loading && rankings.length > 0 && (
                <div className="flex justify-center items-center gap-2 mt-4 text-sm">
                    <button
                        onClick={() => setPage(p => Math.max(1, p - 1))}
                        disabled={page === 1}
                        className={`px-3 py-1 rounded border ${page === 1 ? 'bg-slate-800 text-slate-600 border-slate-700 cursor-not-allowed' : 'bg-slate-800 text-slate-300 border-slate-600 hover:bg-slate-700'}`}
                    >
                        上一頁
                    </button>

                    <div className="flex gap-1">
                        {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                            let p = page;
                            if (totalPages <= 5) {
                                p = i + 1;
                            } else if (page <= 3) {
                                p = i + 1;
                            } else if (page >= totalPages - 2) {
                                p = totalPages - 4 + i;
                            } else {
                                p = page - 2 + i;
                            }

                            return (
                                <button
                                    key={p}
                                    onClick={() => setPage(p)}
                                    className={`w-8 h-8 flex items-center justify-center rounded border ${page === p ? 'bg-teal-600 text-white border-teal-500 font-bold' : 'bg-slate-800 text-slate-300 border-slate-600 hover:bg-slate-700'}`}
                                >
                                    {p}
                                </button>
                            );
                        })}
                    </div>

                    <button
                        onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                        disabled={page === totalPages}
                        className={`px-3 py-1 rounded border ${page === totalPages ? 'bg-slate-800 text-slate-600 border-slate-700 cursor-not-allowed' : 'bg-slate-800 text-slate-300 border-slate-600 hover:bg-slate-700'}`}
                    >
                        下一頁
                    </button>
                    <span className="text-slate-500 ml-2">共 {totalPages} 頁</span>
                </div>
            )}
        </div>
    );
};
