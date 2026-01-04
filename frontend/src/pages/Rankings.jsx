import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMobileView } from "@/context/MobileViewContext";
import { supabase } from '@/lib/supabaseClient';

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
    const [dataDate, setDataDate] = useState('');

    const fetchRankings = async () => {
        setLoading(true);
        try {
            const limit = 20;
            const offset = (page - 1) * limit;

            // Build query on stock_snapshot
            let query = supabase
                .from('stock_snapshot')
                .select('code, name, close, volume, foreign_buy, trust_buy, dealer_buy, foreign_streak, trust_streak', { count: 'exact' });

            // Apply Filters
            if (filterForeign && !isNaN(parseInt(filterForeign))) {
                query = query.gte('foreign_streak', parseInt(filterForeign));
            }
            if (filterTrust && !isNaN(parseInt(filterTrust))) {
                query = query.gte('trust_streak', parseInt(filterTrust));
            }

            // Apply Sorting
            if (sortColumn) {
                query = query.order(sortColumn, { ascending: sortDirection === 'asc' });
            } else {
                // Default sort: Foreign Buy (Net) Descending
                query = query.order(sortType === 'sell' ? 'foreign_buy' : 'foreign_buy', { ascending: sortType === 'sell' });
            }

            // Pagination
            query = query.range(offset, offset + limit - 1);

            const { data, error, count } = await query;

            if (error) {
                console.error('Fetch rankings error:', error);
                setRankings([]);
                return;
            }

            // Transform data
            const transformed = (data || []).map(d => ({
                code: d.code,
                name: d.name,
                close: d.close,
                change_pct: 0, // Snapshot doesn't have change_pct, usually calculated or joined
                volume: d.volume,
                foreign_streak: d.foreign_streak || 0,
                trust_streak: d.trust_streak || 0,
                foreign_cumulative: d.foreign_buy || 0, // Using daily net as cumulative for now
                trust_cumulative: d.trust_buy || 0,
                foreign_holding_shares: 0,
                foreign_holding_pct: 0,
                trust_holding_shares: 0,
                trust_holding_pct: 0
            }));

            setRankings(transformed);
            setTotalPages(Math.ceil((count || 0) / limit));

            // Update date from system status or just use today
            const today = new Date();
            setDataDate(`${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`);

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
        if (value === null || value === undefined) return <span className="text-slate-500">-</span>;
        if (value === 0) return <span className="text-slate-500">0</span>;
        const isBuy = value > 0;
        return (
            <span className={`font-bold ${isBuy ? 'text-red-400' : 'text-green-400'}`}>
                {isBuy ? `+${value}` : value}
            </span>
        );
    };

    const getColor = (val) => val > 0 ? 'text-red-400' : val < 0 ? 'text-green-400' : 'text-slate-500';

    return (
        <div className="h-screen w-screen overflow-hidden flex flex-col pb-10 bg-slate-900 text-slate-300">
            {/* Header - Compact */}
            <div className="shrink-0 px-3 py-2 border-b border-slate-800 flex justify-between items-center bg-slate-900 z-10">
                <h1 className="text-lg font-bold text-white flex items-center gap-2">
                    <span className="text-teal-500">📊</span> 法人買賣超
                </h1>
                {dataDate && <span className="text-xs text-slate-500">{dataDate}</span>}
            </div>

            {/* Controls - Compact */}
            <div className="shrink-0 p-2 bg-slate-800/50 border-b border-slate-700">
                <div className="flex items-center gap-2 overflow-x-auto no-scrollbar">
                    {/* Buy/Sell Toggle */}
                    <div className="flex bg-slate-900 rounded p-0.5 shrink-0">
                        <button onClick={() => setSortType('buy')} className={`px-3 py-1 text-xs font-bold rounded transition-colors ${sortType === 'buy' ? 'bg-red-500/20 text-red-400 border border-red-500/50' : 'text-slate-400 hover:text-slate-200'}`}>買超</button>
                        <button onClick={() => setSortType('sell')} className={`px-3 py-1 text-xs font-bold rounded transition-colors ${sortType === 'sell' ? 'bg-green-500/20 text-green-400 border border-green-500/50' : 'text-slate-400 hover:text-slate-200'}`}>賣超</button>
                    </div>

                    {/* Filter Inputs */}
                    <div className="flex items-center gap-2 bg-slate-900/30 px-2 py-1 rounded shrink-0">
                        <div className="flex items-center gap-1">
                            <span className="text-[10px] text-slate-500">外資連</span>
                            <input type="number" value={filterForeign} onChange={(e) => setFilterForeign(e.target.value)} className="bg-slate-800 border border-slate-600 rounded px-1 py-0.5 w-8 text-center text-white text-[10px] focus:outline-none focus:border-teal-500" placeholder="0" />
                        </div>
                        <div className="flex items-center gap-1">
                            <span className="text-[10px] text-slate-500">投信連</span>
                            <input type="number" value={filterTrust} onChange={(e) => setFilterTrust(e.target.value)} className="bg-slate-800 border border-slate-600 rounded px-1 py-0.5 w-8 text-center text-white text-[10px] focus:outline-none focus:border-teal-500" placeholder="0" />
                        </div>
                    </div>
                </div>
            </div>

            {/* Table Container - Scrollable */}
            <div className="flex-1 overflow-auto p-2">
                {loading ? (
                    <div className="flex items-center justify-center h-40 text-slate-500 gap-2">
                        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-teal-500"></div>
                        <div className="text-xs">載入中...</div>
                    </div>
                ) : (
                    <div className="bg-slate-800 rounded border border-slate-700 overflow-hidden">
                        <div className="overflow-x-auto">
                            <table className="w-full text-xs border-collapse whitespace-nowrap text-left">
                                <thead>
                                    <tr className="bg-slate-900 text-slate-400 border-b border-slate-700">
                                        <th className="p-2 font-bold text-slate-300 sticky left-0 z-20 bg-slate-900 shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)] min-w-[80px]">股票</th>
                                        <th className="p-2 font-bold text-right cursor-pointer hover:text-white min-w-[70px]" onClick={() => handleSort('close')}>現價%<SortIcon column="close" /></th>
                                        <th className="p-2 font-bold text-right cursor-pointer hover:text-white min-w-[70px]" onClick={() => handleSort('volume')}>成交量<SortIcon column="volume" /></th>
                                        <th className="p-2 font-bold text-center border-l border-slate-700 min-w-[60px]">外資連</th>
                                        <th className="p-2 font-bold text-center min-w-[60px]">投信連</th>
                                        <th className="p-2 font-bold text-right text-orange-400 border-l border-slate-700 min-w-[80px]">外資買賣</th>
                                        <th className="p-2 font-bold text-right text-yellow-400 border-l border-slate-700 min-w-[80px]">投信買賣</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {rankings.map((stock) => (
                                        <tr key={stock.code} onClick={() => navigate(`/dashboard?code=${stock.code}`)} className="group hover:bg-slate-700/50 cursor-pointer border-b border-slate-700/50 transition-colors">
                                            <td className="p-2 font-bold text-slate-200 sticky left-0 z-10 bg-slate-900 group-hover:bg-slate-800 transition-colors shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)]">
                                                <div className="flex flex-col">
                                                    <span className="text-sm text-white">{stock.name.substring(0, 4)}</span>
                                                    <span className="text-[10px] text-slate-500 font-mono">{stock.code}</span>
                                                </div>
                                            </td>
                                            <td className={`p-2 text-right font-mono font-bold ${getColor(stock.change_pct)}`}>
                                                <div className="flex flex-col items-end">
                                                    <span>{stock.close}</span>
                                                    <span className="text-[10px]">{stock.change_pct > 0 ? '+' : ''}{stock.change_pct}%</span>
                                                </div>
                                            </td>
                                            <td className="p-2 text-right font-mono text-slate-300">{fmtVolume(stock.volume)}</td>
                                            <td className="p-2 text-center border-l border-slate-700/50"><StreakBadge value={stock.foreign_streak} /></td>
                                            <td className="p-2 text-center"><StreakBadge value={stock.trust_streak} /></td>
                                            <td className={`p-2 text-right font-mono border-l border-slate-700/50 ${getColor(stock.foreign_cumulative)}`}>{fmtCumulative(stock.foreign_cumulative)}</td>
                                            <td className={`p-2 text-right font-mono border-l border-slate-700/50 ${getColor(stock.trust_cumulative)}`}>{fmtCumulative(stock.trust_cumulative)}</td>
                                        </tr>
                                    ))}
                                    {rankings.length === 0 && !loading && (
                                        <tr><td colSpan="7" className="text-center py-8 text-slate-500">無符合條件的資料</td></tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                )}
            </div>

            {/* Pagination - Compact */}
            {!loading && rankings.length > 0 && (
                <div className="shrink-0 p-2 border-t border-slate-800 bg-slate-900 flex justify-between items-center">
                    <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="px-3 py-1 bg-slate-800 rounded text-xs disabled:opacity-30">上一頁</button>
                    <span className="text-xs text-slate-400">{page} / {totalPages}</span>
                    <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} className="px-3 py-1 bg-slate-800 rounded text-xs disabled:opacity-30">下一頁</button>
                </div>
            )}
        </div>
    );
};
