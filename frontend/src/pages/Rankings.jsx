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
            // Use Supabase directly - try institutional_investors first
            const limit = 20;
            const offset = (page - 1) * limit;

            console.log('Fetching rankings from Supabase...');
            const { data: debugData, error: debugError } = await supabase.from('institutional_investors').select('count');
            console.log('Debug institutional count:', debugData, debugError);

            // Get latest date from institutional_investors
            const { data: latestData, error: latestError } = await supabase
                .from('institutional_investors')
                .select('date_int')
                .order('date_int', { ascending: false })
                .limit(1);

            if (latestError || !latestData?.length) {
                console.error('No institutional data found:', latestError);

                // Fallback to stock_snapshot
                const { data: snapData, error: snapError } = await supabase
                    .from('stock_snapshot')
                    .select('code, name, close, volume, foreign_buy, trust_buy, dealer_buy')
                    .order('foreign_buy', { ascending: sortType === 'sell' })
                    .range(offset, offset + limit - 1);

                if (snapError) {
                    console.error('stock_snapshot error:', snapError);
                    setRankings([]);
                    return;
                }

                console.log('Got snapshot data:', snapData?.length);
                const transformed = (snapData || []).map(d => ({
                    ...d, foreign_streak: 0, trust_streak: 0,
                    foreign_cumulative: d.foreign_buy || 0,
                    trust_cumulative: d.trust_buy || 0,
                    foreign_holding_shares: 0, trust_holding_shares: 0, trust_holding_pct: 0
                }));
                setRankings(transformed);
                setTotalPages(10);
                return;
            }

            const latestDate = latestData[0].date_int;
            console.log('Latest date:', latestDate);

            // Get institutional data for latest date
            const { data, error } = await supabase
                .from('institutional_investors')
                .select('code, foreign_net, trust_net, dealer_net')
                .eq('date_int', latestDate)
                .order('foreign_net', { ascending: sortType === 'sell' })
                .range(offset, offset + limit - 1);

            if (error) {
                console.error('Supabase error:', error);
                setRankings([]);
                return;
            }

            console.log('Got institutional data:', data?.length);
            setDataDate(`${String(latestDate).slice(0, 4)}-${String(latestDate).slice(4, 6)}-${String(latestDate).slice(6, 8)}`);

            // Get stock codes for snapshot lookup
            const codes = (data || []).map(d => d.code);

            // Fetch stock_snapshot for price and volume data
            let snapshotMap = {};
            if (codes.length > 0) {
                const { data: snapData, error: snapError } = await supabase
                    .from('stock_snapshot')
                    .select('code, name, close, volume, foreign_streak, trust_streak')
                    .in('code', codes);

                if (!snapError && snapData) {
                    snapData.forEach(s => {
                        snapshotMap[s.code] = s;
                    });
                }
            }

            const transformed = (data || []).map(d => {
                const snap = snapshotMap[d.code] || {};
                return {
                    code: d.code,
                    name: snap.name || d.code,
                    close: snap.close || 0,
                    change_pct: 0, // Will be calculated if we have previous close
                    volume: snap.volume || 0,
                    foreign_streak: snap.foreign_streak || 0,
                    trust_streak: snap.trust_streak || 0,
                    foreign_cumulative: d.foreign_net || 0,
                    trust_cumulative: d.trust_net || 0,
                    foreign_holding_shares: 0,
                    foreign_holding_pct: 0,
                    trust_holding_shares: 0,
                    trust_holding_pct: 0
                };
            });

            setRankings(transformed);
            setTotalPages(10);
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
