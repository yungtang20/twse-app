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
            // Use Supabase directly - try institutional_investors first
            const limit = 20;
            const offset = (page - 1) * limit;

            console.log('Fetching rankings from Supabase...');

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
                    .select('code, name, close, change_pct, volume, foreign_buy, trust_buy, dealer_buy')
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
                .select('code, name, foreign_buy, foreign_sell, trust_buy, trust_sell, dealer_buy, dealer_sell')
                .eq('date_int', latestDate)
                .order(sortType === 'buy' ? 'foreign_buy' : 'foreign_sell', { ascending: sortType === 'sell' })
                .range(offset, offset + limit - 1);

            if (error) {
                console.error('Supabase error:', error);
                setRankings([]);
                return;
            }

            console.log('Got institutional data:', data?.length);
            setDataDate(`${String(latestDate).slice(0, 4)}-${String(latestDate).slice(4, 6)}-${String(latestDate).slice(6, 8)}`);

            const transformed = (data || []).map(d => ({
                code: d.code,
                name: d.name || d.code,
                close: 0,
                change_pct: 0,
                volume: 0,
                foreign_streak: 0,
                trust_streak: 0,
                foreign_cumulative: (d.foreign_buy || 0) - (d.foreign_sell || 0),
                trust_cumulative: (d.trust_buy || 0) - (d.trust_sell || 0),
                foreign_holding_shares: 0,
                foreign_holding_pct: 0,
                trust_holding_shares: 0,
                trust_holding_pct: 0
            }));

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

    // Note: data_date is set from fetchRankings() response (data.data_date)

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
            <div className="bg-gradient-to-r from-teal-600 to-teal-700 text-white px-4 py-2 mb-4 rounded font-bold text-lg flex justify-between items-center">
                <span>📊 法人買賣超統計</span>
                {dataDate && <span className="text-sm font-normal opacity-80">資料日期：{dataDate}</span>}
            </div>

            {/* Controls */}
            <div className="flex flex-col gap-2 mb-4 bg-slate-800/50 p-2 rounded border border-slate-700">
                <div className="flex gap-2">
                    <button onClick={() => setSortType('buy')}
                        className={`flex-1 px-3 py-1 text-sm font-bold rounded ${sortType === 'buy' ? 'bg-red-500/20 text-red-400 border border-red-500/50' : 'bg-slate-800 text-slate-400 border border-slate-600'}`}>
                        買超排行
                    </button>
                    <button onClick={() => setSortType('sell')}
                        className={`flex-1 px-3 py-1 text-sm font-bold rounded ${sortType === 'sell' ? 'bg-green-500/20 text-green-400 border border-green-500/50' : 'bg-slate-800 text-slate-400 border border-slate-600'}`}>
                        賣超排行
                    </button>
                </div>

                <div className="flex items-center gap-2 bg-slate-900/30 p-1.5 rounded">
                    <span className="text-xs text-slate-400 whitespace-nowrap">連買/賣：</span>
                    <div className="flex items-center gap-1">
                        <span className="text-xs text-slate-500">外資</span>
                        <input type="number" value={filterForeign} onChange={(e) => setFilterForeign(e.target.value)}
                            className="bg-slate-800 border border-slate-600 rounded px-1 py-0.5 w-8 text-center text-white text-xs" />
                        <span className="text-xs text-slate-500">天</span>
                    </div>
                    <div className="flex items-center gap-1 ml-2">
                        <span className="text-xs text-slate-500">投信</span>
                        <input type="number" value={filterTrust} onChange={(e) => setFilterTrust(e.target.value)}
                            className="bg-slate-800 border border-slate-600 rounded px-1 py-0.5 w-8 text-center text-white text-xs" />
                        <span className="text-xs text-slate-500">天</span>
                    </div>
                </div>
            </div>

            {/* Table Container - Horizontal Scroll */}
            <div className="bg-slate-800/50 border border-slate-700 rounded overflow-x-auto relative min-h-[400px] no-scrollbar">
                {loading ? (
                    <div className="flex items-center justify-center h-64 text-slate-500">載入中...</div>
                ) : (
                    <table className="w-full text-xs border-collapse whitespace-nowrap text-left">
                        <thead>
                            <tr className="bg-slate-800 text-slate-400 border-b border-slate-700">
                                <th rowSpan="2" className="p-2 font-bold text-slate-300 sticky left-0 z-20 bg-slate-800 shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)] min-w-[80px]">
                                    股票
                                </th>
                                <th rowSpan="2" className="p-2 font-bold text-slate-300 cursor-pointer hover:text-white text-right min-w-[70px]" onClick={() => handleSort('close')}>
                                    現價%<SortIcon column="close" />
                                </th>
                                <th rowSpan="2" className="p-2 font-bold text-slate-300 cursor-pointer hover:text-white text-right min-w-[70px]" onClick={() => handleSort('volume')}>
                                    成交量<SortIcon column="volume" />
                                </th>
                                <th colSpan="2" className="p-2 font-bold text-slate-300 cursor-pointer hover:text-white text-center border-l border-slate-700" onClick={() => handleSort('streak')}>
                                    連買連賣<SortIcon column="streak" />
                                </th>
                                <th colSpan="2" className="p-2 font-bold text-orange-400 bg-orange-500/10 cursor-pointer hover:text-orange-300 text-center border-l border-slate-700" onClick={() => handleSort('foreign_holding')}>
                                    外資持股<SortIcon column="foreign_holding" />
                                </th>
                                <th colSpan="2" className="p-2 font-bold text-yellow-400 bg-yellow-500/10 cursor-pointer hover:text-yellow-300 text-center border-l border-slate-700" onClick={() => handleSort('trust_holding')}>
                                    投信持股(估)<SortIcon column="trust_holding" />
                                </th>
                            </tr>
                            <tr className="bg-slate-800/80 text-slate-400 border-b border-slate-700">
                                <th className="p-2 text-red-400 text-center border-l border-slate-700 min-w-[80px]">
                                    <div className="flex flex-col items-center">
                                        <span className="cursor-pointer hover:text-red-300" onClick={() => handleSort('foreign_streak')}>外資</span>
                                        <div className="flex gap-0.5 mt-1 w-full justify-center">
                                            <span onClick={() => handleSort('foreign_cumulative')} className="px-1.5 py-0.5 text-[10px] bg-slate-700 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計張數排序">張</span>
                                            <span onClick={() => handleSort('foreign_cumulative_amount')} className="px-1.5 py-0.5 text-[10px] bg-slate-700 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計金額排序">金</span>
                                        </div>
                                    </div>
                                </th>
                                <th className="p-2 text-yellow-400 text-center min-w-[80px]">
                                    <div className="flex flex-col items-center">
                                        <span className="cursor-pointer hover:text-yellow-300" onClick={() => handleSort('trust_streak')}>投信</span>
                                        <div className="flex gap-0.5 mt-1 w-full justify-center">
                                            <span onClick={() => handleSort('trust_cumulative')} className="px-1.5 py-0.5 text-[10px] bg-slate-700 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計張數排序">張</span>
                                            <span onClick={() => handleSort('trust_cumulative_amount')} className="px-1.5 py-0.5 text-[10px] bg-slate-700 rounded cursor-pointer hover:bg-slate-600 text-slate-300" title="依累計金額排序">金</span>
                                        </div>
                                    </div>
                                </th>
                                <th className="p-2 text-orange-400 text-right border-l border-slate-700 min-w-[70px]">張數</th>
                                <th className="p-2 text-orange-400 text-right min-w-[60px]">比率</th>
                                <th className="p-2 text-yellow-400 text-right border-l border-slate-700 min-w-[70px]">張數</th>
                                <th className="p-2 text-yellow-400 text-right min-w-[60px]">比率</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rankings.map((stock) => (
                                <tr key={stock.code} onClick={() => navigate('/', { state: { code: stock.code } })}
                                    className="group hover:bg-slate-700/50 cursor-pointer border-b border-slate-700/50 transition-colors">
                                    <td className="p-2 font-bold text-slate-200 sticky left-0 z-10 bg-slate-900 group-hover:bg-slate-800 transition-colors shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)]">
                                        <div className="flex flex-col">
                                            <span className="text-sm text-white">{stock.name.substring(0, 4)}</span>
                                            <span className="text-xs text-slate-400">{stock.code}</span>
                                        </div>
                                    </td>
                                    <td className={`p-2 text-right font-mono font-bold ${getColor(stock.change_pct)}`}>
                                        <div className="flex flex-col items-end">
                                            <span>{stock.close}</span>
                                            <span className="text-xs">{stock.change_pct > 0 ? '+' : ''}{stock.change_pct}%</span>
                                        </div>
                                    </td>
                                    <td className="p-2 text-right font-mono text-slate-300">
                                        {fmtVolume(stock.volume)}
                                    </td>
                                    <td className="p-2 text-center border-l border-slate-700/50">
                                        <div className="flex flex-col items-center justify-center gap-0.5">
                                            <StreakBadge value={stock.foreign_streak} />
                                            <div className={`text-[10px] ${getColor(stock.foreign_cumulative)}`}>
                                                {fmtCumulative(stock.foreign_cumulative)}
                                            </div>
                                            <div className={`text-[10px] ${getColor(stock.foreign_cumulative)}`}>
                                                {fmtAmount(stock.foreign_cumulative, stock.close)}億
                                            </div>
                                        </div>
                                    </td>
                                    <td className="p-2 text-center">
                                        <div className="flex flex-col items-center justify-center gap-0.5">
                                            <StreakBadge value={stock.trust_streak} />
                                            <div className={`text-[10px] ${getColor(stock.trust_cumulative)}`}>
                                                {fmtCumulative(stock.trust_cumulative)}
                                            </div>
                                            <div className={`text-[10px] ${getColor(stock.trust_cumulative)}`}>
                                                {fmtAmount(stock.trust_cumulative, stock.close)}億
                                            </div>
                                        </div>
                                    </td>
                                    <td className="p-2 text-right font-mono text-orange-300 border-l border-slate-700/50">
                                        {fmtCumulative(stock.foreign_holding_shares)}
                                    </td>
                                    <td className="p-2 text-right font-mono text-orange-300">
                                        {stock.foreign_holding_pct}%
                                    </td>
                                    <td className="p-2 text-right font-mono text-yellow-300 border-l border-slate-700/50">
                                        {fmtCumulative(stock.trust_holding_shares || 0)}
                                    </td>
                                    <td className="p-2 text-right font-mono text-yellow-300">
                                        {stock.trust_holding_pct || 0}%
                                    </td>
                                </tr>
                            ))}

                            {rankings.length === 0 && !loading && (
                                <tr>
                                    <td colSpan="9" className="text-center py-8 text-slate-500">
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
