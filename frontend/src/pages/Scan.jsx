import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMobileView } from "@/context/MobileViewContext";
import { supabase } from '@/lib/supabaseClient';
import { calculateVP, calculateMFI, calculateMA, calculateRSI, calculateVWAP, calculateKD, calculateMACD, calculateVSBC, calculateNVI, calculatePVI, calculateBollinger } from '@/utils/indicators';

export const Scan = () => {
    const navigate = useNavigate();
    const { isMobileView } = useMobileView();
    const [activeFilter, setActiveFilter] = useState('vp');
    const [scanResults, setScanResults] = useState([]);
    const [processLog, setProcessLog] = useState([]);
    const [loading, setLoading] = useState(false);
    const [minVol, setMinVol] = useState(500);
    const [minPrice, setMinPrice] = useState(50);
    const [dataDate, setDataDate] = useState(null);
    const [vpDirection, setVpDirection] = useState('support'); // 'support' or 'resistance'
    const [maPattern, setMaPattern] = useState('below_ma200'); // 'below_ma200', 'below_ma20', 'bull'
    const [patternType, setPatternType] = useState('morning_star'); // 'morning_star' or 'evening_star'
    const [tolerance, setTolerance] = useState(0.02); // 2% default
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'desc' });

    // Column Visibility State
    const [visibleColumns, setVisibleColumns] = useState({
        vp_high: true,
        vp_low: true,
        vp_poc: true,
        vwap: true,
        mfi: true,
        ma20: false, ma25: false, ma60: false, ma120: false, ma200: false,
        vol_ma5: false, vol_ma60: false, rsi: false,
        foreign: false, trust: false, dealer: false, big_trader: false, concentration: false
    });
    const [showColumnSelector, setShowColumnSelector] = useState(false);

    // Pagination
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 24;

    const fetchSystemStatus = async () => {
        // Skip in production mode - use current date
        if (!import.meta.env.DEV) {
            const today = new Date();
            setDataDate(`${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`);
            return;
        }
        try {
            const res = await fetch('/api/admin/status');
            const data = await res.json();
            if (data.success && data.data) {
                const dateStr = data.data.latest_date?.toString();
                if (dateStr && dateStr.length === 8) {
                    setDataDate(`${dateStr.substring(0, 4)}-${dateStr.substring(4, 6)}-${dateStr.substring(6, 8)}`);
                }
            }
        } catch (error) {
            console.error('Failed to fetch system status:', error);
        }
    };

    const fetchScanResults = async () => {
        setLoading(true);
        setProcessLog([]);

        try {
            // Step 1: Get stock list from stock_snapshot with basic filters
            const { data: stocks, error: stockError } = await supabase
                .from('stock_snapshot')
                .select('code, name, close, change_pct, volume, foreign_buy, trust_buy, dealer_buy')
                .gte('volume', minVol * 1000)
                .gte('close', minPrice)
                .order('volume', { ascending: false })
                .limit(100);

            if (stockError) {
                console.error('Failed to fetch stocks:', stockError);
                setScanResults([]);
                setLoading(false);
                return;
            }

            setProcessLog([`讀取 ${stocks.length} 檔股票...`]);

            // Step 2: For each stock, fetch history and calculate indicators
            const results = [];
            const batchSize = 10;

            for (let i = 0; i < Math.min(stocks.length, 50); i += batchSize) {
                const batch = stocks.slice(i, i + batchSize);
                setProcessLog(prev => [...prev, `計算指標 ${i + 1}-${Math.min(i + batchSize, stocks.length)}...`]);

                const batchResults = await Promise.all(batch.map(async (stock) => {
                    try {
                        // Fetch history for this stock
                        const { data: history, error: histError } = await supabase
                            .from('stock_history')
                            .select('date_int, open, high, low, close, volume')
                            .eq('code', stock.code)
                            .order('date_int', { ascending: true })
                            .limit(60);

                        if (histError || !history || history.length < 20) return null;

                        // Convert history format
                        const chartData = history.map(h => ({
                            time: `${String(h.date_int).slice(0, 4)}-${String(h.date_int).slice(4, 6)}-${String(h.date_int).slice(6, 8)}`,
                            open: h.open, high: h.high, low: h.low, close: h.close, value: h.volume
                        }));

                        // Calculate indicators
                        const last = chartData.length - 1;
                        const vp = calculateVP(chartData, 20);
                        const mfi = calculateMFI(chartData, 14);
                        const ma20 = calculateMA(chartData, 20);
                        const ma60 = calculateMA(chartData, 60);
                        const ma200 = calculateMA(chartData, 200);
                        const vwap = calculateVWAP(chartData);

                        const result = {
                            ...stock,
                            vp_high: vp.upper[last] || 0,
                            vp_low: vp.lower[last] || 0,
                            vp_poc: vp.poc[last] || 0,
                            mfi: mfi[last] || 0,
                            ma20: ma20[last] || 0,
                            ma60: ma60[last] || 0,
                            ma200: ma200[last] || 0,
                            vwap: vwap[last] || 0,
                            vol_ma60: calculateMA(chartData, 60, 'value')[last] || 0
                        };

                        // Apply filter based on activeFilter
                        if (activeFilter === 'vp') {
                            const close = stock.close;
                            if (vpDirection === 'support') {
                                const distFromLow = result.vp_low > 0 ? (close - result.vp_low) / result.vp_low : 1;
                                if (distFromLow >= 0 && distFromLow <= tolerance) return result;
                            } else {
                                const distFromHigh = result.vp_high > 0 ? (result.vp_high - close) / result.vp_high : 1;
                                if (distFromHigh >= 0 && distFromHigh <= tolerance) return result;
                            }
                        } else if (activeFilter === 'mfi') {
                            if (result.mfi < 30) return result;
                        } else if (activeFilter === 'ma') {
                            if (maPattern === 'below_ma200' && stock.close < result.ma200 && result.ma200 > 0) return result;
                            if (maPattern === 'below_ma20' && stock.close < result.ma20 && result.ma20 > 0) return result;
                            if (maPattern === 'bull' && result.ma20 > result.ma60) return result;
                        } else if (activeFilter === 'kd_month') {
                            // KD golden cross: K crosses above D, K < 80
                            const kd = calculateKD(chartData, 9);
                            if (last >= 1 && kd.k[last] && kd.d[last]) {
                                const kCross = kd.k[last - 1] <= kd.d[last - 1] && kd.k[last] > kd.d[last];
                                if (kCross && kd.k[last] < 80) return result;
                            }
                        } else if (activeFilter === 'vsbc') {
                            // Price > POC, MA20 > MA60, Price > MA20
                            if (stock.close > result.vp_poc && result.ma20 > result.ma60 && stock.close > result.ma20) return result;
                        } else if (activeFilter === 'smart_money') {
                            // Volume > 1.1x avg, Price > MA200, MFI < 80
                            const volRatio = result.vol_ma60 > 0 ? stock.volume / result.vol_ma60 : 0;
                            if (volRatio > 1.1 && stock.close > result.ma200 && result.mfi < 80) return result;
                        } else if (activeFilter === '2560') {
                            // Price > MA25, Vol cross, Bullish, Bias < 10%
                            const ma25 = calculateMA(chartData, 25);
                            const volMa5 = calculateMA(chartData, 5, 'value');
                            const volMa20 = calculateMA(chartData, 20, 'value');
                            const bias = result.ma20 > 0 ? ((stock.close - result.ma20) / result.ma20 * 100) : 999;
                            const isBullish = chartData[last].close > chartData[last].open;
                            const volCross = last > 0 && volMa5[last] > volMa20[last] && volMa5[last - 1] <= volMa20[last - 1];
                            if (stock.close > (ma25[last] || 0) && (volCross || volMa5[last] > volMa20[last]) && isBullish && bias >= 0 && bias < 10) return result;
                        } else if (activeFilter === 'five_stage') {
                            // 5-stage: MA bull + NVI > PVI + RSI > 50 + Bullish + MFI > 50
                            const nvi = calculateNVI(chartData);
                            const pvi = calculatePVI(chartData);
                            const rsi = calculateRSI(chartData, 14);
                            const isBullish = chartData[last].close > chartData[last].open;
                            let score = 0;
                            if (result.ma20 > result.ma60) score++;
                            if (nvi[last] > pvi[last]) score++;
                            if (rsi[last] > 50) score++;
                            if (isBullish) score++;
                            if (result.mfi > 50) score++;
                            if (score >= 4) return result;
                        } else if (activeFilter === 'institutional') {
                            // MA20 > MA60, RSI < 60, High volume
                            const rsi = calculateRSI(chartData, 14);
                            if (result.ma20 > result.ma60 && rsi[last] < 60 && stock.volume > minVol * 2000) return result;
                        } else if (activeFilter === 'six_dim') {
                            // 6 indicators: MACD/KD/RSI/Bollinger/NVI/MFI >= 5 bullish
                            const macd = calculateMACD(chartData);
                            const kd = calculateKD(chartData, 9);
                            const rsi = calculateRSI(chartData, 14);
                            const boll = calculateBollinger(chartData, 20);
                            const nvi = calculateNVI(chartData);
                            const pvi = calculatePVI(chartData);
                            let cnt = 0;
                            if (macd.osc[last] > 0) cnt++;
                            if (kd.k[last] > kd.d[last]) cnt++;
                            if (rsi[last] > 50) cnt++;
                            if (boll.mid[last] && stock.close > boll.mid[last]) cnt++;
                            if (nvi[last] > pvi[last]) cnt++;
                            if (result.mfi > 50) cnt++;
                            if (cnt >= 5) return result;
                        } else if (activeFilter === 'patterns') {
                            // Morning Star / Evening Star
                            if (chartData.length >= 3) {
                                const c0 = chartData[last - 2], c1 = chartData[last - 1], c2 = chartData[last];
                                if (patternType === 'morning_star') {
                                    const bear0 = c0.close < c0.open && (c0.open - c0.close) > (c0.high - c0.low) * 0.5;
                                    const small1 = Math.abs(c1.close - c1.open) < (c1.high - c1.low) * 0.3;
                                    const bull2 = c2.close > c2.open && (c2.close - c2.open) > (c2.high - c2.low) * 0.5;
                                    if (bear0 && small1 && bull2 && c2.close > (c0.open + c0.close) / 2) return result;
                                } else {
                                    const bull0 = c0.close > c0.open && (c0.close - c0.open) > (c0.high - c0.low) * 0.5;
                                    const small1 = Math.abs(c1.close - c1.open) < (c1.high - c1.low) * 0.3;
                                    const bear2 = c2.close < c2.open && (c2.open - c2.close) > (c2.high - c2.low) * 0.5;
                                    if (bull0 && small1 && bear2 && c2.close < (c0.open + c0.close) / 2) return result;
                                }
                            }
                        } else if (activeFilter === 'pv_div') {
                            // Price-Volume divergence: Price up, Volume down (3 days)
                            if (chartData.length >= 3) {
                                let priceUp = 0, volDown = 0;
                                for (let k = 1; k <= 3 && last - k >= 0; k++) {
                                    if (chartData[last - k + 1].close > chartData[last - k].close) priceUp++;
                                    if (chartData[last - k + 1].value < chartData[last - k].value) volDown++;
                                }
                                if (priceUp >= 2 && volDown >= 2) return result;
                            }
                        } else {
                            return result;
                        }
                        return null;
                    } catch (e) {
                        console.error(`Error processing ${stock.code}:`, e);
                        return null;
                    }
                }));

                results.push(...batchResults.filter(r => r !== null));
            }

            setProcessLog(prev => [...prev, `掃描完成，符合 ${results.length} 檔`]);
            setScanResults(results);
            setCurrentPage(1);
        } catch (error) {
            console.error('Scan failed:', error);
            setScanResults([]);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchSystemStatus();
        fetchScanResults();
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
        { id: 'vp', name: '[1] VP掃描', desc: "尋找股價接近 VP 下緣支撐或上緣壓力的個股 (容忍度 2%)。" },
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

    return (
        <div className={`bg-slate-900 min-h-screen p-4 text-slate-300 font-sans ${isMobileView ? 'max-w-md mx-auto border-x border-slate-700' : ''}`}>
            {/* Header */}
            <div className="flex justify-between items-center mb-4">
                <div>
                    <h1 className="text-xl font-bold text-white flex items-center gap-2">
                        <span className="text-blue-500">⚡</span> 市場掃描
                    </h1>
                </div>
                {dataDate && <span className="text-sm text-slate-400">資料日期：{dataDate}</span>}
            </div>

            {/* Filter & Control Bar */}
            <div className="grid grid-cols-2 gap-2 mb-2">
                {/* Strategy Select */}
                <div className="w-full">
                    <div className="relative h-full">
                        <select
                            value={activeFilter}
                            onChange={(e) => setActiveFilter(e.target.value)}
                            className="w-full h-full bg-slate-800 text-white border border-slate-700 rounded px-3 py-2 text-sm appearance-none focus:outline-none focus:border-blue-500 transition-colors cursor-pointer flex items-center"
                        >
                            {filters.map(f => (
                                <option key={f.id} value={f.id}>{f.name}</option>
                            ))}
                        </select>
                        <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400 text-xs">▼</div>
                    </div>
                </div>

                {/* VP Controls */}
                {activeFilter === 'vp' && (
                    <div className="flex flex-col justify-center gap-1 bg-slate-800 rounded border border-slate-700 px-2 py-1 w-full h-full">
                        <div className="flex justify-between items-center w-full">
                            <span className="text-[10px] text-slate-400 font-bold whitespace-nowrap">容忍度: {(tolerance * 100).toFixed(0)}%</span>
                            <div className="flex bg-slate-900 rounded p-0.5">
                                <button
                                    onClick={() => setVpDirection('support')}
                                    className={`px-2 py-0.5 text-[10px] font-bold rounded transition-colors ${vpDirection === 'support' ? 'bg-blue-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                                >
                                    支撐
                                </button>
                                <button
                                    onClick={() => setVpDirection('resistance')}
                                    className={`px-2 py-0.5 text-[10px] font-bold rounded transition-colors ${vpDirection === 'resistance' ? 'bg-red-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                                >
                                    壓力
                                </button>
                            </div>
                        </div>
                        <input
                            type="range"
                            min="0"
                            max="0.1"
                            step="0.01"
                            value={tolerance}
                            onChange={(e) => setTolerance(parseFloat(e.target.value))}
                            className="w-full h-1 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-blue-500"
                        />
                    </div>
                )}

                {/* MA Controls */}
                {activeFilter === 'ma' && (
                    <div className="flex items-center justify-center bg-slate-800 rounded border border-slate-700 px-2 py-1 w-full h-full overflow-x-auto">
                        <div className="flex bg-slate-900 rounded p-0.5 whitespace-nowrap">
                            <button
                                onClick={() => setMaPattern('below_ma200')}
                                className={`px-2 py-1 text-[10px] font-bold rounded transition-colors ${maPattern === 'below_ma200' ? 'bg-blue-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                            >
                                [1] 低於MA200
                            </button>
                            <button
                                onClick={() => setMaPattern('below_ma20')}
                                className={`px-2 py-1 text-[10px] font-bold rounded transition-colors ${maPattern === 'below_ma20' ? 'bg-blue-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                            >
                                [2] 低於MA20
                            </button>
                            <button
                                onClick={() => setMaPattern('bull')}
                                className={`px-2 py-1 text-[10px] font-bold rounded transition-colors ${maPattern === 'bull' ? 'bg-blue-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                            >
                                [3] 均線多頭
                            </button>
                        </div>
                    </div>
                )}
                {/* Pattern Type Toggle (only for [b] K線型態) */}
                {activeFilter === 'patterns' && (
                    <div className="flex items-center gap-2">
                        <span className="text-slate-400 text-xs font-medium">型態:</span>
                        <div className="flex gap-1 bg-slate-800/60 p-0.5 rounded">
                            <button
                                onClick={() => setPatternType('morning_star')}
                                className={`px-2 py-1 text-[10px] font-bold rounded transition-colors ${patternType === 'morning_star' ? 'bg-green-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                            >
                                [1] 晨星
                            </button>
                            <button
                                onClick={() => setPatternType('evening_star')}
                                className={`px-2 py-1 text-[10px] font-bold rounded transition-colors ${patternType === 'evening_star' ? 'bg-red-500 text-white' : 'text-slate-400 hover:text-slate-200'}`}
                            >
                                [2] 黃昏之星
                            </button>
                        </div>
                    </div>
                )}
            </div>

            {/* Column Selector Panel (Toggled) */}


            {/* Screening Process Panel (Info Box) */}
            {/* Screening Process Panel (Info Box) */}
            <div className="bg-slate-800/80 border border-blue-500/30 rounded p-3 mb-4 text-sm text-slate-300 shadow-sm">
                <div className="flex flex-col gap-3">
                    {/* Row 1: Logic */}
                    <div className="flex flex-col sm:flex-row sm:items-start gap-1 sm:gap-3">
                        <div className="text-blue-400 font-bold whitespace-nowrap shrink-0">ℹ️ 篩選邏輯:</div>
                        <div className="text-white font-medium whitespace-normal break-words leading-relaxed">{activeDesc}</div>
                    </div>

                    {/* Row 2: Filter Conditions & Controls */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-t border-slate-700/50 pt-2">
                        <div className="flex flex-wrap items-center gap-2">
                            <div className="text-slate-400 font-bold text-xs whitespace-nowrap">篩選條件:</div>
                            <div className="flex items-center gap-2 bg-slate-900/50 px-2 py-1 rounded border border-slate-700">
                                <span className="text-xs text-slate-500 whitespace-nowrap">量&gt;</span>
                                <input
                                    type="number"
                                    value={minVol}
                                    onChange={(e) => setMinVol(Number(e.target.value))}
                                    className="bg-transparent text-white w-12 text-xs focus:outline-none text-right"
                                />
                                <span className="text-xs text-slate-500 whitespace-nowrap">張</span>
                            </div>
                            <div className="flex items-center gap-2 bg-slate-900/50 px-2 py-1 rounded border border-slate-700">
                                <span className="text-xs text-slate-500 whitespace-nowrap">價&gt;</span>
                                <input
                                    type="number"
                                    value={minPrice}
                                    onChange={(e) => setMinPrice(Number(e.target.value))}
                                    className="bg-transparent text-white w-10 text-xs focus:outline-none text-right"
                                />
                                <span className="text-xs text-slate-500 whitespace-nowrap">元</span>
                            </div>
                            {loading && <span className="text-yellow-500 text-xs animate-pulse ml-2">掃描運算中...</span>}
                        </div>

                        <div className="flex items-center justify-end gap-2">
                            {!loading && scanResults.length > 0 && <span className="text-green-400 text-xs font-bold whitespace-nowrap">符合: {scanResults.length} 檔</span>}
                            <button
                                onClick={() => setShowColumnSelector(!showColumnSelector)}
                                className="text-xs bg-slate-800 hover:bg-slate-700 text-slate-400 hover:text-white px-2 py-1 rounded border border-slate-700 transition-colors flex items-center gap-1 whitespace-nowrap"
                            >
                                ⚙️ 欄位 {showColumnSelector ? '▲' : '▼'}
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            {/* Column Selector Panel (Moved Below Info Box) */}
            {
                showColumnSelector && (
                    <div className="mb-4 p-3 bg-slate-800 border border-slate-700 rounded flex flex-wrap gap-x-4 gap-y-2 text-sm animate-in fade-in slide-in-from-top-2 duration-200">
                        {/* Original Columns */}
                        <div className="flex gap-3 border-r border-slate-700 pr-3">
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.vp_high} onChange={() => toggleColumn('vp_high')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> VP上限</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.vp_low} onChange={() => toggleColumn('vp_low')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> VP下限</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.vp_poc} onChange={() => toggleColumn('vp_poc')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> POC</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.vwap} onChange={() => toggleColumn('vwap')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> VWAP</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.mfi} onChange={() => toggleColumn('mfi')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> MFI</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.rsi} onChange={() => toggleColumn('rsi')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> RSI</label>
                        </div>
                        {/* MAs */}
                        <div className="flex gap-3 border-r border-slate-700 pr-3">
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.ma20} onChange={() => toggleColumn('ma20')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> MA20</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.ma25} onChange={() => toggleColumn('ma25')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> MA25</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.ma60} onChange={() => toggleColumn('ma60')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> MA60</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.ma120} onChange={() => toggleColumn('ma120')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> MA120</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.ma200} onChange={() => toggleColumn('ma200')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> MA200</label>
                        </div>
                        {/* VolMAs */}
                        <div className="flex gap-3 border-r border-slate-700 pr-3">
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.vol_ma5} onChange={() => toggleColumn('vol_ma5')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> VolMA5</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.vol_ma60} onChange={() => toggleColumn('vol_ma60')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> VolMA60</label>
                        </div>
                        {/* Chips */}
                        <div className="flex gap-3">
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.foreign} onChange={() => toggleColumn('foreign')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> 外資</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.trust} onChange={() => toggleColumn('trust')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> 投信</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.dealer} onChange={() => toggleColumn('dealer')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> 自營</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.big_trader} onChange={() => toggleColumn('big_trader')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> 大戶</label>
                            <label className="flex items-center gap-2 cursor-pointer hover:text-white whitespace-nowrap"><input type="checkbox" checked={visibleColumns.concentration} onChange={() => toggleColumn('concentration')} className="rounded bg-slate-700 border-slate-600 text-blue-500" /> 集保</label>
                        </div>
                    </div>
                )
            }

            {/* Results Display */}
            <div className="bg-slate-800/50 rounded-lg p-2 border border-slate-700/50 min-h-[400px]">
                {loading ? (
                    <div className="flex items-center justify-center h-full text-slate-500">
                        <div className="animate-pulse">掃描中...</div>
                    </div>
                ) : (
                    <div className="overflow-x-auto relative">
                        <table className="w-full text-left border-collapse text-sm whitespace-nowrap">
                            <thead>
                                <tr className="bg-slate-800 text-slate-400 border-b border-slate-700">
                                    <th className="p-2 text-left cursor-pointer hover:text-white sticky left-0 z-20 bg-slate-800 shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)]" onClick={() => handleSort('name')}>
                                        股票 {sortConfig.key === 'name' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                    </th>
                                    <th className="p-2 font-bold text-right cursor-pointer hover:text-white" onClick={() => handleSort('close')}>
                                        現價 (%) {sortConfig.key === 'close' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                    </th>
                                    <th className="p-2 font-bold text-right cursor-pointer hover:text-white" onClick={() => handleSort('volume')}>
                                        成交量 (量比) {sortConfig.key === 'volume' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                    </th>
                                    {visibleColumns.vp_high && (
                                        <th className="p-2 font-bold text-right cursor-pointer hover:text-white" onClick={() => handleSort('vp_high')}>
                                            VP 上限 {sortConfig.key === 'vp_high' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                        </th>
                                    )}
                                    {visibleColumns.vp_low && (
                                        <th className="p-2 font-bold text-right cursor-pointer hover:text-white" onClick={() => handleSort('vp_low')}>
                                            VP 下限 {sortConfig.key === 'vp_low' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                        </th>
                                    )}
                                    {visibleColumns.vp_poc && (
                                        <th className="p-2 font-bold text-right text-orange-400 cursor-pointer hover:text-orange-200" onClick={() => handleSort('vp_poc')}>
                                            POC {sortConfig.key === 'vp_poc' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                        </th>
                                    )}
                                    {visibleColumns.vwap && (
                                        <th className="p-2 font-bold text-right text-blue-400 cursor-pointer hover:text-blue-200" onClick={() => handleSort('vwap')}>
                                            VWAP {sortConfig.key === 'vwap' && (sortConfig.direction === 'asc' ? '↑' : '↓')}
                                        </th>
                                    )}
                                    {visibleColumns.mfi && <th className="p-2 font-bold text-right text-purple-400 cursor-pointer hover:text-purple-200" onClick={() => handleSort('mfi')}>MFI {sortConfig.key === 'mfi' && (sortConfig.direction === 'asc' ? '↑' : '↓')}</th>}
                                    {visibleColumns.rsi && <th className="p-2 font-bold text-right text-purple-400 cursor-pointer hover:text-purple-200" onClick={() => handleSort('rsi')}>RSI {sortConfig.key === 'rsi' && (sortConfig.direction === 'asc' ? '↑' : '↓')}</th>}

                                    {visibleColumns.ma20 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('ma20')}>MA20</th>}
                                    {visibleColumns.ma25 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('ma25')}>MA25</th>}
                                    {visibleColumns.ma60 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('ma60')}>MA60</th>}
                                    {visibleColumns.ma120 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('ma120')}>MA120</th>}
                                    {visibleColumns.ma200 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('ma200')}>MA200</th>}

                                    {visibleColumns.vol_ma5 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('vol_ma5')}>VolMA5</th>}
                                    {visibleColumns.vol_ma60 && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('vol_ma60')}>VolMA60</th>}

                                    {visibleColumns.foreign && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('foreign')}>外資</th>}
                                    {visibleColumns.trust && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('trust')}>投信</th>}
                                    {visibleColumns.dealer && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('dealer')}>自營</th>}
                                    {visibleColumns.big_trader && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('big_trader')}>大戶</th>}
                                    {visibleColumns.concentration && <th className="p-2 font-bold text-right text-slate-400 cursor-pointer hover:text-white" onClick={() => handleSort('concentration')}>集保</th>}
                                </tr>
                            </thead>
                            <tbody>
                                {currentItems.map((stock) => {
                                    const volRatio = (stock.volume && stock.vol_ma60) ? (stock.volume / stock.vol_ma60).toFixed(1) : '-';

                                    // Calculate distance for VP
                                    let dist = null;
                                    if (activeFilter === 'vp') {
                                        if (vpDirection === 'support' && stock.vp_low) {
                                            dist = ((stock.close - stock.vp_low) / stock.vp_low * 100).toFixed(1);
                                        } else if (vpDirection === 'resistance' && stock.vp_high) {
                                            dist = ((stock.vp_high - stock.close) / stock.vp_high * 100).toFixed(1);
                                        }
                                    }

                                    return (
                                        <tr
                                            key={stock.code}
                                            onClick={() => navigate(`/stock/${stock.code}`)}
                                            className="group border-b border-slate-700/50 hover:bg-slate-700/30 cursor-pointer transition-colors"
                                        >
                                            <td className="p-2 sticky left-0 z-10 bg-slate-900 group-hover:bg-slate-800 transition-colors shadow-[2px_0_5px_-2px_rgba(0,0,0,0.5)]">
                                                <div className="flex flex-col">
                                                    <span className="text-white font-bold">{stock.name.substring(0, 4)}</span>
                                                    <span className="text-slate-500 text-xs font-mono">{stock.code}</span>
                                                </div>
                                            </td>
                                            <td className="p-2 text-right">
                                                <div className="flex flex-col items-end">
                                                    <span className={`font-bold font-mono ${Number(stock.change_pct) > 0 ? 'text-red-400' : Number(stock.change_pct) < 0 ? 'text-green-400' : 'text-slate-400'}`}>
                                                        {stock.close?.toFixed(2) || '-'}
                                                    </span>
                                                    <span className={`text-xs ${Number(stock.change_pct) > 0 ? 'text-red-400' : Number(stock.change_pct) < 0 ? 'text-green-400' : 'text-slate-400'}`}>
                                                        ({Number(stock.change_pct) > 0 ? '+' : ''}{stock.change_pct}%)
                                                    </span>
                                                </div>
                                            </td>
                                            <td className="p-2 text-right">
                                                <div className="flex flex-col items-end">
                                                    <span className="text-slate-200 font-mono">{fmtSheets(stock.volume)} 張</span>
                                                    <span className="text-slate-500 text-xs">({volRatio}x)</span>
                                                </div>
                                            </td>
                                            {visibleColumns.vp_high && (
                                                <td className={`p-2 text-right font-mono ${stock.close > stock.vp_high ? 'text-red-400 font-bold' : stock.close < stock.vp_high ? 'text-green-400' : 'text-slate-300'}`}>
                                                    {Math.round(stock.vp_high || 0)}
                                                </td>
                                            )}
                                            {visibleColumns.vp_low && (
                                                <td className={`p-2 text-right font-mono ${stock.close > stock.vp_low ? 'text-red-400 font-bold' : stock.close < stock.vp_low ? 'text-green-400' : 'text-slate-300'}`}>
                                                    {Math.round(stock.vp_low || 0)}
                                                </td>
                                            )}
                                            {visibleColumns.vp_poc && (
                                                <td className={`p-2 text-right font-bold font-mono ${stock.close > stock.vp_poc ? 'text-red-400' : stock.close < stock.vp_poc ? 'text-green-400' : 'text-orange-400'}`}>
                                                    {fmtPrice(stock.vp_poc)}
                                                </td>
                                            )}
                                            {visibleColumns.vwap && (
                                                <td className={`p-2 text-right font-bold font-mono ${stock.close > stock.vwap ? 'text-red-400' : stock.close < stock.vwap ? 'text-green-400' : 'text-blue-400'}`}>
                                                    {fmtPrice(stock.vwap)}
                                                </td>
                                            )}
                                            {visibleColumns.mfi && <td className={`p-2 text-right font-bold font-mono ${stock.mfi > 50 ? 'text-red-400' : stock.mfi < 50 ? 'text-green-400' : 'text-purple-400'}`}>{stock.mfi ? Math.round(stock.mfi) : '-'}</td>}
                                            {visibleColumns.rsi && <td className={`p-2 text-right font-bold font-mono ${stock.rsi > 50 ? 'text-red-400' : stock.rsi < 50 ? 'text-green-400' : 'text-purple-400'}`}>{stock.rsi ? Math.round(stock.rsi) : '-'}</td>}

                                            {visibleColumns.ma20 && <td className={`p-2 text-right font-mono ${stock.close > stock.ma20 ? 'text-red-400' : stock.close < stock.ma20 ? 'text-green-400' : 'text-slate-300'}`}>{fmtPrice(stock.ma20)}</td>}
                                            {visibleColumns.ma25 && <td className={`p-2 text-right font-mono ${stock.close > stock.ma25 ? 'text-red-400' : stock.close < stock.ma25 ? 'text-green-400' : 'text-slate-300'}`}>{fmtPrice(stock.ma25)}</td>}
                                            {visibleColumns.ma60 && <td className={`p-2 text-right font-mono ${stock.close > stock.ma60 ? 'text-red-400' : stock.close < stock.ma60 ? 'text-green-400' : 'text-slate-300'}`}>{fmtPrice(stock.ma60)}</td>}
                                            {visibleColumns.ma120 && <td className={`p-2 text-right font-mono ${stock.close > stock.ma120 ? 'text-red-400' : stock.close < stock.ma120 ? 'text-green-400' : 'text-slate-300'}`}>{fmtPrice(stock.ma120)}</td>}
                                            {visibleColumns.ma200 && <td className={`p-2 text-right font-mono ${stock.close > stock.ma200 ? 'text-red-400' : stock.close < stock.ma200 ? 'text-green-400' : 'text-slate-300'}`}>{fmtPrice(stock.ma200)}</td>}

                                            {visibleColumns.vol_ma5 && <td className={`p-2 text-right font-mono ${stock.vol_ma5 > stock.vol_ma60 ? 'text-red-400' : stock.vol_ma5 < stock.vol_ma60 ? 'text-green-400' : 'text-slate-300'}`}>{fmtSheets(stock.vol_ma5)}</td>}
                                            {visibleColumns.vol_ma60 && <td className={`p-2 text-right font-mono ${stock.vol_ma5 > stock.vol_ma60 ? 'text-red-400' : stock.vol_ma5 < stock.vol_ma60 ? 'text-green-400' : 'text-slate-300'}`}>{fmtSheets(stock.vol_ma60)}</td>}

                                            {visibleColumns.foreign && <td className={`p-2 text-right font-bold font-mono ${stock.foreign > 0 ? 'text-red-400' : stock.foreign < 0 ? 'text-green-400' : 'text-slate-500'}`}>{fmtSheets(stock.foreign)}</td>}
                                            {visibleColumns.trust && <td className={`p-2 text-right font-bold font-mono ${stock.trust > 0 ? 'text-red-400' : stock.trust < 0 ? 'text-green-400' : 'text-slate-500'}`}>{fmtSheets(stock.trust)}</td>}
                                            {visibleColumns.dealer && <td className={`p-2 text-right font-bold font-mono ${stock.dealer > 0 ? 'text-red-400' : stock.dealer < 0 ? 'text-green-400' : 'text-slate-500'}`}>{fmtSheets(stock.dealer)}</td>}
                                            {visibleColumns.big_trader && <td className={`p-2 text-right font-bold font-mono ${stock.big_trader > 0 ? 'text-red-400' : stock.big_trader < 0 ? 'text-green-400' : 'text-slate-500'}`}>{stock.big_trader ? stock.big_trader.toFixed(2) + '%' : '-'}</td>}
                                            {visibleColumns.concentration && <td className="p-2 text-right text-slate-300 font-mono">{stock.concentration ? stock.concentration.toLocaleString() : '-'}</td>}
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                )}

                {/* Pagination */}
                {scanResults.length > 0 && (
                    <div className="mt-4 pt-2 border-t border-slate-700/50 flex flex-col sm:flex-row justify-between items-center gap-4 text-sm text-slate-500">
                        <span>顯示 {indexOfFirstItem + 1}-{Math.min(indexOfLastItem, scanResults.length)} 筆，共 {scanResults.length} 筆</span>
                        <div className="flex gap-1">
                            <button onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 1} className="px-2 py-1 bg-slate-700 rounded hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed">上一頁</button>
                            <button onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage === totalPages} className="px-2 py-1 bg-slate-700 rounded hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed">下一頁</button>
                        </div>
                    </div>
                )}
            </div>
        </div >
    );
};
