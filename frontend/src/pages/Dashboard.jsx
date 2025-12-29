import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { createChart, CandlestickSeries, LineSeries, HistogramSeries } from 'lightweight-charts';

import { useMobileView } from "@/context/MobileViewContext";

export function Dashboard() {
    const navigate = useNavigate();
    const { isMobileView } = useMobileView();
    const [period, setPeriod] = useState('日');
    const periods = ['日', '週', '月'];
    const [activeIndicators, setActiveIndicators] = useState(['MA20', 'MA60', 'MA120', 'MA200']);
    const subIndicators = ['KD', 'RSI', 'MACD', 'MFI', 'NVI/PVI', 'SMI/SVI', 'ADL', '外資', '投信', '自營', '集保', '大戶'];
    const [activeSubIndicator, setActiveSubIndicator] = useState('KD');
    const [activeSubIndicator2, setActiveSubIndicator2] = useState('MACD');
    const [chartData, setChartData] = useState([]);
    const [stockInfo, setStockInfo] = useState({ name: '加權指數', code: '0000' });
    const [hoverIdx, setHoverIdx] = useState(-1);
    const [searchTerm, setSearchTerm] = useState('');
    const [stockList, setStockList] = useState([]);
    const [filteredStocks, setFilteredStocks] = useState([]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const [shareholderThreshold, setShareholderThreshold] = useState(1000);
    // const [isMobileView, setIsMobileView] = useState(false); // Moved to Context

    const mainContainerRef = useRef(null);
    const volumeContainerRef = useRef(null);
    const subChartContainerRef = useRef(null);
    const subChartContainerRef2 = useRef(null);
    const chartRefs = useRef({ main: null, volume: null, subChart: null, subChart2: null, subSeries: {}, subSeries2: {}, isDisposed: false });

    const dataRef = useRef([]);

    // Chart Heights
    const [chartHeights, setChartHeights] = useState({ main: 300, volume: 80, sub1: 120, sub2: 120 });
    const isResizing = useRef(null);
    const lastTouchY = useRef(0);

    const handleResizeStart = (type) => (e) => {
        isResizing.current = type;
        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';
    };

    const handleResizeMove = useCallback((e) => {
        if (!isResizing.current) return;
        const deltaY = e.movementY;
        setChartHeights(prev => {
            const newHeights = { ...prev };
            if (isResizing.current === 'main-vol') {
                newHeights.main = Math.max(100, prev.main + deltaY);
            } else if (isResizing.current === 'vol-sub1') {
                newHeights.volume = Math.max(50, prev.volume + deltaY);
            } else if (isResizing.current === 'sub1-sub2') {
                newHeights.sub1 = Math.max(50, prev.sub1 + deltaY);
            } else if (isResizing.current === 'sub2-bottom') {
                newHeights.sub2 = Math.max(50, prev.sub2 + deltaY);
            }
            return newHeights;
        });
    }, []);

    const handleResizeEnd = useCallback(() => {
        isResizing.current = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    }, []);

    const handleTouchStart = (type) => (e) => {
        isResizing.current = type;
        lastTouchY.current = e.touches[0].clientY;
        document.body.style.overflow = 'hidden'; // Prevent scrolling while resizing
    };

    const handleTouchMove = useCallback((e) => {
        if (!isResizing.current) return;
        const currentY = e.touches[0].clientY;
        const deltaY = currentY - lastTouchY.current;
        lastTouchY.current = currentY;

        setChartHeights(prev => {
            const newHeights = { ...prev };
            if (isResizing.current === 'main-vol') {
                newHeights.main = Math.max(100, prev.main + deltaY);
            } else if (isResizing.current === 'vol-sub1') {
                newHeights.volume = Math.max(50, prev.volume + deltaY);
            } else if (isResizing.current === 'sub1-sub2') {
                newHeights.sub1 = Math.max(50, prev.sub1 + deltaY);
            } else if (isResizing.current === 'sub2-bottom') {
                newHeights.sub2 = Math.max(50, prev.sub2 + deltaY);
            }
            return newHeights;
        });
    }, []);

    const handleTouchEnd = useCallback(() => {
        isResizing.current = null;
        document.body.style.overflow = '';
    }, []);

    useEffect(() => {
        window.addEventListener('mousemove', handleResizeMove);
        window.addEventListener('mouseup', handleResizeEnd);
        window.addEventListener('touchmove', handleTouchMove);
        window.addEventListener('touchend', handleTouchEnd);
        return () => {
            window.removeEventListener('mousemove', handleResizeMove);
            window.removeEventListener('mouseup', handleResizeEnd);
            window.removeEventListener('touchmove', handleTouchMove);
            window.removeEventListener('touchend', handleTouchEnd);
        };
    }, [handleResizeMove, handleResizeEnd, handleTouchMove, handleTouchEnd]);

    const indicatorConfig = [
        { name: 'MA20', color: '#f97316' }, { name: 'MA60', color: '#a855f7' },
        { name: 'MA120', color: '#3b82f6' }, { name: 'MA200', color: '#ef4444' },
        { name: 'VWAP', color: '#eab308' }, { name: 'BBW', color: '#6366f1' },
        { name: 'VP', color: '#14b8a6' }, { name: 'VSBC', color: '#ec4899' }, { name: 'Fib', color: '#84cc16' },
    ];

    const [rawData, setRawData] = useState([]);

    // Fetch Stock List
    useEffect(() => {
        fetch('http://localhost:8000/api/stocks?limit=5000')
            .then(res => res.json())
            .then(data => {
                if (data.success) {
                    setStockList(data.data.stocks);
                }
            })
            .catch(err => console.error('Stock list fetch error:', err));
    }, []);

    // Filter Stocks
    useEffect(() => {
        if (!searchTerm) {
            setFilteredStocks([]);
            return;
        }
        const lower = searchTerm.toLowerCase();
        const filtered = stockList.filter(s =>
            s.code.includes(lower) || s.name.includes(lower)
        ).slice(0, 10);
        setFilteredStocks(filtered);
    }, [searchTerm, stockList]);

    // Fetch History & Shareholding
    useEffect(() => {
        const fetchData = async () => {
            try {
                console.log('Fetching data for:', stockInfo.code);
                const res = await fetch(`http://localhost:8000/api/stocks/${stockInfo.code}/history?limit=2000`);
                const json = await res.json();

                const shRes = await fetch(`http://localhost:8000/api/stocks/${stockInfo.code}/shareholding?threshold=${shareholderThreshold}`);
                const shJson = await shRes.json();

                // 建立兩個 Map：一個用於集保總人數，一個用於大戶持股
                const totalHoldersMap = new Map();
                const largeHoldersMap = new Map();

                if (shJson.success) {
                    // 集保總人數 (不分門檻)
                    if (shJson.data.total_holders) {
                        shJson.data.total_holders.forEach(item => totalHoldersMap.set(item.date_int, item.total_holders));
                    }
                    // 大戶持股 (依門檻篩選)
                    if (shJson.data.large_holders) {
                        shJson.data.large_holders.forEach(item => largeHoldersMap.set(item.date_int, item.proportion));
                    }
                }

                if (json.success) {
                    const formatted = json.data.history.map(item => {
                        const dateStr = String(item.date_int);
                        return {
                            time: `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`,
                            open: Number(item.open), high: Number(item.high), low: Number(item.low), close: Number(item.close),
                            value: Number(item.volume),
                            amount: Number(item.amount || 0),
                            foreign: Number(item.foreign_buy || 0),
                            trust: Number(item.trust_buy || 0),
                            dealer: Number(item.dealer_buy || 0),
                            tdcc: totalHoldersMap.get(item.date_int) || Number(item.tdcc_count || 0),
                            large: largeHoldersMap.get(item.date_int) || Number(item.large_shareholder_pct || 0),
                            color: Number(item.close) >= Number(item.open) ? '#ef4444' : '#22c55e'
                        };
                    });
                    formatted.sort((a, b) => new Date(a.time) - new Date(b.time));
                    console.log('Fetched data:', formatted.length, 'records, date range:', formatted[0]?.time, '-', formatted[formatted.length - 1]?.time);
                    setRawData(formatted);
                }
            } catch (err) { console.error('Fetch error:', err); }
        };
        fetchData();
    }, [stockInfo.code, shareholderThreshold]);

    // Aggregate Data
    useEffect(() => {
        if (rawData.length === 0) return;
        let processed = rawData;
        if (period === '週') {
            const map = new Map();
            rawData.forEach(d => {
                const date = new Date(d.time);
                date.setDate(date.getDate() - date.getDay());
                const key = date.toISOString().slice(0, 10);
                if (!map.has(key)) map.set(key, { time: key, open: d.open, high: d.high, low: d.low, close: d.close, value: d.value, amount: d.amount, foreign: d.foreign, trust: d.trust, dealer: d.dealer, tdcc: d.tdcc, large: d.large });
                else { const w = map.get(key); w.high = Math.max(w.high, d.high); w.low = Math.min(w.low, d.low); w.close = d.close; w.value += d.value; w.amount += d.amount; w.foreign += d.foreign; w.trust += d.trust; w.dealer += d.dealer; w.tdcc = d.tdcc; w.large = d.large; }
            });
            processed = Array.from(map.values()).map(d => ({ ...d, color: d.close >= d.open ? '#ef4444' : '#22c55e' }));
        } else if (period === '月') {
            const map = new Map();
            rawData.forEach(d => {
                const key = d.time.slice(0, 7) + '-01';
                if (!map.has(key)) map.set(key, { time: key, open: d.open, high: d.high, low: d.low, close: d.close, value: d.value, amount: d.amount, foreign: d.foreign, trust: d.trust, dealer: d.dealer, tdcc: d.tdcc, large: d.large });
                else { const m = map.get(key); m.high = Math.max(m.high, d.high); m.low = Math.min(m.low, d.low); m.close = d.close; m.value += d.value; m.amount += d.amount; m.foreign += d.foreign; m.trust += d.trust; m.dealer += d.dealer; m.tdcc = d.tdcc; m.large = d.large; }
            });
            processed = Array.from(map.values()).map(d => ({ ...d, color: d.close >= d.open ? '#ef4444' : '#22c55e' }));
        }
        setChartData(processed);
        dataRef.current = processed;
        setHoverIdx(processed.length - 1);
    }, [rawData, period]);

    // Indicators Calculations
    const calculateMA = useCallback((data, p, key = 'close') => {
        const result = [];
        for (let i = 0; i < data.length; i++) {
            if (i < p - 1) { result.push(null); continue; }
            let sum = 0;
            for (let j = 0; j < p; j++) sum += data[i - j][key];
            result.push(sum / p);
        }
        return result;
    }, []);

    const calculateKD = useCallback((data, kPeriod = 9) => {
        const kArr = [], dArr = [];
        let prevK = 50, prevD = 50;
        for (let i = 0; i < data.length; i++) {
            if (i < kPeriod - 1) { kArr.push(null); dArr.push(null); continue; }
            let highest = data[i].high, lowest = data[i].low;
            for (let j = 0; j < kPeriod; j++) { highest = Math.max(highest, data[i - j].high); lowest = Math.min(lowest, data[i - j].low); }
            const rsv = highest === lowest ? 50 : ((data[i].close - lowest) / (highest - lowest)) * 100;
            const k = (2 / 3) * prevK + (1 / 3) * rsv;
            const d = (2 / 3) * prevD + (1 / 3) * k;
            kArr.push(k); dArr.push(d);
            prevK = k; prevD = d;
        }
        return { k: kArr, d: dArr };
    }, []);

    const calculateMACD = useCallback((data) => {
        const difArr = [], macdArr = [], oscArr = [];
        const k12 = 2 / 13, k26 = 2 / 27, kSig = 2 / 10;
        let ema12 = 0, ema26 = 0, sig = 0;
        for (let i = 0; i < data.length; i++) {
            const c = data[i].close;
            ema12 = i === 0 ? c : c * k12 + ema12 * (1 - k12);
            ema26 = i === 0 ? c : c * k26 + ema26 * (1 - k26);
            const dif = ema12 - ema26;
            sig = i === 0 ? dif : dif * kSig + sig * (1 - kSig);
            difArr.push(dif); macdArr.push(sig); oscArr.push(dif - sig);
        }
        return { dif: difArr, macd: macdArr, osc: oscArr };
    }, []);

    const calculateRSI = useCallback((data, p) => {
        const result = [];
        let avgGain = 0, avgLoss = 0;
        for (let i = 0; i < data.length; i++) {
            if (i === 0) { result.push(null); continue; }
            const change = data[i].close - data[i - 1].close;
            const gain = change > 0 ? change : 0, loss = change < 0 ? -change : 0;
            if (i <= p) { avgGain += gain; avgLoss += loss; if (i === p) { avgGain /= p; avgLoss /= p; } result.push(i < p ? null : 100 - (100 / (1 + (avgLoss === 0 ? 100 : avgGain / avgLoss)))); }
            else { avgGain = (avgGain * (p - 1) + gain) / p; avgLoss = (avgLoss * (p - 1) + loss) / p; result.push(100 - (100 / (1 + (avgLoss === 0 ? 100 : avgGain / avgLoss)))); }
        }
        return result;
    }, []);

    const calculateMFI = useCallback((data, p = 14) => {
        const result = [];
        const mfPositive = [], mfNegative = [];
        for (let i = 0; i < data.length; i++) {
            if (i === 0) { result.push(null); mfPositive.push(0); mfNegative.push(0); continue; }
            const tp = (data[i].high + data[i].low + data[i].close) / 3;
            const tpPrev = (data[i - 1].high + data[i - 1].low + data[i - 1].close) / 3;
            const mf = tp * data[i].value;
            if (tp > tpPrev) { mfPositive.push(mf); mfNegative.push(0); }
            else if (tp < tpPrev) { mfPositive.push(0); mfNegative.push(mf); }
            else { mfPositive.push(0); mfNegative.push(0); }
            if (i < p) { result.push(null); continue; }
            let posSum = 0, negSum = 0;
            for (let j = i - p + 1; j <= i; j++) { posSum += mfPositive[j]; negSum += mfNegative[j]; }
            const mfi = negSum === 0 ? 100 : 100 - (100 / (1 + posSum / negSum));
            result.push(mfi);
        }
        return result;
    }, []);

    const calculateVWAP = useCallback((data) => {
        const result = [];
        let cumPV = 0, cumV = 0;
        for (let i = 0; i < data.length; i++) {
            const tp = (data[i].high + data[i].low + data[i].close) / 3;
            cumPV += tp * data[i].value;
            cumV += data[i].value;
            result.push(cumV === 0 ? null : cumPV / cumV);
        }
        return result;
    }, []);

    const calculateBollinger = useCallback((data, p = 20) => {
        const upper = [], mid = [], lower = [];
        for (let i = 0; i < data.length; i++) {
            if (i < p - 1) { upper.push(null); mid.push(null); lower.push(null); continue; }
            let sum = 0, sumSq = 0;
            for (let j = 0; j < p; j++) { sum += data[i - j].close; sumSq += data[i - j].close ** 2; }
            const mean = sum / p;
            const std = Math.sqrt(sumSq / p - mean ** 2);
            mid.push(mean);
            upper.push(mean + 2 * std);
            lower.push(mean - 2 * std);
        }
        return { upper, mid, lower };
    }, []);

    const calculateVP = useCallback((data, lookback = 20) => {
        const poc = [], vpUpper = [], vpLower = [];
        for (let i = 0; i < data.length; i++) {
            if (i < lookback - 1) { poc.push(null); vpUpper.push(null); vpLower.push(null); continue; }
            const high = Math.max(...data.slice(i - lookback + 1, i + 1).map(d => d.high));
            const low = Math.min(...data.slice(i - lookback + 1, i + 1).map(d => d.low));
            const step = (high - low) / 10 || 1;
            const volAtPrice = new Map();
            for (let j = i - lookback + 1; j <= i; j++) {
                const priceKey = Math.round((data[j].close - low) / step);
                volAtPrice.set(priceKey, (volAtPrice.get(priceKey) || 0) + data[j].value);
            }
            let maxVol = 0, pocKey = 5;
            volAtPrice.forEach((vol, key) => { if (vol > maxVol) { maxVol = vol; pocKey = key; } });
            const pocPrice = low + pocKey * step + step / 2;
            const sortedPrices = [...volAtPrice.entries()].sort((a, b) => b[1] - a[1]);
            const totalVol = sortedPrices.reduce((s, [, v]) => s + v, 0);
            let cumVol = 0, vaKeys = [];
            for (const [key, vol] of sortedPrices) { cumVol += vol; vaKeys.push(key); if (cumVol >= totalVol * 0.7) break; }
            const vaHigh = low + (Math.max(...vaKeys) + 1) * step;
            const vaLow = low + Math.min(...vaKeys) * step;
            poc.push(pocPrice); vpUpper.push(vaHigh); vpLower.push(vaLow);
        }
        return { poc, upper: vpUpper, lower: vpLower };
    }, []);

    const calculateVSBC = useCallback((data, win = 10) => {
        const upper = [], lower = [];
        for (let i = 0; i < data.length; i++) {
            if (i < win - 1) { upper.push(null); lower.push(null); continue; }
            let signedVolSum = 0, volSum = 0, rangeSum = 0, midSum = 0;
            for (let j = 0; j < win; j++) {
                const d = data[i - j];
                const sign = d.close >= d.open ? 1 : -1;
                signedVolSum += sign * d.value;
                volSum += d.value;
                rangeSum += d.high - d.low;
                midSum += (d.high + d.low) / 2;
            }
            const avgRange = rangeSum / win || 1;
            const baseMid = midSum / win;
            const shift = Math.max(-0.5, Math.min(0.5, signedVolSum / (volSum || 1)));
            const vsbcMid = baseMid + shift * avgRange;
            upper.push(vsbcMid + avgRange * 0.5);
            lower.push(vsbcMid - avgRange * 0.5);
        }
        return { upper, lower };
    }, []);

    const calculateADL = useCallback((data) => {
        const result = [];
        let adl = 0;
        for (let i = 0; i < data.length; i++) {
            const hl = data[i].high - data[i].low;
            const mfm = hl === 0 ? 0 : ((data[i].close - data[i].low) - (data[i].high - data[i].close)) / hl;
            adl += mfm * data[i].value;
            result.push(adl);
        }
        return result;
    }, []);

    const calculateNVI = useCallback((data) => {
        const result = [];
        let nvi = 1000;
        for (let i = 0; i < data.length; i++) {
            if (i > 0 && data[i].value < data[i - 1].value) {
                nvi += nvi * ((data[i].close - data[i - 1].close) / data[i - 1].close);
            }
            result.push(nvi);
        }
        return result;
    }, []);

    const calculatePVI = useCallback((data) => {
        const result = [];
        let pvi = 1000;
        for (let i = 0; i < data.length; i++) {
            if (i > 0 && data[i].value > data[i - 1].value) {
                pvi += pvi * ((data[i].close - data[i - 1].close) / data[i - 1].close);
            }
            result.push(pvi);
        }
        return result;
    }, []);

    const calculateSMI = useCallback((data, p = 14) => {
        const result = [];
        for (let i = 0; i < data.length; i++) {
            if (i < p - 1) { result.push(null); continue; }
            let highMax = data[i].high, lowMin = data[i].low;
            for (let j = 1; j < p; j++) { highMax = Math.max(highMax, data[i - j].high); lowMin = Math.min(lowMin, data[i - j].low); }
            const midpoint = (highMax + lowMin) / 2;
            const range = highMax - lowMin;
            const smi = range === 0 ? 0 : ((data[i].close - midpoint) / (range / 2)) * 100;
            result.push(smi);
        }
        return result;
    }, []);

    // Memoize Indicators
    const volumeMA5 = useMemo(() => calculateMA(chartData, 5, 'value'), [chartData, calculateMA]);
    const volumeMA60 = useMemo(() => calculateMA(chartData, 60, 'value'), [chartData, calculateMA]);
    const kdData = useMemo(() => calculateKD(chartData), [chartData, calculateKD]);
    const macdData = useMemo(() => calculateMACD(chartData), [chartData, calculateMACD]);
    const rsi5Data = useMemo(() => calculateRSI(chartData, 5), [chartData, calculateRSI]);
    const rsi10Data = useMemo(() => calculateRSI(chartData, 10), [chartData, calculateRSI]);
    const mfiData = useMemo(() => calculateMFI(chartData), [chartData, calculateMFI]);
    const vwapData = useMemo(() => calculateVWAP(chartData), [chartData, calculateVWAP]);
    const bollingerData = useMemo(() => calculateBollinger(chartData), [chartData, calculateBollinger]);
    const vpData = useMemo(() => calculateVP(chartData), [chartData, calculateVP]);
    const vsbcData = useMemo(() => calculateVSBC(chartData), [chartData, calculateVSBC]);
    const adlData = useMemo(() => calculateADL(chartData), [chartData, calculateADL]);
    const nviData = useMemo(() => calculateNVI(chartData), [chartData, calculateNVI]);
    const pviData = useMemo(() => calculatePVI(chartData), [chartData, calculatePVI]);
    const smiData = useMemo(() => calculateSMI(chartData), [chartData, calculateSMI]);
    const tdccData = useMemo(() => chartData.map(d => d.tdcc), [chartData]);
    const largeData = useMemo(() => chartData.map(d => d.large), [chartData]);

    // Initialize Charts
    useEffect(() => {
        if (!mainContainerRef.current || !volumeContainerRef.current || !subChartContainerRef.current || !subChartContainerRef2.current) return;
        if (chartRefs.current.main) return;

        const width = mainContainerRef.current.clientWidth || 800;
        const chartOpts = (h, showTime = false) => ({
            width, height: h,
            layout: { background: { type: 'solid', color: '#0f172a' }, textColor: '#94a3b8' },
            grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
            crosshair: { mode: 0 },
            rightPriceScale: { borderColor: '#334155', scaleMargins: { top: 0.1, bottom: 0.1 }, minimumWidth: 80 },
            timeScale: { visible: showTime, borderColor: '#334155', timeVisible: true, rightOffset: 5, fixRightEdge: true },
        });

        const mainChart = createChart(mainContainerRef.current, chartOpts(chartHeights.main));
        const candleSeries = mainChart.addSeries(CandlestickSeries, { upColor: '#ef4444', downColor: '#22c55e', borderUpColor: '#ef4444', borderDownColor: '#22c55e', wickUpColor: '#ef4444', wickDownColor: '#22c55e', lastValueVisible: false, priceLineVisible: false });

        // 成交量格式化函數 (萬/億)
        const volumeFormatter = (val) => {
            if (val >= 100000000) return (val / 100000000).toFixed(1) + '億';
            if (val >= 10000) return (val / 10000).toFixed(0) + '萬';
            return val.toFixed(0);
        };

        const volumeChart = createChart(volumeContainerRef.current, {
            ...chartOpts(chartHeights.volume),
            rightPriceScale: { borderColor: '#334155', scaleMargins: { top: 0.1, bottom: 0.1 }, minimumWidth: 80 },
            localization: { priceFormatter: volumeFormatter }
        });
        const volumeSeries = volumeChart.addSeries(HistogramSeries, { priceFormat: { type: 'custom', formatter: volumeFormatter, minMove: 1 }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
        const volMA5Series = volumeChart.addSeries(LineSeries, { color: '#3b82f6', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
        const volMA60Series = volumeChart.addSeries(LineSeries, { color: '#a855f7', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });

        const subChart = createChart(subChartContainerRef.current, chartOpts(chartHeights.sub1, false));
        const subChart2 = createChart(subChartContainerRef2.current, chartOpts(chartHeights.sub2, true));

        chartRefs.current = { main: mainChart, volume: volumeChart, subChart, subChart2, candleSeries, volumeSeries, volMA5Series, volMA60Series, indicatorSeries: {}, subSeries: {}, subSeries2: {}, isDisposed: false };

        const allCharts = [mainChart, volumeChart, subChart, subChart2];
        allCharts.forEach(chart => {
            chart.subscribeCrosshairMove(p => {
                if (chartRefs.current.isDisposed) return;
                const time = p?.time;
                allCharts.forEach(c => { if (c !== chart && time) try { c.setCrosshairPosition(NaN, time, null); } catch { } });
                if (time) {
                    const idx = dataRef.current.findIndex(d => d.time === time);
                    if (idx >= 0) setHoverIdx(idx);
                }
            });
        });

        // Sync Time Scales
        allCharts.forEach(src => {
            src.timeScale().subscribeVisibleLogicalRangeChange(r => {
                if (r && !chartRefs.current.isDisposed) {
                    allCharts.forEach(c => {
                        if (c !== src) {
                            try { c.timeScale().setVisibleLogicalRange(r); } catch { }
                        }
                    });
                }
            });
        });

        // Use ResizeObserver to handle container size changes (including Mobile View toggle)
        const resizeObserver = new ResizeObserver(entries => {
            if (entries.length === 0 || !entries[0].contentRect) return;
            const newWidth = entries[0].contentRect.width;
            allCharts.forEach(c => c.applyOptions({ width: newWidth }));
        });
        resizeObserver.observe(mainContainerRef.current);

        return () => {
            chartRefs.current.isDisposed = true;
            resizeObserver.disconnect();
            allCharts.forEach(c => c.remove());
            chartRefs.current.main = null;
        };
    }, []);

    // Resize Effect
    useEffect(() => {
        const refs = chartRefs.current;
        if (refs.main) refs.main.applyOptions({ height: chartHeights.main });
        if (refs.volume) refs.volume.applyOptions({ height: chartHeights.volume });
        if (refs.subChart) refs.subChart.applyOptions({ height: chartHeights.sub1 });
        if (refs.subChart2) refs.subChart2.applyOptions({ height: chartHeights.sub2 });
    }, [chartHeights]);

    // Update Main Chart Data
    useEffect(() => {
        const refs = chartRefs.current;
        if (!refs.main || chartData.length === 0) {
            console.log('Skipping chart update: main not ready or no data');
            return;
        }

        console.log('Updating chart with', chartData.length, 'candles');

        try {
            refs.candleSeries.setData(chartData);
            refs.volumeSeries.setData(chartData);
            refs.volMA5Series.setData(volumeMA5.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.volMA60Series.setData(volumeMA60.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));

            console.log('Data set successfully, calling fitContent...');

            // Fit content on all charts with longer delay
            setTimeout(() => {
                try {
                    refs.main.timeScale().fitContent();
                    console.log('Main chart fitContent done');
                    if (refs.volume) refs.volume.timeScale().fitContent();
                    if (refs.subChart) refs.subChart.timeScale().fitContent();
                    if (refs.subChart2) refs.subChart2.timeScale().fitContent();
                    console.log('All charts fitContent complete');
                } catch (e) {
                    console.error('fitContent error:', e);
                }
            }, 300);
        } catch (e) {
            console.error('Chart update error:', e);
        }
    }, [chartData, volumeMA5, volumeMA60]);

    // Update Main Indicators
    useEffect(() => {
        const refs = chartRefs.current;
        if (!refs.main || chartData.length === 0) return;
        const colors = { MA20: '#f97316', MA60: '#a855f7', MA120: '#3b82f6', MA200: '#ef4444', VWAP: '#eab308', BBW: '#6366f1', VP: '#14b8a6', VSBC: '#ec4899', Fib: '#84cc16' };
        const periods = { MA20: 20, MA60: 60, MA120: 120, MA200: 200 };

        const multiLineKeys = ['BBW_upper', 'BBW_mid', 'BBW_lower', 'VP_poc', 'VP_upper', 'VP_lower', 'VSBC_upper', 'VSBC_lower', 'Fib236', 'Fib382', 'Fib500', 'Fib618', 'Fib786'];
        Object.keys(refs.indicatorSeries).forEach(k => {
            const baseKey = k.split('_')[0];
            // Fix: Remove series if neither the key nor its base key is active
            if (!activeIndicators.includes(k) && !activeIndicators.includes(baseKey)) {
                try { refs.main.removeSeries(refs.indicatorSeries[k]); } catch { }
                delete refs.indicatorSeries[k];
            }
        });

        activeIndicators.forEach(ind => {
            if (periods[ind]) {
                if (!refs.indicatorSeries[ind]) refs.indicatorSeries[ind] = refs.main.addSeries(LineSeries, { color: colors[ind], lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
                const data = calculateMA(chartData, periods[ind]);
                refs.indicatorSeries[ind].setData(data.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            } else if (ind === 'VWAP') {
                if (!refs.indicatorSeries[ind]) refs.indicatorSeries[ind] = refs.main.addSeries(LineSeries, { color: colors[ind], lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false });
                refs.indicatorSeries[ind].setData(vwapData.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            } else if (ind === 'BBW') {
                ['upper', 'mid', 'lower'].forEach((type, idx) => {
                    const key = `BBW_${type}`;
                    const lineColors = ['#6366f1', '#8b5cf6', '#6366f1'];
                    const styles = [2, 0, 2];
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: lineColors[idx], lineWidth: 1, lineStyle: styles[idx], priceLineVisible: false, lastValueVisible: false });
                    refs.indicatorSeries[key].setData(bollingerData[type].map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
                });
            } else if (ind === 'VP') {
                ['poc', 'upper', 'lower'].forEach((type, idx) => {
                    const key = `VP_${type}`;
                    const lineColors = ['#14b8a6', '#10b981', '#059669'];
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: lineColors[idx], lineWidth: type === 'poc' ? 2 : 1, lineStyle: type === 'poc' ? 0 : 2, priceLineVisible: false, lastValueVisible: false });
                    refs.indicatorSeries[key].setData(vpData[type].map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
                });
            } else if (ind === 'VSBC') {
                ['upper', 'lower'].forEach((type, idx) => {
                    const key = `VSBC_${type}`;
                    const lineColors = ['#ec4899', '#f472b6'];
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: lineColors[idx], lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false });
                    refs.indicatorSeries[key].setData(vsbcData[type].map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
                });
            } else if (ind === 'Fib') {
                const maxHigh = Math.max(...chartData.map(d => d.high));
                const minLow = Math.min(...chartData.map(d => d.low));
                const range = maxHigh - minLow;
                const fibLevels = [0.236, 0.382, 0.5, 0.618, 0.786];
                const fibColors = ['#84cc16', '#22c55e', '#eab308', '#f97316', '#ef4444'];
                fibLevels.forEach((level, idx) => {
                    const key = `Fib${Math.round(level * 1000)}`;
                    const fibValue = minLow + range * level;
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: fibColors[idx], lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: true, title: `${(level * 100).toFixed(1)}%` });
                    refs.indicatorSeries[key].setData(chartData.map(d => ({ time: d.time, value: fibValue })));
                });
            }
        });
    }, [activeIndicators, chartData, calculateMA, vwapData, bollingerData, vpData, vsbcData]);

    // Update Sub Chart 1
    useEffect(() => {
        const refs = chartRefs.current;
        if (!refs.subChart || chartData.length === 0) return;
        Object.values(refs.subSeries).forEach(s => { try { refs.subChart.removeSeries(s); } catch { } });
        refs.subSeries = {};
        const lineOpts = { lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false };
        const histOpts = { priceFormat: { type: 'volume' }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false };

        if (activeSubIndicator === 'KD') {
            const kS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' });
            const dS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#f97316' });
            kS.setData(kdData.k.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            dS.setData(kdData.d.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries = { k: kS, d: dS };
        } else if (activeSubIndicator === 'MACD') {
            const difS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' });
            const macdS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            const histS = refs.subChart.addSeries(HistogramSeries, { priceFormat: { type: 'price', precision: 2 }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
            difS.setData(macdData.dif.map((v, i) => ({ time: chartData[i].time, value: v })));
            macdS.setData(macdData.macd.map((v, i) => ({ time: chartData[i].time, value: v })));
            histS.setData(macdData.osc.map((v, i) => ({ time: chartData[i].time, value: v, color: v >= 0 ? '#ef4444' : '#22c55e' })));
            refs.subSeries = { dif: difS, macd: macdS, hist: histS };
        } else if (activeSubIndicator === 'RSI') {
            const r5S = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' });
            const r10S = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            r5S.setData(rsi5Data.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            r10S.setData(rsi10Data.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries = { r5: r5S, r10: r10S };
        } else if (activeSubIndicator === 'MFI') {
            const mfiS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#eab308' });
            mfiS.setData(mfiData.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries = { mfi: mfiS };
        } else if (activeSubIndicator === 'NVI/PVI') {
            const nviS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#22c55e' });
            const pviS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            nviS.setData(nviData.map((v, i) => ({ time: chartData[i].time, value: v })));
            pviS.setData(pviData.map((v, i) => ({ time: chartData[i].time, value: v })));
            refs.subSeries = { nvi: nviS, pvi: pviS };
        } else if (activeSubIndicator === 'SMI/SVI') {
            const smiS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#06b6d4' });
            const sviS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#f59e0b' });
            smiS.setData(smiData.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            sviS.setData(smiData.map((v, i) => v !== null ? { time: chartData[i].time, value: v * 0.8 } : null).filter(Boolean));
            refs.subSeries = { smi: smiS, svi: sviS };
        } else if (['外資', '投信', '自營'].includes(activeSubIndicator)) {
            const keyMap = { '外資': 'foreign', '投信': 'trust', '自營': 'dealer' };
            const key = keyMap[activeSubIndicator];
            const series = refs.subChart.addSeries(HistogramSeries, { ...histOpts });
            series.setData(chartData.map(d => ({ time: d.time, value: d[key], color: d[key] >= 0 ? '#ef4444' : '#22c55e' })));
            refs.subSeries = { [key]: series };
        } else if (activeSubIndicator === '集保') {
            const tdccS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#8b5cf6' });
            tdccS.setData(tdccData.map((v, i) => v ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries = { tdcc: tdccS };
        } else if (activeSubIndicator === '大戶') {
            const largeS = refs.subChart.addSeries(LineSeries, { ...lineOpts, color: '#ec4899' });
            largeS.setData(largeData.map((v, i) => v ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries = { large: largeS };
        } else {
            const histS = refs.subChart.addSeries(HistogramSeries, { priceFormat: { type: 'price', precision: 2 }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
            histS.setData(macdData.osc.map((v, i) => ({ time: chartData[i].time, value: v, color: v >= 0 ? '#ef4444' : '#22c55e' })));
            refs.subSeries = { hist: histS };
        }
    }, [activeSubIndicator, chartData, kdData, macdData, rsi5Data, rsi10Data, mfiData, nviData, pviData, adlData, smiData, tdccData, largeData]);

    // Update Sub Chart 2
    useEffect(() => {
        const refs = chartRefs.current;
        if (!refs.subChart2 || chartData.length === 0) return;
        Object.values(refs.subSeries2).forEach(s => { try { refs.subChart2.removeSeries(s); } catch { } });
        refs.subSeries2 = {};
        const lineOpts = { lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false };
        const histOpts = { priceFormat: { type: 'volume' }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false };

        if (activeSubIndicator2 === 'KD') {
            const kS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' });
            const dS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#f97316' });
            kS.setData(kdData.k.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            dS.setData(kdData.d.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries2 = { k: kS, d: dS };
        } else if (activeSubIndicator2 === 'MACD') {
            const difS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' });
            const macdS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            const histS = refs.subChart2.addSeries(HistogramSeries, { priceFormat: { type: 'price', precision: 2 }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
            difS.setData(macdData.dif.map((v, i) => ({ time: chartData[i].time, value: v })));
            macdS.setData(macdData.macd.map((v, i) => ({ time: chartData[i].time, value: v })));
            histS.setData(macdData.osc.map((v, i) => ({ time: chartData[i].time, value: v, color: v >= 0 ? '#ef4444' : '#22c55e' })));
            refs.subSeries2 = { dif: difS, macd: macdS, hist: histS };
        } else if (activeSubIndicator2 === 'RSI') {
            const r5S = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' });
            const r10S = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            r5S.setData(rsi5Data.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            r10S.setData(rsi10Data.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries2 = { r5: r5S, r10: r10S };
        } else if (activeSubIndicator2 === 'MFI') {
            const mfiS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#eab308' });
            mfiS.setData(mfiData.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries2 = { mfi: mfiS };
        } else if (activeSubIndicator2 === 'NVI/PVI') {
            const nviS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#22c55e' });
            const pviS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            nviS.setData(nviData.map((v, i) => ({ time: chartData[i].time, value: v })));
            pviS.setData(pviData.map((v, i) => ({ time: chartData[i].time, value: v })));
            refs.subSeries2 = { nvi: nviS, pvi: pviS };
        } else if (activeSubIndicator2 === 'SMI/SVI') {
            const smiS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#06b6d4' });
            const sviS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#f59e0b' });
            smiS.setData(smiData.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            sviS.setData(smiData.map((v, i) => v !== null ? { time: chartData[i].time, value: v * 0.8 } : null).filter(Boolean));
            refs.subSeries2 = { smi: smiS, svi: sviS };
        } else if (activeSubIndicator2 === 'ADL') {
            const adlS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#a855f7' });
            adlS.setData(adlData.map((v, i) => ({ time: chartData[i].time, value: v })));
            refs.subSeries2 = { adl: adlS };
        } else if (['外資', '投信', '自營'].includes(activeSubIndicator2)) {
            const keyMap = { '外資': 'foreign', '投信': 'trust', '自營': 'dealer' };
            const key = keyMap[activeSubIndicator2];
            const series = refs.subChart2.addSeries(HistogramSeries, { ...histOpts });
            series.setData(chartData.map(d => ({ time: d.time, value: d[key], color: d[key] >= 0 ? '#ef4444' : '#22c55e' })));
            refs.subSeries2 = { [key]: series };
        } else if (activeSubIndicator2 === '集保') {
            const tdccS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#8b5cf6' });
            tdccS.setData(tdccData.map((v, i) => v ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries2 = { tdcc: tdccS };
        } else if (activeSubIndicator2 === '大戶') {
            const largeS = refs.subChart2.addSeries(LineSeries, { ...lineOpts, color: '#ec4899' });
            largeS.setData(largeData.map((v, i) => v ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.subSeries2 = { large: largeS };
        } else {
            const histS = refs.subChart2.addSeries(HistogramSeries, { priceFormat: { type: 'price', precision: 2 }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
            histS.setData(macdData.osc.map((v, i) => ({ time: chartData[i].time, value: v, color: v >= 0 ? '#ef4444' : '#22c55e' })));
            refs.subSeries2 = { hist: histS };
        }
    }, [activeSubIndicator2, chartData, kdData, macdData, rsi5Data, rsi10Data, mfiData, nviData, pviData, adlData, smiData, tdccData, largeData]);

    const toggleIndicator = (ind) => setActiveIndicators(prev => prev.includes(ind) ? prev.filter(i => i !== ind) : [...prev, ind]);

    // Hover Data
    const hoverData = chartData[hoverIdx] || null;
    const prevData = chartData[hoverIdx - 1] || null;
    const priceChange = hoverData && prevData ? (hoverData.close - prevData.close).toFixed(2) : '0';
    const priceChangePercent = hoverData && prevData ? ((hoverData.close - prevData.close) / prevData.close * 100).toFixed(2) : '0';
    const volChange = hoverData && prevData ? (hoverData.value - prevData.value) : 0;
    const hoverVolMA5 = volumeMA5[hoverIdx];
    const hoverVolMA60 = volumeMA60[hoverIdx];
    const hoverK = kdData.k[hoverIdx];
    const hoverD = kdData.d[hoverIdx];
    const hoverDif = macdData.dif[hoverIdx];
    const hoverMacd = macdData.macd[hoverIdx];
    const hoverOsc = macdData.osc[hoverIdx];
    const hoverRsi5 = rsi5Data[hoverIdx];
    const hoverRsi10 = rsi10Data[hoverIdx];
    const hoverMfi = mfiData[hoverIdx];
    const hoverNvi = nviData[hoverIdx];
    const hoverPvi = pviData[hoverIdx];
    const hoverAdl = adlData[hoverIdx];
    const hoverSmi = smiData[hoverIdx];

    // Sub Chart Values with Descriptions (CMoney Style)
    const indicatorDescriptions = {
        'KD': 'K值 > D值且向上交叉為黃金交叉，買進訊號；K值 < D值向下交叉為死亡交叉，賣出訊號',
        'MACD': 'DIF由下往上穿越MACD為黃金交叉，買進訊號；DIF由上往下穿越MACD為死亡交叉，賣出訊號',
        'RSI': 'RSI > 80 為超買區，可能回檔；RSI < 20 為超賣區，可能反彈',
        'MFI': 'MFI > 80 為超買區；MFI < 20 為超賣區',
        'NVI/PVI': 'NVI追蹤縮量日走勢(法人動態)；PVI追蹤放量日走勢(散戶行為)',
        'SMI/SVI': 'SMI聰明錢指數(法人買賣力道)；SVI散戶指數(反向參考)',
        'ADL': 'ADL上升表示資金持續流入；ADL下降表示資金持續流出',
        '外資': '外資買賣超金額，正值為買超，負值為賣超',
        '投信': '投信買賣超金額，正值為買超，負值為賣超',
        '自營': '自營商買賣超金額，正值為買超，負值為賣超',
        '集保': '集保戶數變化，戶數減少表示籌碼集中',
        '大戶': '大戶持股比例，比例上升表示主力布局'
    };

    const getSubValues = () => {
        const desc = indicatorDescriptions[activeSubIndicator] || '';
        let values = null;
        switch (activeSubIndicator) {
            case 'KD': values = <><span className="text-blue-400">K(9): {hoverK?.toFixed(2) || '-'}</span> <span className="text-orange-400">D(9): {hoverD?.toFixed(2) || '-'}</span></>; break;
            case 'MACD': values = <><span className="text-blue-400">DIF: {hoverDif?.toFixed(2) || '-'}</span> <span className="text-red-400">MACD: {hoverMacd?.toFixed(2) || '-'}</span> <span className={hoverOsc >= 0 ? 'text-red-400' : 'text-green-400'}>OSC: {hoverOsc?.toFixed(2) || '-'}</span></>; break;
            case 'RSI': values = <><span className="text-blue-400">RSI(5): {hoverRsi5?.toFixed(2) || '-'}</span> <span className="text-red-400">RSI(10): {hoverRsi10?.toFixed(2) || '-'}</span></>; break;
            case 'MFI': values = <span className="text-yellow-400">MFI(14): {hoverMfi?.toFixed(2) || '-'}</span>; break;
            case 'NVI/PVI': values = <><span className="text-green-400">NVI: {hoverNvi?.toFixed(0) || '-'}</span> <span className="text-red-400">PVI: {hoverPvi?.toFixed(0) || '-'}</span></>; break;
            case 'SMI/SVI': values = <><span className="text-cyan-400">SMI: {hoverSmi?.toFixed(2) || '-'}</span> <span className="text-amber-400">SVI: {hoverSmi ? (hoverSmi * 0.8).toFixed(2) : '-'}</span></>; break;
            case 'ADL': values = <span className="text-purple-400">ADL: {hoverAdl ? (hoverAdl / 1e9).toFixed(2) + 'B' : '-'}</span>; break;
            case '外資': values = <span className={hoverData?.foreign >= 0 ? 'text-red-400' : 'text-green-400'}>外資: {hoverData?.foreign ? (hoverData.foreign / 100000000).toFixed(2) + '億' : '-'}</span>; break;
            case '投信': values = <span className={hoverData?.trust >= 0 ? 'text-red-400' : 'text-green-400'}>投信: {hoverData?.trust ? (hoverData.trust / 100000000).toFixed(2) + '億' : '-'}</span>; break;
            case '自營': values = <span className={hoverData?.dealer >= 0 ? 'text-red-400' : 'text-green-400'}>自營: {hoverData?.dealer ? (hoverData.dealer / 100000000).toFixed(2) + '億' : '-'}</span>; break;
            case '集保': values = <span className="text-violet-400">集保戶數: {hoverData?.tdcc || '-'}</span>; break;
            case '大戶': values = <span className="text-pink-400">大戶持股: {hoverData?.large ? hoverData.large.toFixed(2) + '%' : '-'}</span>; break;
            default: values = <span className="text-slate-500">(尚未實作)</span>;
        }
        return <>{values} <span className="text-slate-500 text-[10px] ml-2">{desc}</span></>;
    };

    const getSubValues2 = () => {
        const desc = indicatorDescriptions[activeSubIndicator2] || '';
        let values = null;
        switch (activeSubIndicator2) {
            case 'KD': values = <><span className="text-blue-400">K(9): {hoverK?.toFixed(2) || '-'}</span> <span className="text-orange-400">D(9): {hoverD?.toFixed(2) || '-'}</span></>; break;
            case 'MACD': values = <><span className="text-blue-400">DIF: {hoverDif?.toFixed(2) || '-'}</span> <span className="text-red-400">MACD: {hoverMacd?.toFixed(2) || '-'}</span> <span className={hoverOsc >= 0 ? 'text-red-400' : 'text-green-400'}>OSC: {hoverOsc?.toFixed(2) || '-'}</span></>; break;
            case 'RSI': values = <><span className="text-blue-400">RSI(5): {hoverRsi5?.toFixed(2) || '-'}</span> <span className="text-red-400">RSI(10): {hoverRsi10?.toFixed(2) || '-'}</span></>; break;
            case 'MFI': values = <span className="text-yellow-400">MFI(14): {hoverMfi?.toFixed(2) || '-'}</span>; break;
            case 'NVI/PVI': values = <><span className="text-green-400">NVI: {hoverNvi?.toFixed(0) || '-'}</span> <span className="text-red-400">PVI: {hoverPvi?.toFixed(0) || '-'}</span></>; break;
            case 'SMI/SVI': values = <><span className="text-cyan-400">SMI: {hoverSmi?.toFixed(2) || '-'}</span> <span className="text-amber-400">SVI: {hoverSmi ? (hoverSmi * 0.8).toFixed(2) : '-'}</span></>; break;
            case 'ADL': values = <span className="text-purple-400">ADL: {hoverAdl ? (hoverAdl / 1e9).toFixed(2) + 'B' : '-'}</span>; break;
            case '外資': values = <span className={hoverData?.foreign >= 0 ? 'text-red-400' : 'text-green-400'}>外資: {hoverData?.foreign ? (hoverData.foreign / 100000000).toFixed(2) + '億' : '-'}</span>; break;
            case '投信': values = <span className={hoverData?.trust >= 0 ? 'text-red-400' : 'text-green-400'}>投信: {hoverData?.trust ? (hoverData.trust / 100000000).toFixed(2) + '億' : '-'}</span>; break;
            case '自營': values = <span className={hoverData?.dealer >= 0 ? 'text-red-400' : 'text-green-400'}>自營: {hoverData?.dealer ? (hoverData.dealer / 100000000).toFixed(2) + '億' : '-'}</span>; break;
            case '集保': values = <span className="text-violet-400">集保戶數: {hoverData?.tdcc || '-'}</span>; break;
            case '大戶': values = <span className="text-pink-400">大戶持股: {hoverData?.large ? hoverData.large.toFixed(2) + '%' : '-'}</span>; break;
            default: values = <span className="text-slate-500">(尚未實作)</span>;
        }
        return <>{values} <span className="text-slate-500 text-[10px] ml-2">{desc}</span></>;
    };

    const hoverMA20 = calculateMA(chartData, 20)[hoverIdx];
    const hoverMA60 = calculateMA(chartData, 60)[hoverIdx];
    const hoverMA120 = calculateMA(chartData, 120)[hoverIdx];
    const hoverMA200 = calculateMA(chartData, 200)[hoverIdx];
    const hoverVWAP = vwapData[hoverIdx];
    const hoverVPPoc = vpData.poc[hoverIdx];
    const hoverVPUpper = vpData.upper[hoverIdx];
    const hoverVPLower = vpData.lower[hoverIdx];
    const hoverBBUpper = bollingerData.upper[hoverIdx];
    const hoverBBMid = bollingerData.mid[hoverIdx];
    const hoverBBLower = bollingerData.lower[hoverIdx];
    const hoverVSBCUpper = vsbcData.upper[hoverIdx];
    const hoverVSBCLower = vsbcData.lower[hoverIdx];

    return (
        <div className={`bg-slate-900 min-h-screen p-3 ${isMobileView ? 'flex justify-center' : ''}`}>
            <div className={`w-full transition-all duration-300 ${isMobileView ? 'max-w-[375px]' : ''}`}>

                <div className={`bg-slate-800 rounded px-3 py-2 mb-2 text-sm text-slate-300 flex ${isMobileView ? 'flex-col items-start gap-2' : 'flex-wrap gap-4 items-center'}`}>
                    <div className="relative flex justify-between w-full">
                        <input
                            type="text"
                            placeholder="輸入代號..."
                            className={`bg-slate-700 text-white px-2 py-1 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 ${isMobileView ? 'w-full' : 'w-32'}`}
                            value={searchTerm}
                            onChange={(e) => { setSearchTerm(e.target.value); setShowSuggestions(true); }}
                            onFocus={() => setShowSuggestions(true)}
                            onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
                            onKeyDown={(e) => {
                                if (e.key === 'Enter' && filteredStocks.length > 0) {
                                    setStockInfo(filteredStocks[0]);
                                    setSearchTerm('');
                                    setShowSuggestions(false);
                                }
                            }}
                        />


                        {showSuggestions && filteredStocks.length > 0 && (
                            <div className="absolute top-full left-0 w-full bg-slate-700 border border-slate-600 rounded mt-1 z-50 max-h-60 overflow-y-auto shadow-lg">
                                {filteredStocks.map(s => (
                                    <div
                                        key={s.code}
                                        className="px-3 py-2 hover:bg-slate-600 cursor-pointer text-white"
                                        onClick={() => {
                                            setStockInfo(s);
                                            setSearchTerm('');
                                            setShowSuggestions(false);
                                        }}
                                    >
                                        <span className="font-bold text-yellow-400">{s.code}</span> {s.name}
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    <div className={`flex ${isMobileView ? 'flex-col gap-1 w-full' : 'gap-3 items-center flex-wrap'}`}>
                        <div className="flex justify-between items-center">
                            <span className="text-white font-bold text-lg">{stockInfo.name} ({stockInfo.code})</span>
                            <span className="text-slate-400 text-xs">{hoverData?.time || '-'}</span>
                        </div>

                        <div className={`flex ${isMobileView ? 'justify-between text-xs' : 'gap-3'}`}>
                            <span>收 <b className="text-white">{hoverData?.close?.toFixed(2) || '-'}</b></span>
                            <span className={Number(priceChange) >= 0 ? 'text-red-400' : 'text-green-400'}>
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
                                <span>收 <b className="text-white">{hoverData?.close?.toFixed(2) || '-'}</b><span className={Number(priceChange) >= 0 ? 'text-red-400' : 'text-green-400'}>({Number(priceChange) > 0 ? '+' : ''}{priceChange})</span></span>
                                <span>量 <b className="text-yellow-400">{hoverData ? (hoverData.value / 1000).toFixed(0) : '-'}</b><span className={Number(volChange) >= 0 ? 'text-red-400' : 'text-green-400'}>({Number(volChange) > 0 ? '+' : ''}{hoverData && prevData ? (volChange / 1000).toFixed(0) : '0'})</span></span>
                                {hoverData?.amount > 0 && <span>額 <b className="text-cyan-400">{(hoverData.amount / 100000000).toFixed(2)}億</b></span>}
                            </>
                        )}
                    </div>


                </div>

                <div className="bg-slate-800 rounded p-2 mb-2 flex flex-wrap items-center gap-3">
                    {indicatorConfig.map(ind => {
                        const active = activeIndicators.includes(ind.name);
                        let val = '-';
                        if (ind.name === 'MA20') val = hoverMA20?.toFixed(2);
                        else if (ind.name === 'MA60') val = hoverMA60?.toFixed(2);
                        else if (ind.name === 'MA120') val = hoverMA120?.toFixed(2);
                        else if (ind.name === 'MA200') val = hoverMA200?.toFixed(2);
                        else if (ind.name === 'VWAP') val = hoverVWAP?.toFixed(2);
                        else if (ind.name === 'BBW') val = hoverBBMid ? `${hoverBBLower?.toFixed(0)}/${hoverBBMid?.toFixed(0)}/${hoverBBUpper?.toFixed(0)}` : null;
                        else if (ind.name === 'VP') val = hoverVPPoc ? `${hoverVPLower?.toFixed(0)}/${hoverVPPoc?.toFixed(0)}/${hoverVPUpper?.toFixed(0)}` : null;
                        else if (ind.name === 'VSBC') val = hoverVSBCLower ? `${hoverVSBCLower?.toFixed(0)}/${hoverVSBCUpper?.toFixed(0)}` : null;
                        return (
                            <button key={ind.name} onClick={() => toggleIndicator(ind.name)}
                                className={`flex items-center gap-1.5 text-xs transition-opacity ${active ? 'opacity-100' : 'opacity-40'}`}>
                                <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: ind.color }} />
                                <span className="text-slate-300">{ind.name}</span>
                                {active && val && <span style={{ color: ind.color }}>{val}</span>}
                            </button>
                        );
                    })}
                    <div className="ml-auto flex gap-1">
                        {periods.map(p => (
                            <button key={p} onClick={() => setPeriod(p)} className={`px-3 py-1 text-xs rounded ${period === p ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-400'}`}>{p}</button>
                        ))}
                    </div>
                </div>

                <div ref={mainContainerRef} className="w-full rounded overflow-hidden" />

                <div className="h-2 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center"
                    onMouseDown={handleResizeStart('main-vol')}
                    onTouchStart={handleTouchStart('main-vol')}
                >
                    <div className="w-10 h-1 bg-slate-600 rounded-full"></div>
                </div>

                <div className="mt-1">
                    <div className="flex gap-3 text-xs text-slate-400 mb-1 px-1">
                        <span>成交量</span>
                        <span className="text-blue-400">— MA5: {hoverVolMA5 ? (hoverVolMA5 / 10000).toFixed(0) : '-'}</span>
                        <span className="text-purple-400">— MA60: {hoverVolMA60 ? (hoverVolMA60 / 10000).toFixed(0) : '-'}</span>
                        <span className="text-yellow-400">量: {hoverData ? (hoverData.value / 10000).toFixed(0) : '-'}</span>
                    </div>
                    <div ref={volumeContainerRef} className="w-full rounded overflow-hidden" />
                </div>

                <div className="h-2 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center"
                    onMouseDown={handleResizeStart('vol-sub1')}
                    onTouchStart={handleTouchStart('vol-sub1')}
                >
                    <div className="w-10 h-1 bg-slate-600 rounded-full"></div>
                </div>

                <div className="mt-1">
                    <div className="flex gap-2 mb-1 px-1 items-center">
                        <span className="text-xs text-slate-400">副圖1:</span>
                        <select
                            value={activeSubIndicator}
                            onChange={(e) => setActiveSubIndicator(e.target.value)}
                            className="bg-slate-700 text-white text-xs px-2 py-1 rounded focus:outline-none"
                        >
                            {subIndicators.map(ind => (
                                <option key={ind} value={ind}>{ind}</option>
                            ))}
                        </select>
                        {activeSubIndicator === '大戶' && (
                            <select
                                value={shareholderThreshold}
                                onChange={(e) => setShareholderThreshold(Number(e.target.value))}
                                className="bg-slate-700 text-white text-xs px-2 py-1 rounded focus:outline-none"
                            >
                                <option value={1000}>1000張以上</option>
                                <option value={800}>800張以上</option>
                                <option value={600}>600張以上</option>
                                <option value={400}>400張以上</option>
                            </select>
                        )}
                    </div>
                    <div className="flex gap-3 text-xs text-slate-400 mb-1 px-1 min-h-[1.25rem]">
                        {getSubValues()}
                    </div>
                    <div ref={subChartContainerRef} className="w-full rounded overflow-hidden" />
                </div>

                <div className="h-2 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center"
                    onMouseDown={handleResizeStart('sub1-sub2')}
                    onTouchStart={handleTouchStart('sub1-sub2')}
                >
                    <div className="w-10 h-1 bg-slate-600 rounded-full"></div>
                </div>

                <div className="mt-1">
                    <div className="flex gap-2 mb-1 px-1 items-center">
                        <span className="text-xs text-slate-400">副圖2:</span>
                        <select
                            value={activeSubIndicator2}
                            onChange={(e) => setActiveSubIndicator2(e.target.value)}
                            className="bg-slate-700 text-white text-xs px-2 py-1 rounded focus:outline-none"
                        >
                            {subIndicators.map(ind => (
                                <option key={ind} value={ind}>{ind}</option>
                            ))}
                        </select>
                        {activeSubIndicator2 === '大戶' && (
                            <select
                                value={shareholderThreshold}
                                onChange={(e) => setShareholderThreshold(Number(e.target.value))}
                                className="bg-slate-700 text-white text-xs px-2 py-1 rounded focus:outline-none"
                            >
                                <option value={1000}>1000張以上</option>
                                <option value={800}>800張以上</option>
                                <option value={600}>600張以上</option>
                                <option value={400}>400張以上</option>
                            </select>
                        )}
                    </div>
                    <div className="flex gap-3 text-xs text-slate-400 mb-1 px-1 min-h-[1.25rem]">
                        {getSubValues2()}
                    </div>
                    <div ref={subChartContainerRef2} className="w-full rounded overflow-hidden" />
                </div>

                <div className="h-2 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center"
                    onMouseDown={handleResizeStart('sub2-bottom')}
                    onTouchStart={handleTouchStart('sub2-bottom')}
                >
                    <div className="w-10 h-1 bg-slate-600 rounded-full"></div>
                </div>
            </div>
        </div>
    );
}

export default Dashboard;
