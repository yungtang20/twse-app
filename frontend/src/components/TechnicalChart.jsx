import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { createChart, CandlestickSeries, LineSeries, HistogramSeries } from 'lightweight-charts';
import { useMobileView } from "@/context/MobileViewContext";
import {
    calculateMA, calculateKD, calculateMACD, calculateRSI, calculateMFI,
    calculateVWAP, calculateBollinger, calculateVP, calculateVSBC,
    calculateADL, calculateNVI, calculatePVI, calculateSMI
} from '@/utils/indicators';
import API_BASE_URL from '@/lib/api';

export function TechnicalChart({ code, name, onHoverData, isFullScreen = false }) {
    const { isMobileView } = useMobileView();
    const [period, setPeriod] = useState('日');
    const periods = ['日', '週', '月'];
    const [activeIndicators, setActiveIndicators] = useState(['MA20', 'MA60', 'MA120', 'MA200']);
    const subIndicators = ['KD', 'RSI', 'MACD', 'MFI', 'NVI/PVI', 'SMI/SVI', 'ADL', '外資', '投信', '自營', '集保', '大戶'];
    const [activeSubIndicator, setActiveSubIndicator] = useState('KD');
    const [activeSubIndicator2, setActiveSubIndicator2] = useState('MACD');
    const [chartData, setChartData] = useState([]);
    const [hoverIdx, setHoverIdx] = useState(-1);
    const [shareholderThreshold, setShareholderThreshold] = useState(1000);

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

    // Full Screen Height Calculation
    useEffect(() => {
        if (isFullScreen) {
            const calculateHeights = () => {
                // Total available height - header (approx 40px) - toolbar (approx 40px)
                // Reduced offset from 120 to 80 because header is now more compact
                const totalHeight = window.innerHeight - 80;
                const mainH = Math.floor(totalHeight * 0.60); // Increased from 0.55
                const volH = Math.floor(totalHeight * 0.12);  // Reduced from 0.15
                const subH = Math.floor(totalHeight * 0.14);  // Reduced from 0.15
                setChartHeights({ main: mainH, volume: volH, sub1: subH, sub2: subH });
            };
            calculateHeights();
            window.addEventListener('resize', calculateHeights);
            return () => window.removeEventListener('resize', calculateHeights);
        } else {
            // Reset to default or previous values if needed, or just leave as is
            setChartHeights({ main: 300, volume: 80, sub1: 120, sub2: 120 });
        }
    }, [isFullScreen]);

    const handleResizeStart = (type) => (e) => {
        if (isFullScreen) return; // Disable manual resize in full screen
        isResizing.current = type;
        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';
    };

    const handleResizeMove = useCallback((e) => {
        if (!isResizing.current) return;
        const deltaY = e.movementY;
        setChartHeights(prev => {
            const newHeights = { ...prev };
            if (isResizing.current === 'main-vol') newHeights.main = Math.max(100, prev.main + deltaY);
            else if (isResizing.current === 'vol-sub1') newHeights.volume = Math.max(50, prev.volume + deltaY);
            else if (isResizing.current === 'sub1-sub2') newHeights.sub1 = Math.max(50, prev.sub1 + deltaY);
            else if (isResizing.current === 'sub2-bottom') newHeights.sub2 = Math.max(50, prev.sub2 + deltaY);
            return newHeights;
        });
    }, []);

    const handleResizeEnd = useCallback(() => {
        isResizing.current = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    }, []);

    const handleTouchStart = (type) => (e) => {
        if (isFullScreen) return; // Disable manual resize in full screen
        isResizing.current = type;
        lastTouchY.current = e.touches[0].clientY;
        document.body.style.overflow = 'hidden';
    };

    const handleTouchMove = useCallback((e) => {
        if (!isResizing.current) return;
        const currentY = e.touches[0].clientY;
        const deltaY = currentY - lastTouchY.current;
        lastTouchY.current = currentY;
        setChartHeights(prev => {
            const newHeights = { ...prev };
            if (isResizing.current === 'main-vol') newHeights.main = Math.max(100, prev.main + deltaY);
            else if (isResizing.current === 'vol-sub1') newHeights.volume = Math.max(50, prev.volume + deltaY);
            else if (isResizing.current === 'sub1-sub2') newHeights.sub1 = Math.max(50, prev.sub1 + deltaY);
            else if (isResizing.current === 'sub2-bottom') newHeights.sub2 = Math.max(50, prev.sub2 + deltaY);
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

    const toggleIndicator = (name) => {
        setActiveIndicators(prev => {
            if (prev.includes(name)) return prev.filter(i => i !== name);
            return [...prev, name];
        });
    };

    useEffect(() => {
        if (!code) return;
        const fetchData = async () => {
            try {
                const res = await fetch(`${API_BASE_URL}/api/stocks/${code}/history?limit=2000`);
                const json = await res.json();
                const shRes = await fetch(`${API_BASE_URL}/api/stocks/${code}/shareholding?threshold=${shareholderThreshold}`);
                const shJson = await shRes.json();
                const totalHoldersMap = new Map();
                const largeHoldersMap = new Map();
                if (shJson.success) {
                    if (shJson.data.total_holders) shJson.data.total_holders.forEach(item => totalHoldersMap.set(item.date_int, item.total_holders));
                    if (shJson.data.large_holders) shJson.data.large_holders.forEach(item => largeHoldersMap.set(item.date_int, item.proportion));
                }
                if (json.success) {
                    const formatted = json.data.history.map(item => {
                        const dateStr = String(item.date_int);
                        return {
                            time: `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`,
                            open: Number(item.open), high: Number(item.high), low: Number(item.low), close: Number(item.close),
                            value: Number(item.volume), amount: Number(item.amount || 0),
                            foreign: Number(item.foreign_buy || 0), trust: Number(item.trust_buy || 0), dealer: Number(item.dealer_buy || 0),
                            tdcc: totalHoldersMap.get(item.date_int) || Number(item.tdcc_count || 0),
                            large: largeHoldersMap.get(item.date_int) || Number(item.large_shareholder_pct || 0),
                            color: Number(item.close) >= Number(item.open) ? '#ef4444' : '#22c55e'
                        };
                    });
                    formatted.sort((a, b) => new Date(a.time) - new Date(b.time));
                    setRawData(formatted);
                }
            } catch (err) { console.error('Fetch error:', err); }
        };
        fetchData();
    }, [code, shareholderThreshold]);

    useEffect(() => {
        if (rawData.length === 0) return;
        let processed = rawData;
        if (period === '週') {
            const map = new Map();
            rawData.forEach(d => {
                const date = new Date(d.time);
                date.setDate(date.getDate() - date.getDay());
                const key = date.toISOString().slice(0, 10);
                if (!map.has(key)) map.set(key, { ...d, time: key });
                else { const w = map.get(key); w.high = Math.max(w.high, d.high); w.low = Math.min(w.low, d.low); w.close = d.close; w.value += d.value; w.amount += d.amount; w.foreign += d.foreign; w.trust += d.trust; w.dealer += d.dealer; w.tdcc = d.tdcc; w.large = d.large; }
            });
            processed = Array.from(map.values()).map(d => ({ ...d, color: d.close >= d.open ? '#ef4444' : '#22c55e' }));
        } else if (period === '月') {
            const map = new Map();
            rawData.forEach(d => {
                const key = d.time.slice(0, 7) + '-01';
                if (!map.has(key)) map.set(key, { ...d, time: key });
                else { const m = map.get(key); m.high = Math.max(m.high, d.high); m.low = Math.min(m.low, d.low); m.close = d.close; m.value += d.value; m.amount += d.amount; m.foreign += d.foreign; m.trust += d.trust; m.dealer += d.dealer; m.tdcc = d.tdcc; m.large = d.large; }
            });
            processed = Array.from(map.values()).map(d => ({ ...d, color: d.close >= d.open ? '#ef4444' : '#22c55e' }));
        }
        setChartData(processed);
        dataRef.current = processed;
        setHoverIdx(processed.length - 1);
    }, [rawData, period]);

    const volumeMA5 = useMemo(() => calculateMA(chartData, 5, 'value'), [chartData]);
    const volumeMA60 = useMemo(() => calculateMA(chartData, 60, 'value'), [chartData]);
    const kdData = useMemo(() => calculateKD(chartData), [chartData]);
    const macdData = useMemo(() => calculateMACD(chartData), [chartData]);
    const rsi5Data = useMemo(() => calculateRSI(chartData, 5), [chartData]);
    const rsi10Data = useMemo(() => calculateRSI(chartData, 10), [chartData]);
    const mfiData = useMemo(() => calculateMFI(chartData), [chartData]);
    const vwapData = useMemo(() => calculateVWAP(chartData), [chartData]);
    const bollingerData = useMemo(() => calculateBollinger(chartData), [chartData]);
    const vpData = useMemo(() => calculateVP(chartData), [chartData]);
    const vsbcData = useMemo(() => calculateVSBC(chartData), [chartData]);
    const adlData = useMemo(() => calculateADL(chartData), [chartData]);
    const nviData = useMemo(() => calculateNVI(chartData), [chartData]);
    const pviData = useMemo(() => calculatePVI(chartData), [chartData]);
    const smiData = useMemo(() => calculateSMI(chartData), [chartData]);
    const tdccData = useMemo(() => chartData.map(d => d.tdcc), [chartData]);
    const largeData = useMemo(() => chartData.map(d => d.large), [chartData]);

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
        const volumeFormatter = (val) => val >= 100000000 ? (val / 100000000).toFixed(1) + '億' : val >= 10000 ? (val / 10000).toFixed(0) + '萬' : val.toFixed(0);
        const volumeChart = createChart(volumeContainerRef.current, { ...chartOpts(chartHeights.volume), localization: { priceFormatter: volumeFormatter } });
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
        allCharts.forEach(src => {
            src.timeScale().subscribeVisibleLogicalRangeChange(r => {
                if (r && !chartRefs.current.isDisposed) allCharts.forEach(c => { if (c !== src) try { c.timeScale().setVisibleLogicalRange(r); } catch { } });
            });
        });
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

    useEffect(() => {
        const refs = chartRefs.current;
        if (refs.main) refs.main.applyOptions({ height: chartHeights.main });
        if (refs.volume) refs.volume.applyOptions({ height: chartHeights.volume });
        if (refs.subChart) refs.subChart.applyOptions({ height: chartHeights.sub1 });
        if (refs.subChart2) refs.subChart2.applyOptions({ height: chartHeights.sub2 });
    }, [chartHeights]);

    useEffect(() => {
        const refs = chartRefs.current;
        if (!refs.main || chartData.length === 0) return;
        try {
            refs.candleSeries.setData(chartData);
            refs.volumeSeries.setData(chartData);
            refs.volMA5Series.setData(volumeMA5.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            refs.volMA60Series.setData(volumeMA60.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            setTimeout(() => {
                try {
                    const total = chartData.length;
                    const range = 60;
                    refs.main.timeScale().setVisibleLogicalRange({ from: total - range, to: total });
                } catch (e) { }
            }, 300);
        } catch (e) { }
    }, [chartData, volumeMA5, volumeMA60]);

    useEffect(() => {
        const refs = chartRefs.current;
        if (!refs.main || chartData.length === 0) return;
        const colors = { MA20: '#f97316', MA60: '#a855f7', MA120: '#3b82f6', MA200: '#ef4444', VWAP: '#eab308', BBW: '#6366f1', VP: '#14b8a6', VSBC: '#ec4899', Fib: '#84cc16' };
        const periods = { MA20: 20, MA60: 60, MA120: 120, MA200: 200 };
        Object.keys(refs.indicatorSeries).forEach(k => {
            const baseKey = k.split('_')[0];
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
                    const key = `BBW_${type}`, lineColors = ['#6366f1', '#8b5cf6', '#6366f1'], styles = [2, 0, 2];
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: lineColors[idx], lineWidth: 1, lineStyle: styles[idx], priceLineVisible: false, lastValueVisible: false });
                    refs.indicatorSeries[key].setData(bollingerData[type].map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
                });
            } else if (ind === 'VP') {
                ['poc', 'upper', 'lower'].forEach((type, idx) => {
                    const key = `VP_${type}`, lineColors = ['#14b8a6', '#10b981', '#059669'];
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: lineColors[idx], lineWidth: type === 'poc' ? 2 : 1, lineStyle: type === 'poc' ? 0 : 2, priceLineVisible: false, lastValueVisible: false });
                    refs.indicatorSeries[key].setData(vpData[type].map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
                });
            } else if (ind === 'VSBC') {
                ['upper', 'lower'].forEach((type, idx) => {
                    const key = `VSBC_${type}`, lineColors = ['#ec4899', '#f472b6'];
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: lineColors[idx], lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false });
                    refs.indicatorSeries[key].setData(vsbcData[type].map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
                });
            } else if (ind === 'Fib') {
                const maxHigh = Math.max(...chartData.map(d => d.high)), minLow = Math.min(...chartData.map(d => d.low)), range = maxHigh - minLow;
                const fibLevels = [0.236, 0.382, 0.5, 0.618, 0.786], fibColors = ['#84cc16', '#22c55e', '#eab308', '#f97316', '#ef4444'];
                fibLevels.forEach((level, idx) => {
                    const key = `Fib${Math.round(level * 1000)}`, fibValue = minLow + range * level;
                    if (!refs.indicatorSeries[key]) refs.indicatorSeries[key] = refs.main.addSeries(LineSeries, { color: fibColors[idx], lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: true, title: `${(level * 100).toFixed(1)}%` });
                    refs.indicatorSeries[key].setData(chartData.map(d => ({ time: d.time, value: fibValue })));
                });
            }
        });
    }, [activeIndicators, chartData, vwapData, bollingerData, vpData, vsbcData]);

    const updateSubChart = (chart, seriesRef, indicator, dataMap) => {
        if (!chart || chartData.length === 0) return;
        Object.values(seriesRef).forEach(s => { try { chart.removeSeries(s); } catch { } });
        for (const key in seriesRef) delete seriesRef[key];
        const lineOpts = { lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false };
        const histOpts = { priceFormat: { type: 'volume' }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false };
        if (indicator === 'KD') {
            const kS = chart.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' }), dS = chart.addSeries(LineSeries, { ...lineOpts, color: '#f97316' });
            kS.setData(dataMap.kd.k.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            dS.setData(dataMap.kd.d.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            seriesRef.k = kS; seriesRef.d = dS;
        } else if (indicator === 'MACD') {
            const difS = chart.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' }), macdS = chart.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' }), histS = chart.addSeries(HistogramSeries, { priceFormat: { type: 'price', precision: 2 }, priceScaleId: '', priceLineVisible: false, lastValueVisible: false });
            difS.setData(dataMap.macd.dif.map((v, i) => ({ time: chartData[i].time, value: v }))); macdS.setData(dataMap.macd.macd.map((v, i) => ({ time: chartData[i].time, value: v }))); histS.setData(dataMap.macd.osc.map((v, i) => ({ time: chartData[i].time, value: v, color: v > 0 ? '#ef4444' : v < 0 ? '#22c55e' : '#94a3b8' })));
            seriesRef.dif = difS; seriesRef.macd = macdS; seriesRef.hist = histS;
        } else if (indicator === 'RSI') {
            const r5S = chart.addSeries(LineSeries, { ...lineOpts, color: '#3b82f6' }), r10S = chart.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            r5S.setData(dataMap.rsi5.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean)); r10S.setData(dataMap.rsi10.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            seriesRef.r5 = r5S; seriesRef.r10 = r10S;
        } else if (indicator === 'MFI') {
            const mfiS = chart.addSeries(LineSeries, { ...lineOpts, color: '#eab308' });
            mfiS.setData(dataMap.mfi.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            seriesRef.mfi = mfiS;
        } else if (indicator === 'NVI/PVI') {
            const nviS = chart.addSeries(LineSeries, { ...lineOpts, color: '#22c55e' }), pviS = chart.addSeries(LineSeries, { ...lineOpts, color: '#ef4444' });
            nviS.setData(dataMap.nvi.map((v, i) => ({ time: chartData[i].time, value: v }))); pviS.setData(dataMap.pvi.map((v, i) => ({ time: chartData[i].time, value: v })));
            seriesRef.nvi = nviS; seriesRef.pvi = pviS;
        } else if (indicator === 'SMI/SVI') {
            const smiS = chart.addSeries(LineSeries, { ...lineOpts, color: '#06b6d4' }), sviS = chart.addSeries(LineSeries, { ...lineOpts, color: '#f59e0b' });
            smiS.setData(dataMap.smi.map((v, i) => v !== null ? { time: chartData[i].time, value: v } : null).filter(Boolean)); sviS.setData(dataMap.smi.map((v, i) => v !== null ? { time: chartData[i].time, value: v * 0.8 } : null).filter(Boolean));
            seriesRef.smi = smiS; seriesRef.svi = sviS;
        } else if (['外資', '投信', '自營'].includes(indicator)) {
            const key = { '外資': 'foreign', '投信': 'trust', '自營': 'dealer' }[indicator], series = chart.addSeries(HistogramSeries, { ...histOpts });
            series.setData(chartData.map(d => ({ time: d.time, value: d[key], color: d[key] > 0 ? '#ef4444' : d[key] < 0 ? '#22c55e' : '#94a3b8' })));
            seriesRef[key] = series;
        } else if (indicator === '集保') {
            const tdccS = chart.addSeries(LineSeries, { ...lineOpts, color: '#8b5cf6' });
            tdccS.setData(dataMap.tdcc.map((v, i) => v ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            seriesRef.tdcc = tdccS;
        } else if (indicator === '大戶') {
            const largeS = chart.addSeries(LineSeries, { ...lineOpts, color: '#ec4899' });
            largeS.setData(dataMap.large.map((v, i) => v ? { time: chartData[i].time, value: v } : null).filter(Boolean));
            seriesRef.large = largeS;
        }
    };

    useEffect(() => {
        const refs = chartRefs.current, dataMap = { kd: kdData, macd: macdData, rsi5: rsi5Data, rsi10: rsi10Data, mfi: mfiData, nvi: nviData, pvi: pviData, smi: smiData, adl: adlData, tdcc: tdccData, large: largeData };
        updateSubChart(refs.subChart, refs.subSeries, activeSubIndicator, dataMap);
    }, [activeSubIndicator, chartData, kdData, macdData, rsi5Data, rsi10Data, mfiData, nviData, pviData, adlData, smiData, tdccData, largeData]);

    useEffect(() => {
        const refs = chartRefs.current, dataMap = { kd: kdData, macd: macdData, rsi5: rsi5Data, rsi10: rsi10Data, mfi: mfiData, nvi: nviData, pvi: pviData, smi: smiData, adl: adlData, tdcc: tdccData, large: largeData };
        updateSubChart(refs.subChart2, refs.subSeries2, activeSubIndicator2, dataMap);
    }, [activeSubIndicator2, chartData, kdData, macdData, rsi5Data, rsi10Data, mfiData, nviData, pviData, adlData, smiData, tdccData, largeData]);

    useEffect(() => { if (onHoverData) onHoverData(chartData[hoverIdx], chartData[hoverIdx - 1], { volumeMA5: volumeMA5[hoverIdx], volumeMA60: volumeMA60[hoverIdx], kd: { k: kdData.k[hoverIdx], d: kdData.d[hoverIdx] }, macd: { dif: macdData.dif[hoverIdx], macd: macdData.macd[hoverIdx], osc: macdData.osc[hoverIdx] }, rsi: { r5: rsi5Data[hoverIdx], r10: rsi10Data[hoverIdx] }, mfi: mfiData[hoverIdx], nvi: nviData[hoverIdx], pvi: pviData[hoverIdx], adl: adlData[hoverIdx], smi: smiData[hoverIdx], ma: { ma20: calculateMA(chartData, 20)[hoverIdx], ma60: calculateMA(chartData, 60)[hoverIdx], ma120: calculateMA(chartData, 120)[hoverIdx], ma200: calculateMA(chartData, 200)[hoverIdx] }, vwap: vwapData[hoverIdx], vp: { poc: vpData.poc[hoverIdx], upper: vpData.upper[hoverIdx], lower: vpData.lower[hoverIdx] }, bb: { upper: bollingerData.upper[hoverIdx], mid: bollingerData.mid[hoverIdx], lower: bollingerData.lower[hoverIdx] }, vsbc: { upper: vsbcData.upper[hoverIdx], lower: vsbcData.lower[hoverIdx] } }); }, [hoverIdx, chartData, onHoverData]);

    const getSubValues = (active, subRef) => {
        const h = chartData[hoverIdx];
        if (!h) return null;
        switch (active) {
            case 'KD': return <><span className="text-blue-400">K(9): {kdData.k[hoverIdx]?.toFixed(2)}</span> <span className="text-orange-400">D(9): {kdData.d[hoverIdx]?.toFixed(2)}</span></>;
            case 'MACD': return <><span className="text-blue-400">DIF: {macdData.dif[hoverIdx]?.toFixed(2)}</span> <span className="text-red-400">MACD: {macdData.macd[hoverIdx]?.toFixed(2)}</span> <span className={macdData.osc[hoverIdx] > 0 ? 'text-red-400' : macdData.osc[hoverIdx] < 0 ? 'text-green-400' : 'text-slate-400'}>OSC: {macdData.osc[hoverIdx]?.toFixed(2)}</span></>;
            case 'RSI': return <><span className="text-blue-400">RSI(5): {rsi5Data[hoverIdx]?.toFixed(2)}</span> <span className="text-red-400">RSI(10): {rsi10Data[hoverIdx]?.toFixed(2)}</span></>;
            case 'MFI': return <span className="text-yellow-400">MFI(14): {mfiData[hoverIdx]?.toFixed(2)}</span>;
            case 'NVI/PVI': return <><span className="text-green-400">NVI: {nviData[hoverIdx]?.toFixed(0)}</span> <span className="text-red-400">PVI: {pviData[hoverIdx]?.toFixed(0)}</span></>;
            case 'SMI/SVI': return <><span className="text-cyan-400">SMI: {smiData[hoverIdx]?.toFixed(2)}</span> <span className="text-amber-400">SVI: {(smiData[hoverIdx] * 0.8).toFixed(2)}</span></>;
            case 'ADL': return <span className="text-purple-400">ADL: {(adlData[hoverIdx] / 1e9).toFixed(2)}B</span>;
            case '外資': return <span className={h.foreign > 0 ? 'text-red-400' : h.foreign < 0 ? 'text-green-400' : 'text-slate-400'}>外資: {(h.foreign / 1e8).toFixed(2)}億</span>;
            case '投信': return <span className={h.trust > 0 ? 'text-red-400' : h.trust < 0 ? 'text-green-400' : 'text-slate-400'}>投信: {(h.trust / 1e8).toFixed(2)}億</span>;
            case '自營': return <span className={h.dealer > 0 ? 'text-red-400' : h.dealer < 0 ? 'text-green-400' : 'text-slate-400'}>自營: {(h.dealer / 1e8).toFixed(2)}億</span>;
            case '集保': return <span className="text-violet-400">集保戶數: {h.tdcc}</span>;
            case '大戶': return <span className="text-pink-400">大戶持股: {h.large?.toFixed(2)}%</span>;
            default: return null;
        }
    };

    // Helper to render indicator values overlay
    const renderIndicatorOverlay = () => {
        const values = [];
        indicatorConfig.forEach(ind => {
            if (!activeIndicators.includes(ind.name)) return;
            let val = null;
            if (ind.name === 'MA20') val = calculateMA(chartData, 20)[hoverIdx]?.toFixed(2);
            else if (ind.name === 'MA60') val = calculateMA(chartData, 60)[hoverIdx]?.toFixed(2);
            else if (ind.name === 'MA120') val = calculateMA(chartData, 120)[hoverIdx]?.toFixed(2);
            else if (ind.name === 'MA200') val = calculateMA(chartData, 200)[hoverIdx]?.toFixed(2);
            else if (ind.name === 'VWAP') val = vwapData[hoverIdx]?.toFixed(2);
            else if (ind.name === 'BBW') val = bollingerData.mid[hoverIdx] ? `${bollingerData.lower[hoverIdx]?.toFixed(0)}/${bollingerData.mid[hoverIdx]?.toFixed(0)}/${bollingerData.upper[hoverIdx]?.toFixed(0)}` : null;
            else if (ind.name === 'VP') val = vpData.poc[hoverIdx] ? `${vpData.lower[hoverIdx]?.toFixed(0)}/${vpData.poc[hoverIdx]?.toFixed(0)}/${vpData.upper[hoverIdx]?.toFixed(0)}` : null;
            else if (ind.name === 'VSBC') val = vsbcData.lower[hoverIdx] ? `${vsbcData.lower[hoverIdx]?.toFixed(0)}/${vsbcData.upper[hoverIdx]?.toFixed(0)}` : null;

            if (val) {
                values.push(
                    <div key={ind.name} className="flex items-center gap-1 mr-3">
                        <span className="w-2 h-2 rounded-full" style={{ backgroundColor: ind.color }} />
                        <span style={{ color: ind.color }}>{ind.name} {val}</span>
                    </div>
                );
            }
        });
        return (
            <div className="absolute top-2 left-2 z-10 flex flex-wrap pointer-events-none text-[10px] font-mono bg-slate-900/50 p-1 rounded backdrop-blur-sm">
                {values}
            </div>
        );
    };

    return (
        <div className="w-full h-full flex flex-col">
            {/* Header: Scrollable Toggles */}
            <div className="bg-slate-800 rounded p-0.5 mb-0.5 flex items-center justify-between shrink-0">
                <div className="flex overflow-x-auto no-scrollbar gap-1 px-1 items-center flex-1 mask-linear-fade">
                    {indicatorConfig.map(ind => {
                        const active = activeIndicators.includes(ind.name);
                        return (
                            <button
                                key={ind.name}
                                onClick={() => toggleIndicator(ind.name)}
                                className={`flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] whitespace-nowrap transition-all ${active ? 'bg-slate-700 text-white shadow-sm' : 'text-slate-500 hover:text-slate-400'}`}
                            >
                                <span className="w-1 h-1 rounded-full" style={{ backgroundColor: ind.color }} />
                                <span>{ind.name}</span>
                            </button>
                        );
                    })}
                </div>
                <div className="flex gap-0.5 ml-1 shrink-0">
                    {periods.map(p => (
                        <button
                            key={p}
                            onClick={() => setPeriod(p)}
                            className={`px-1.5 py-0.5 text-[9px] rounded font-medium ${period === p ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-500'}`}
                        >
                            {p}
                        </button>
                    ))}
                </div>
            </div>

            {/* Main Chart with Overlay */}
            <div className="relative w-full">
                {renderIndicatorOverlay()}
                <div ref={mainContainerRef} className="w-full rounded overflow-hidden" />
            </div>

            {/* Resizer */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('main-vol')} onTouchStart={handleTouchStart('main-vol')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>

            {/* Volume Section */}
            <div className="mt-0 relative">
                <div className="flex gap-2 text-[9px] text-slate-400 px-1 absolute top-0 left-0 z-10 pointer-events-none">
                    <span className="text-blue-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">MA5:{volumeMA5[hoverIdx] ? (volumeMA5[hoverIdx] / 10000).toFixed(0) : '-'}</span>
                    <span className="text-purple-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">MA60:{volumeMA60[hoverIdx] ? (volumeMA60[hoverIdx] / 10000).toFixed(0) : '-'}</span>
                    <span className="text-yellow-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">V:{chartData[hoverIdx] ? (chartData[hoverIdx].value / 10000).toFixed(0) : '-'}</span>
                </div>
                <div ref={volumeContainerRef} className="w-full rounded overflow-hidden" />
            </div>

            {/* Resizer */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('vol-sub1')} onTouchStart={handleTouchStart('vol-sub1')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>

            {/* Sub Chart 1 */}
            <div className="mt-0 relative">
                <div className="flex gap-1 px-1 items-center absolute top-0 left-0 z-10">
                    <select value={activeSubIndicator} onChange={(e) => setActiveSubIndicator(e.target.value)} className="bg-slate-800/60 text-white text-[9px] px-0.5 py-0 rounded border border-slate-700/50 focus:outline-none backdrop-blur-[1px]">
                        {subIndicators.map(ind => <option key={ind} value={ind}>{ind}</option>)}
                    </select>
                    {activeSubIndicator === '大戶' && (
                        <select value={shareholderThreshold} onChange={(e) => setShareholderThreshold(Number(e.target.value))} className="bg-slate-800/60 text-white text-[9px] px-0.5 py-0 rounded border border-slate-700/50 focus:outline-none backdrop-blur-[1px]">
                            <option value={1000}>1000+</option><option value={800}>800+</option><option value={600}>600+</option><option value={400}>400+</option>
                        </select>
                    )}
                    <div className="flex gap-1 text-[9px] text-slate-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">
                        {getSubValues(activeSubIndicator, chartRefs.current.subSeries)}
                    </div>
                </div>
                <div ref={subChartContainerRef} className="w-full rounded overflow-hidden" />
            </div>

            {/* Resizer */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('sub1-sub2')} onTouchStart={handleTouchStart('sub1-sub2')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>

            {/* Sub Chart 2 */}
            <div className="mt-0 relative">
                <div className="flex gap-1 px-1 items-center absolute top-0 left-0 z-10">
                    <select value={activeSubIndicator2} onChange={(e) => setActiveSubIndicator2(e.target.value)} className="bg-slate-800/60 text-white text-[9px] px-0.5 py-0 rounded border border-slate-700/50 focus:outline-none backdrop-blur-[1px]">
                        {subIndicators.map(ind => <option key={ind} value={ind}>{ind}</option>)}
                    </select>
                    {activeSubIndicator2 === '大戶' && (
                        <select value={shareholderThreshold} onChange={(e) => setShareholderThreshold(Number(e.target.value))} className="bg-slate-800/60 text-white text-[9px] px-0.5 py-0 rounded border border-slate-700/50 focus:outline-none backdrop-blur-[1px]">
                            <option value={1000}>1000+</option><option value={800}>800+</option><option value={600}>600+</option><option value={400}>400+</option>
                        </select>
                    )}
                    <div className="flex gap-1 text-[9px] text-slate-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">
                        {getSubValues(activeSubIndicator2, chartRefs.current.subSeries2)}
                    </div>
                </div>
                <div ref={subChartContainerRef2} className="w-full rounded overflow-hidden" />
            </div>

            {/* Bottom Resizer (Optional, maybe remove to save space) */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('sub2-bottom')} onTouchStart={handleTouchStart('sub2-bottom')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>
        </div>
    );
}

export default TechnicalChart;
