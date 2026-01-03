import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { createChart, CandlestickSeries, LineSeries, HistogramSeries } from 'lightweight-charts';
import { useMobileView } from "@/context/MobileViewContext";
import {
    calculateMA, calculateKD, calculateMACD, calculateRSI, calculateMFI,
    calculateVWAP, calculateBollinger, calculateVP, calculateVSBC,
    calculateADL, calculateNVI, calculatePVI, calculateSMI
} from '@/utils/indicators';
import { getStockHistory } from '@/lib/supabaseClient';

export function TechnicalChart({ code, name, onHoverData, isFullScreen = false }) {
    const { isMobileView } = useMobileView();
    const [period, setPeriod] = useState('日');
    const periods = ['日', '週', '月'];
    const [activeIndicators, setActiveIndicators] = useState(['MA20', 'MA60', 'MA120', 'MA200']);
    const subIndicators = ['KD', 'RSI', 'MACD', 'MFI', 'NVI/PVI', 'SMI/SVI', 'ADL', '外資', '投信', '自營', '集保', '大戶'];
    const [activeSubIndicator, setActiveSubIndicator] = useState('KD');
    const [activeSubIndicator2, setActiveSubIndicator2] = useState('MACD');
    const [rawData, setRawData] = useState([]); // Store raw daily data
    const [hoverIdx, setHoverIdx] = useState(-1);
    const [shareholderThreshold, setShareholderThreshold] = useState(1000);
    const [debugStatus, setDebugStatus] = useState('Init...');

    // Aggregation Logic
    const chartData = useMemo(() => {
        if (rawData.length === 0) return [];
        if (period === '日') return rawData;

        const grouped = [];
        let currentGroup = null;

        const getGroupKey = (d, p) => {
            const date = new Date(d.time);
            if (p === '週') {
                // Get Monday of the week
                const day = date.getDay();
                const diff = date.getDate() - day + (day === 0 ? -6 : 1);
                const monday = new Date(date.setDate(diff));
                return `${monday.getFullYear()}-${String(monday.getMonth() + 1).padStart(2, '0')}-${String(monday.getDate()).padStart(2, '0')}`;
            } else {
                // Month
                return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
            }
        };

        rawData.forEach(d => {
            const key = getGroupKey(d, period);
            if (!currentGroup || currentGroup.key !== key) {
                if (currentGroup) grouped.push(currentGroup.data);
                currentGroup = {
                    key,
                    data: { ...d, volume: 0, amount: 0, foreign: 0, trust: 0, dealer: 0 }
                };
            }

            // Aggregate
            const g = currentGroup.data;
            g.high = Math.max(g.high, d.high);
            g.low = Math.min(g.low, d.low);
            g.close = d.close; // Last close
            g.volume += d.value; // Sum volume (using 'value' field which is volume)
            g.amount += d.amount || 0;
            g.foreign += d.foreign || 0;
            g.trust += d.trust || 0;
            g.dealer += d.dealer || 0;
            g.time = d.time; // Update time to latest date in group
            g.value = g.volume; // Sync value
        });
        if (currentGroup) grouped.push(currentGroup.data);
        return grouped;
    }, [rawData, period]);

    // Fetch Data
    useEffect(() => {
        if (!code) {
            setDebugStatus('No code');
            return;
        }

        const fetchData = async () => {
            setDebugStatus(`Fetching ${code}...`);
            try {
                // Use Supabase client directly for production compatibility
                const historyData = await getStockHistory(code, 500);
                if (historyData && historyData.length > 0) {
                    const count = historyData.length;
                    setDebugStatus(`Data: ${count} recs`);
                    const formatted = historyData.map(d => ({
                        time: d.date_int ? `${String(d.date_int).slice(0, 4)}-${String(d.date_int).slice(4, 6)}-${String(d.date_int).slice(6, 8)}` : '',
                        open: d.open,
                        high: d.high,
                        low: d.low,
                        close: d.close,
                        value: d.volume, // Volume
                        amount: d.amount,
                        tdcc: d.tdcc_count,
                        large: d.large_shareholder_pct,
                        foreign: d.foreign_buy,
                        trust: d.trust_buy,
                        dealer: d.dealer_buy
                    })).sort((a, b) => new Date(a.time) - new Date(b.time));

                    setRawData(formatted); // Set Raw Data
                    if (onHoverData && formatted.length > 0) {
                        onHoverData(formatted[formatted.length - 1], formatted.length > 1 ? formatted[formatted.length - 2] : null);
                    }
                } else {
                    setDebugStatus(`No data for ${code}`);
                }
            } catch (err) {
                console.error("Fetch history failed", err);
                setDebugStatus(`Err: ${err.message}`);
            }
        };

        fetchData();
    }, [code]); // eslint-disable-line react-hooks/exhaustive-deps

    const indicatorConfig = [
        { name: 'MA20', color: '#f97316' },
        { name: 'MA60', color: '#a855f7' },
        { name: 'MA120', color: '#3b82f6' },
        { name: 'MA200', color: '#ef4444' },
        { name: 'VWAP', color: '#eab308' },
        { name: 'BBW', color: '#6366f1' },
        { name: 'VP', color: '#14b8a6' },
        { name: 'VSBC', color: '#ec4899' },
        { name: 'Fib', color: '#84cc16' }
    ];

    // Calculate Volume MAs
    const volumeMA5 = useMemo(() => calculateMA(chartData, 5, 'value'), [chartData]);
    const volumeMA60 = useMemo(() => calculateMA(chartData, 60, 'value'), [chartData]);

    // Calculate Technical Indicators
    const vwapData = useMemo(() => calculateVWAP(chartData), [chartData]);
    const bollingerData = useMemo(() => calculateBollinger(chartData), [chartData]);
    const vpData = useMemo(() => calculateVP(chartData), [chartData]);
    const vsbcData = useMemo(() => calculateVSBC(chartData), [chartData]);
    const kdData = useMemo(() => calculateKD(chartData), [chartData]);
    const macdData = useMemo(() => calculateMACD(chartData), [chartData]);
    const rsi5Data = useMemo(() => calculateRSI(chartData, 5), [chartData]);
    const rsi10Data = useMemo(() => calculateRSI(chartData, 10), [chartData]);
    const mfiData = useMemo(() => calculateMFI(chartData), [chartData]);
    const nviData = useMemo(() => calculateNVI(chartData), [chartData]);
    const pviData = useMemo(() => calculatePVI(chartData), [chartData]);
    const adlData = useMemo(() => calculateADL(chartData), [chartData]);
    const smiData = useMemo(() => calculateSMI(chartData), [chartData]);
    const tdccData = useMemo(() => chartData.map(d => d.tdcc || null), [chartData]);
    const largeData = useMemo(() => chartData.map(d => d.large || null), [chartData]);

    const mainContainerRef = useRef(null);
    const volumeContainerRef = useRef(null);
    const subChartContainerRef = useRef(null);
    const subChartContainerRef2 = useRef(null);
    const chartRefs = useRef({ main: null, volume: null, subChart: null, subChart2: null, subSeries: {}, subSeries2: {}, isDisposed: false });

    const dataRef = useRef([]);

    // Chart Heights - Proportions: Main 62%, Volume 10%, Sub1 14%, Sub2 14%
    const [chartHeights, setChartHeights] = useState({ main: 310, volume: 50, sub1: 70, sub2: 70 });
    const isResizing = useRef(null);
    const lastTouchY = useRef(0);

    // Full Screen Height Calculation
    useEffect(() => {
        const calculateHeights = () => {
            // Get container height or use window height for fullscreen
            let availableHeight;
            if (isFullScreen) {
                const totalHeight = window.innerHeight;
                const headerHeight = 40;
                availableHeight = totalHeight - headerHeight;
            } else {
                // Normal mode - use parent container height or estimate
                const container = mainContainerRef.current?.parentElement;
                availableHeight = container?.clientHeight || 570;
            }

            // Ratios: Main 62%, Vol 10%, Sub1 14%, Sub2 14%
            const mainH = Math.floor(availableHeight * 0.62);
            const volH = Math.floor(availableHeight * 0.10);
            const subH = Math.floor(availableHeight * 0.14);

            setChartHeights({ main: mainH, volume: volH, sub1: subH, sub2: subH });
        };

        calculateHeights();
        window.addEventListener('resize', calculateHeights);

        if (isFullScreen) {
            document.body.style.overflow = 'hidden';
        }

        return () => {
            window.removeEventListener('resize', calculateHeights);
            if (isFullScreen) {
                document.body.style.overflow = '';
            }
        };
    }, [isFullScreen]);

    // Resize Handlers
    const handleResizeStart = useCallback((section) => (e) => {
        e.preventDefault();
        isResizing.current = section;
        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('mouseup', handleMouseUp);
    }, []);

    const handleTouchStart = useCallback((section) => (e) => {
        isResizing.current = section;
        if (e.touches?.[0]) lastTouchY.current = e.touches[0].clientY;
        document.addEventListener('touchmove', handleTouchMove, { passive: false });
        document.addEventListener('touchend', handleTouchEnd);
    }, []);

    const handleMouseMove = useCallback((e) => {
        if (!isResizing.current) return;
        const delta = e.movementY;
        adjustHeights(isResizing.current, delta);
    }, []);

    const handleMouseUp = useCallback(() => {
        isResizing.current = null;
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
    }, [handleMouseMove]);

    const handleTouchMove = useCallback((e) => {
        if (!isResizing.current || !e.touches?.[0]) return;
        e.preventDefault();
        const y = e.touches[0].clientY;
        const delta = y - lastTouchY.current;
        lastTouchY.current = y;
        adjustHeights(isResizing.current, delta);
    }, []);

    const handleTouchEnd = useCallback(() => {
        isResizing.current = null;
        document.removeEventListener('touchmove', handleTouchMove);
        document.removeEventListener('touchend', handleTouchEnd);
    }, [handleTouchMove]);

    const adjustHeights = useCallback((section, delta) => {
        setChartHeights(prev => {
            const minH = 40;
            const n = { ...prev };
            if (section === 'main-vol') {
                n.main = Math.max(minH, prev.main + delta);
                n.volume = Math.max(minH, prev.volume - delta);
            } else if (section === 'vol-sub1') {
                n.volume = Math.max(minH, prev.volume + delta);
                n.sub1 = Math.max(minH, prev.sub1 - delta);
            } else if (section === 'sub1-sub2') {
                n.sub1 = Math.max(minH, prev.sub1 + delta);
                n.sub2 = Math.max(minH, prev.sub2 - delta);
            }
            return n;
        });
    }, []);

    // Toggle Indicator
    const toggleIndicator = useCallback((name) => {
        setActiveIndicators(prev =>
            prev.includes(name) ? prev.filter(x => x !== name) : [...prev, name]
        );
    }, []);

    // ... (resizing logic remains, but maybe restricted)

    // ...

    useEffect(() => {
        if (!mainContainerRef.current || !volumeContainerRef.current || !subChartContainerRef.current || !subChartContainerRef2.current) return;
        if (chartRefs.current.main) return;
        const width = mainContainerRef.current.clientWidth || 800;
        const chartOpts = (h, showTime = false) => ({
            width, height: h,
            layout: { background: { type: 'solid', color: '#0f172a' }, textColor: '#94a3b8' },
            grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
            crosshair: { mode: 0 },
            rightPriceScale: { borderColor: '#334155', scaleMargins: { top: 0.1, bottom: 0.1 }, minimumWidth: 60, width: 60 }, // Consistent scale width
            timeScale: { visible: showTime, borderColor: '#334155', timeVisible: true, rightOffset: 5, fixRightEdge: true },
            handleScale: { axisPressedMouseMove: { time: true, price: false } }, // Disable vertical scaling by user to keep fixed range
            handleScroll: { vertTouchDrag: false, pressedMouseMove: true, horzTouchDrag: true }, // Disable vertical scroll
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

        // Update dataRef for crosshair index lookup
        dataRef.current = chartData;

        try {
            refs.candleSeries.setData(chartData);
            // Volume with color based on price change (up=red, down=green in Taiwan)
            const volumeData = chartData.map(d => ({
                time: d.time,
                value: d.value,
                color: d.close >= d.open ? '#ef4444' : '#22c55e'
            }));
            refs.volumeSeries.setData(volumeData);
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
        try {
            const values = [];
            indicatorConfig.forEach(ind => {
                if (!activeIndicators.includes(ind.name)) return;
                let val = null;
                // Helper for safe formatting
                const fmt = (v, d = 2) => (v !== null && v !== undefined && !isNaN(v)) ? v.toFixed(d) : null;

                if (ind.name === 'MA20') val = fmt(calculateMA(chartData, 20)[hoverIdx]);
                else if (ind.name === 'MA60') val = fmt(calculateMA(chartData, 60)[hoverIdx]);
                else if (ind.name === 'MA120') val = fmt(calculateMA(chartData, 120)[hoverIdx]);
                else if (ind.name === 'MA200') val = fmt(calculateMA(chartData, 200)[hoverIdx]);
                else if (ind.name === 'VWAP') val = fmt(vwapData[hoverIdx]);
                else if (ind.name === 'BBW') {
                    const mid = bollingerData.mid[hoverIdx];
                    const lower = bollingerData.lower[hoverIdx];
                    const upper = bollingerData.upper[hoverIdx];
                    if (mid !== null && lower !== null && upper !== null) {
                        val = `${fmt(lower, 0)}/${fmt(mid, 0)}/${fmt(upper, 0)}`;
                    }
                }
                else if (ind.name === 'VP') {
                    const poc = vpData.poc[hoverIdx];
                    const lower = vpData.lower[hoverIdx];
                    const upper = vpData.upper[hoverIdx];
                    if (poc !== null && lower !== null && upper !== null) {
                        val = `${fmt(lower, 0)}/${fmt(poc, 0)}/${fmt(upper, 0)}`;
                    }
                }
                else if (ind.name === 'VSBC') {
                    const lower = vsbcData.lower[hoverIdx];
                    const upper = vsbcData.upper[hoverIdx];
                    if (lower !== null && upper !== null) {
                        val = `${fmt(lower, 0)}/${fmt(upper, 0)}`;
                    }
                }

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
        } catch (e) {
            console.error('Render overlay error:', e);
            return null;
        }
    };

    return (
        <div className="w-full h-full flex flex-col max-h-screen overflow-hidden">
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
            <div className="relative w-full" style={{ height: chartHeights.main }}>
                {renderIndicatorOverlay()}
                <div ref={mainContainerRef} className="w-full h-full rounded overflow-hidden" />
            </div>

            {/* Resizer */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('main-vol')} onTouchStart={handleTouchStart('main-vol')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>

            {/* Debug Info */}
            <div className="absolute top-0 right-0 bg-black/50 text-white text-xs p-1 z-50 pointer-events-none">
                Debug: {debugStatus}
            </div>

            {/* Volume Section */}
            <div className="mt-0 relative" style={{ height: chartHeights.volume }}>
                <div className="flex gap-2 text-[9px] text-slate-400 px-1 absolute top-0 left-0 z-10 pointer-events-none">
                    <span className="text-blue-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">MA5:{volumeMA5[hoverIdx] ? (volumeMA5[hoverIdx] / 10000).toFixed(0) : '-'}</span>
                    <span className="text-purple-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">MA60:{volumeMA60[hoverIdx] ? (volumeMA60[hoverIdx] / 10000).toFixed(0) : '-'}</span>
                    <span className="text-yellow-400 bg-slate-900/40 px-0.5 rounded backdrop-blur-[1px]">V:{chartData[hoverIdx] ? (chartData[hoverIdx].value / 10000).toFixed(0) : '-'}</span>
                </div>
                <div ref={volumeContainerRef} className="w-full h-full rounded overflow-hidden" />
            </div>

            {/* Resizer */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('vol-sub1')} onTouchStart={handleTouchStart('vol-sub1')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>

            {/* Sub Chart 1 */}
            <div className="mt-0 relative" style={{ height: chartHeights.sub1 }}>
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
                <div ref={subChartContainerRef} className="w-full h-full rounded overflow-hidden" />
            </div>

            {/* Resizer */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('sub1-sub2')} onTouchStart={handleTouchStart('sub1-sub2')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>

            {/* Sub Chart 2 */}
            <div className="mt-0 relative" style={{ height: chartHeights.sub2 }}>
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
                <div ref={subChartContainerRef2} className="w-full h-full rounded overflow-hidden" />
            </div>

            {/* Bottom Resizer (Optional, maybe remove to save space) */}
            <div className="h-1 bg-slate-800 hover:bg-blue-500 cursor-row-resize flex justify-center items-center shrink-0" onMouseDown={handleResizeStart('sub2-bottom')} onTouchStart={handleTouchStart('sub2-bottom')}>
                <div className="w-6 h-0.5 bg-slate-600 rounded-full" />
            </div>
        </div>
    );
}

export default TechnicalChart;
