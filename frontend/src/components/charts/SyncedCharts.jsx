import React, { useEffect, useRef, useState } from 'react';
import { createChart, CandlestickSeries, LineSeries, HistogramSeries } from 'lightweight-charts';

export function SyncedCharts({ data, activeIndicators = [], height = 400, volumeHeight = 80 }) {
    const mainChartRef = useRef(null);
    const volumeChartRef = useRef(null);
    const mainContainerRef = useRef(null);
    const volumeContainerRef = useRef(null);
    const [hoverData, setHoverData] = useState(null);

    useEffect(() => {
        if (!mainContainerRef.current || !volumeContainerRef.current) return;

        let mainRafId = null;
        let volumeRafId = null;

        const initCharts = () => {
            if (!mainContainerRef.current || !volumeContainerRef.current) return;
            const width = mainContainerRef.current.clientWidth || 800;

            // ========== 主圖 (K線) ==========
            const mainChart = createChart(mainContainerRef.current, {
                width: width,
                height: height,
                layout: {
                    background: { type: 'solid', color: 'transparent' },
                    textColor: '#94a3b8',
                },
                grid: {
                    vertLines: { color: '#1e293b' },
                    horzLines: { color: '#1e293b' },
                },
                crosshair: {
                    mode: 1,
                    vertLine: { labelVisible: true },
                    horzLine: { labelVisible: true },
                },
                rightPriceScale: { borderColor: '#334155' },
                timeScale: { borderColor: '#334155', timeVisible: true },
            });

            const candlestickSeries = mainChart.addSeries(CandlestickSeries, {
                upColor: '#ef4444',
                downColor: '#22c55e',
                borderUpColor: '#ef4444',
                borderDownColor: '#22c55e',
                wickUpColor: '#ef4444',
                wickDownColor: '#22c55e',
            });

            // ========== 成交量圖 ==========
            const volumeChart = createChart(volumeContainerRef.current, {
                width: width,
                height: volumeHeight,
                layout: {
                    background: { type: 'solid', color: 'transparent' },
                    textColor: '#9ca3af',
                },
                grid: {
                    vertLines: { visible: false },
                    horzLines: { color: '#1e293b' },
                },
                crosshair: {
                    mode: 1,
                    vertLine: { labelVisible: false },
                    horzLine: { labelVisible: true },
                },
                rightPriceScale: { borderColor: '#334155' },
                timeScale: { visible: false },
            });

            const volumeSeries = volumeChart.addSeries(HistogramSeries, {
                priceFormat: { type: 'volume' },
                priceScaleId: '',
            });

            // ========== 設定數據 ==========
            const sampleData = [
                { time: '2024-12-01', open: 17500, high: 17650, low: 17450, close: 17600 },
                { time: '2024-12-02', open: 17600, high: 17750, low: 17550, close: 17700 },
                { time: '2024-12-03', open: 17700, high: 17800, low: 17600, close: 17650 },
                { time: '2024-12-04', open: 17650, high: 17900, low: 17600, close: 17850 },
                { time: '2024-12-05', open: 17850, high: 17950, low: 17750, close: 17800 },
            ];

            const sampleVolume = [
                { time: '2024-12-01', value: 3200, color: '#ef4444' },
                { time: '2024-12-02', value: 3500, color: '#ef4444' },
                { time: '2024-12-03', value: 2800, color: '#22c55e' },
                { time: '2024-12-04', value: 4100, color: '#ef4444' },
                { time: '2024-12-05', value: 3600, color: '#22c55e' },
            ];

            const hasData = Array.isArray(data) && data.length > 0;
            const source = hasData ? data : sampleData;
            const volumeSource = hasData ? data : sampleVolume;

            candlestickSeries.setData(source);
            volumeSeries.setData(volumeSource);

            // ========== 指標線 ==========
            const colors = {
                MA5: '#3b82f6',
                MA20: '#f97316',
                MA60: '#a855f7',
                MA200: '#ef4444',
                VWAP: '#eab308',
            };

            const indicatorSeries = {};
            activeIndicators.forEach(ind => {
                if (['MA5', 'MA20', 'MA60', 'MA200', 'VWAP'].includes(ind)) {
                    const lineSeries = mainChart.addSeries(LineSeries, {
                        color: colors[ind],
                        lineWidth: 2,
                        title: ind,
                    });
                    const lineData = source.map((d) => ({
                        time: d.time,
                        value: Number(d.close) * (1 + (Math.random() * 0.02 - 0.01))
                    }));
                    lineSeries.setData(lineData);
                    indicatorSeries[ind] = lineSeries;
                }
            });

            // ========== 十字線同步 ==========
            mainChart.subscribeCrosshairMove((param) => {
                if (!param || !param.time) {
                    setHoverData(null);
                    volumeChart.setCrosshairPosition(NaN, null, volumeSeries);
                    return;
                }

                // 同步成交量圖的十字線
                const volumeData = volumeSeries.dataByIndex(
                    volumeSeries.data().findIndex(d => d.time === param.time)
                );
                if (volumeData) {
                    volumeChart.setCrosshairPosition(volumeData.value, param.time, volumeSeries);
                }

                // 取得當日數據
                const idx = source.findIndex(d => d.time === param.time);
                if (idx >= 0) {
                    const d = source[idx];
                    const indicators = {};
                    Object.keys(indicatorSeries).forEach(key => {
                        const seriesData = indicatorSeries[key].dataByIndex(idx);
                        if (seriesData) {
                            indicators[key] = seriesData.value?.toFixed(2);
                        }
                    });

                    setHoverData({
                        time: d.time,
                        open: d.open,
                        high: d.high,
                        low: d.low,
                        close: d.close,
                        volume: d.value || volumeSource[idx]?.value,
                        ...indicators
                    });
                }
            });

            volumeChart.subscribeCrosshairMove((param) => {
                if (!param || !param.time) {
                    mainChart.setCrosshairPosition(NaN, null, candlestickSeries);
                    return;
                }
                const candleData = candlestickSeries.dataByIndex(
                    candlestickSeries.data().findIndex(d => d.time === param.time)
                );
                if (candleData) {
                    mainChart.setCrosshairPosition(candleData.close, param.time, candlestickSeries);
                }
            });

            // ========== 時間軸同步 ==========
            mainChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
                volumeChart.timeScale().setVisibleLogicalRange(range);
            });
            volumeChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
                mainChart.timeScale().setVisibleLogicalRange(range);
            });

            mainChart.timeScale().fitContent();
            volumeChart.timeScale().fitContent();

            mainChartRef.current = mainChart;
            volumeChartRef.current = volumeChart;
        };

        mainRafId = requestAnimationFrame(initCharts);

        const handleResize = () => {
            if (mainChartRef.current && mainContainerRef.current) {
                mainChartRef.current.applyOptions({ width: mainContainerRef.current.clientWidth });
            }
            if (volumeChartRef.current && volumeContainerRef.current) {
                volumeChartRef.current.applyOptions({ width: volumeContainerRef.current.clientWidth });
            }
        };
        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
            if (mainRafId) cancelAnimationFrame(mainRafId);
            if (volumeRafId) cancelAnimationFrame(volumeRafId);
            if (mainChartRef.current) {
                mainChartRef.current.remove();
                mainChartRef.current = null;
            }
            if (volumeChartRef.current) {
                volumeChartRef.current.remove();
                volumeChartRef.current = null;
            }
        };
    }, [data, height, volumeHeight, activeIndicators]);

    return (
        <div>
            {/* 數據懸浮顯示 */}
            {hoverData && (
                <div className="flex flex-wrap gap-4 text-sm text-slate-300 mb-2 p-2 bg-slate-800/80 rounded">
                    <span>日期: <b>{hoverData.time}</b></span>
                    <span>開: <b className="text-white">{hoverData.open?.toLocaleString()}</b></span>
                    <span>高: <b className="text-red-400">{hoverData.high?.toLocaleString()}</b></span>
                    <span>低: <b className="text-green-400">{hoverData.low?.toLocaleString()}</b></span>
                    <span>收: <b className="text-white">{hoverData.close?.toLocaleString()}</b></span>
                    <span>量: <b className="text-yellow-400">{hoverData.volume?.toLocaleString()}</b></span>
                    {hoverData.MA5 && <span className="text-blue-400">MA5: {hoverData.MA5}</span>}
                    {hoverData.MA20 && <span className="text-orange-400">MA20: {hoverData.MA20}</span>}
                    {hoverData.MA60 && <span className="text-purple-400">MA60: {hoverData.MA60}</span>}
                </div>
            )}

            {/* K線圖 */}
            <div ref={mainContainerRef} className="w-full" style={{ minHeight: height }} />

            {/* 成交量圖 */}
            <div className="mt-2">
                <div className="flex items-center gap-4 mb-1">
                    <span className="text-sm text-slate-400">成交量</span>
                    <span className="text-xs text-blue-400">● Vol_MA5</span>
                    <span className="text-xs text-orange-400">● Vol_MA60</span>
                </div>
                <div ref={volumeContainerRef} className="w-full" style={{ minHeight: volumeHeight }} />
            </div>
        </div>
    );
}
