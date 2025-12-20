import React, { useEffect, useRef } from 'react';
import { createChart } from 'lightweight-charts';

export function CandlestickChart({ data, activeIndicators = [], height = 400 }) {
    const chartContainerRef = useRef(null);
    const chartRef = useRef(null);
    const seriesRef = useRef({});

    useEffect(() => {
        if (!chartContainerRef.current) return;

        const initChart = () => {
            if (!chartContainerRef.current) return;
            const width = chartContainerRef.current.clientWidth || 800;

            const chart = createChart(chartContainerRef.current, {
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
                },
                rightPriceScale: {
                    borderColor: '#334155',
                },
                timeScale: {
                    borderColor: '#334155',
                    timeVisible: true,
                },
            });

            const candlestickSeries = chart.addCandlestickSeries({
                upColor: '#ef4444',
                downColor: '#22c55e',
                borderUpColor: '#ef4444',
                borderDownColor: '#22c55e',
                wickUpColor: '#ef4444',
                wickDownColor: '#22c55e',
            });

            const sampleData = [
                { time: '2024-12-01', open: 17500, high: 17650, low: 17450, close: 17600 },
                { time: '2024-12-02', open: 17600, high: 17750, low: 17550, close: 17700 },
                { time: '2024-12-03', open: 17700, high: 17800, low: 17600, close: 17650 },
                { time: '2024-12-04', open: 17650, high: 17900, low: 17600, close: 17850 },
                { time: '2024-12-05', open: 17850, high: 17950, low: 17750, close: 17800 },
                { time: '2024-12-06', open: 17800, high: 17850, low: 17650, close: 17700 },
                { time: '2024-12-09', open: 17700, high: 17800, low: 17550, close: 17600 },
                { time: '2024-12-10', open: 17600, high: 17700, low: 17500, close: 17650 },
                { time: '2024-12-11', open: 17650, high: 17850, low: 17600, close: 17800 },
                { time: '2024-12-12', open: 17800, high: 17900, low: 17700, close: 17856 },
                { time: '2024-12-13', open: 17856, high: 17980, low: 17800, close: 17950 },
                { time: '2024-12-16', open: 17950, high: 18050, low: 17900, close: 18000 },
                { time: '2024-12-17', open: 18000, high: 18100, low: 17950, close: 17980 },
                { time: '2024-12-18', open: 17980, high: 18020, low: 17850, close: 17900 },
                { time: '2024-12-19', open: 17900, high: 17950, low: 17800, close: 17850 },
            ];

            candlestickSeries.setData(data || sampleData);
            seriesRef.current.candle = candlestickSeries;
            chartRef.current = chart;

            const colors = {
                MA5: '#3b82f6',
                MA20: '#f97316',
                MA60: '#a855f7',
                MA200: '#ef4444',
                VWAP: '#eab308',
                BBW: '#6366f1',
                VP: '#14b8a6',
                VSBC: '#ec4899',
                Fib: '#84cc16',
            };

            activeIndicators.forEach(ind => {
                if (['MA5', 'MA20', 'MA60', 'MA200', 'VWAP'].includes(ind)) {
                    const lineSeries = chart.addLineSeries({
                        color: colors[ind],
                        lineWidth: 2,
                        title: ind,
                    });

                    const source = data || sampleData;
                    const lineData = source.map((d) => ({
                        time: d.time,
                        value: d.close * (1 + (Math.random() * 0.02 - 0.01))
                    }));

                    lineSeries.setData(lineData);
                    seriesRef.current[ind] = lineSeries;
                }
            });
        };

        requestAnimationFrame(initChart);

        const handleResize = () => {
            if (chartRef.current && chartContainerRef.current) {
                chartRef.current.applyOptions({
                    width: chartContainerRef.current.clientWidth
                });
            }
        };
        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
            if (chartRef.current) {
                chartRef.current.remove();
                chartRef.current = null;
                seriesRef.current = {};
            }
        };
    }, [data, height, activeIndicators]);

    return <div ref={chartContainerRef} className="w-full" style={{ minHeight: height }} />;
}
