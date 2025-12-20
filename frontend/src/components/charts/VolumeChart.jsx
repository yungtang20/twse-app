import React, { useEffect, useRef } from 'react';
import { createChart } from 'lightweight-charts';

export function VolumeChart({ data, height = 100 }) {
    const chartContainerRef = useRef(null);
    const chartRef = useRef(null);

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
                    textColor: '#9ca3af',
                },
                grid: {
                    vertLines: { visible: false },
                    horzLines: { color: '#1e293b' },
                },
                rightPriceScale: {
                    borderColor: '#334155',
                },
                timeScale: {
                    visible: false,
                },
            });

            const volumeSeries = chart.addHistogramSeries({
                priceFormat: { type: 'volume' },
                priceScaleId: '',
            });

            const sampleVolume = [
                { time: '2024-12-01', value: 3200, color: '#ef4444' },
                { time: '2024-12-02', value: 3500, color: '#ef4444' },
                { time: '2024-12-03', value: 2800, color: '#22c55e' },
                { time: '2024-12-04', value: 4100, color: '#ef4444' },
                { time: '2024-12-05', value: 3600, color: '#22c55e' },
                { time: '2024-12-06', value: 3100, color: '#22c55e' },
                { time: '2024-12-09', value: 2900, color: '#22c55e' },
                { time: '2024-12-10', value: 3300, color: '#ef4444' },
                { time: '2024-12-11', value: 3800, color: '#ef4444' },
                { time: '2024-12-12', value: 3250, color: '#ef4444' },
            ];

            volumeSeries.setData(data || sampleVolume);
            chartRef.current = chart;
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
            }
        };
    }, [data, height]);

    return (
        <div>
            <div className="flex items-center gap-4 mb-1">
                <span className="text-sm text-slate-400">成交量</span>
                <span className="text-xs text-blue-400">● Vol_MA5</span>
                <span className="text-xs text-orange-400">● Vol_MA60</span>
            </div>
            <div ref={chartContainerRef} className="w-full" style={{ minHeight: height }} />
        </div>
    );
}
