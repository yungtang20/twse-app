import React from 'react';

export function IndicatorToggle({ indicators, activeIndicators, onToggle }) {
    const colors = {
        MA5: '#3b82f6',   // blue
        MA20: '#f97316',  // orange
        MA60: '#a855f7',  // purple
        MA200: '#ef4444', // red
        VWAP: '#eab308',  // yellow
        BBW: '#6366f1',   // indigo
        VP: '#14b8a6',    // teal
        VSBC: '#ec4899',  // pink
        Fib: '#84cc16',   // lime
    };

    return (
        <div className="flex flex-wrap gap-3 mb-2">
            {indicators.map(ind => (
                <button
                    key={ind}
                    onClick={() => onToggle(ind)}
                    className={`flex items-center gap-1.5 px-2 py-1 rounded text-sm transition-opacity
                        ${activeIndicators.includes(ind) ? 'opacity-100' : 'opacity-40'}`}
                >
                    <span
                        className="w-2.5 h-2.5 rounded-full"
                        style={{ backgroundColor: colors[ind] || '#94a3b8' }}
                    />
                    <span className="text-slate-300">{ind}</span>
                </button>
            ))}
        </div>
    );
}

export function PeriodSelector({ periods, activePeriod, onSelect }) {
    return (
        <div className="flex gap-1 bg-slate-800 rounded-lg p-1">
            {periods.map(period => (
                <button
                    key={period}
                    onClick={() => onSelect(period)}
                    className={`px-3 py-1 rounded text-sm transition-colors
                        ${activePeriod === period
                            ? 'bg-blue-600 text-white'
                            : 'text-slate-400 hover:text-white'}`}
                >
                    {period}
                </button>
            ))}
        </div>
    );
}

export function SubChartSelector({ indicators, activeIndicator, onSelect }) {
    return (
        <div className="flex items-center gap-2">
            <span className="text-sm text-slate-400">技術指標</span>
            <select
                value={activeIndicator}
                onChange={(e) => onSelect(e.target.value)}
                className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
                {indicators.map(ind => (
                    <option key={ind} value={ind}>{ind}</option>
                ))}
            </select>
        </div>
    );
}
