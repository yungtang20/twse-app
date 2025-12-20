import React from 'react';

export function IndexCard({ title, value, change, prevValue, updateTime }) {
    const isUp = change >= 0;
    return (
        <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-4 backdrop-blur">
            <div className="flex justify-between items-start mb-2">
                <h3 className="text-sm font-medium text-slate-400">{title}</h3>
                <span className="text-xs text-slate-500">{updateTime}</span>
            </div>
            <div className="text-2xl font-bold text-white">
                {value?.toLocaleString()}
            </div>
            <div className="flex items-center gap-2 mt-1">
                <span className={`text-sm font-medium ${isUp ? 'text-red-500' : 'text-green-500'}`}>
                    {isUp ? '▲' : '▼'} {Math.abs(change).toFixed(2)}%
                </span>
                <span className="text-xs text-slate-500">
                    昨日: {prevValue?.toLocaleString()}
                </span>
            </div>
        </div>
    );
}
