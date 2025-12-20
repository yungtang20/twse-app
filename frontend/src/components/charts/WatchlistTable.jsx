import React from 'react';

export function WatchlistTable({ stocks }) {
    const sampleStocks = [
        { code: '2330', name: '台積電', price: 588.00, change: 6.00, changePercent: 1.03, volume: 35210 },
        { code: '2317', name: '鴻海', price: 104.50, change: 0.50, changePercent: 0.48, volume: 12450 },
        { code: '2454', name: '聯發科', price: 892.00, change: -12.00, changePercent: -1.33, volume: 5120 },
        { code: '3008', name: '大立光', price: 2350.00, change: 45.00, changePercent: 1.95, volume: 890 },
        { code: '2303', name: '聯電', price: 48.25, change: 0.25, changePercent: 0.52, volume: 18900 },
        { code: '2603', name: '長榮', price: 145.00, change: -3.00, changePercent: -2.03, volume: 9500 },
    ];

    const data = stocks || sampleStocks;

    return (
        <div className="rounded-xl border border-slate-700 bg-slate-800/50 overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-700">
                <h3 className="text-lg font-semibold text-white">我的自選股</h3>
            </div>
            <div className="overflow-x-auto">
                <table className="w-full">
                    <thead className="bg-slate-800">
                        <tr>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-400">代碼</th>
                            <th className="px-4 py-3 text-left text-xs font-medium text-slate-400">名稱</th>
                            <th className="px-4 py-3 text-right text-xs font-medium text-slate-400">現價</th>
                            <th className="px-4 py-3 text-right text-xs font-medium text-slate-400">漲跌</th>
                            <th className="px-4 py-3 text-right text-xs font-medium text-slate-400">漲跌%</th>
                            <th className="px-4 py-3 text-right text-xs font-medium text-slate-400">成交量</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-700">
                        {data.map((stock) => {
                            const isUp = stock.change >= 0;
                            return (
                                <tr key={stock.code} className="hover:bg-slate-700/50 cursor-pointer transition-colors">
                                    <td className="px-4 py-3 text-sm text-blue-400">{stock.code}</td>
                                    <td className="px-4 py-3 text-sm text-white">{stock.name}</td>
                                    <td className="px-4 py-3 text-sm text-right text-white">{stock.price.toFixed(2)}</td>
                                    <td className={`px-4 py-3 text-sm text-right ${isUp ? 'text-red-500' : 'text-green-500'}`}>
                                        {isUp ? '▲' : '▼'} {Math.abs(stock.change).toFixed(2)}
                                    </td>
                                    <td className={`px-4 py-3 text-sm text-right ${isUp ? 'text-red-500' : 'text-green-500'}`}>
                                        {stock.changePercent > 0 ? '+' : ''}{stock.changePercent.toFixed(2)}%
                                    </td>
                                    <td className="px-4 py-3 text-sm text-right text-slate-300">
                                        {stock.volume.toLocaleString()} 張
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
