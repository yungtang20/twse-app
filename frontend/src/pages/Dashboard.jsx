import { useState } from 'react';
import {
    IndexCard,
    IndicatorToggle,
    PeriodSelector,
    SubChartSelector,
    CandlestickChart,
    VolumeChart,
    WatchlistTable
} from '@/components/charts/ChartComponents';

export function Dashboard() {
    // K線週期狀態
    const [period, setPeriod] = useState('日');
    const periods = ['日', '週', '月'];

    // 主圖指標開關狀態
    const indicators = ['MA5', 'MA20', 'MA60', 'MA200', 'VWAP', 'BBW', 'VP', 'VSBC', 'Fib'];
    const [activeIndicators, setActiveIndicators] = useState(['MA5', 'MA20', 'MA60']);

    // 副圖指標狀態
    const subIndicators = ['日KD', '週KD', '月KD', 'RSI', 'MACD', 'MFI', 'NVI', 'PVI', 'Smart Score', 'ADL', 'SMI', 'SVI'];
    const [activeSubIndicator, setActiveSubIndicator] = useState('MACD');

    // 指標開關處理
    const handleToggleIndicator = (ind) => {
        setActiveIndicators(prev =>
            prev.includes(ind)
                ? prev.filter(i => i !== ind)
                : [...prev, ind]
        );
    };

    // 模擬資料
    const indexData = {
        tse: { title: '加權指數', value: 17856.32, change: 0.68, prevValue: 17735.87, updateTime: '13:45:20' },
        otc: { title: '櫃買指數', value: 234.18, change: 0.53, prevValue: 232.95, updateTime: '13:45:20' },
        vix: { title: '恐慌指數', value: 14.21, change: -5.89, prevValue: 15.10, updateTime: '13:45:20' },
    };

    return (
        <div className="space-y-4 p-4">
            {/* 指數卡片 */}
            <div className="grid gap-4 grid-cols-1 md:grid-cols-3">
                <IndexCard {...indexData.tse} />
                <IndexCard {...indexData.otc} />
                <IndexCard {...indexData.vix} />
            </div>

            {/* 走勢圖區 */}
            <div className="rounded-xl border border-slate-700 bg-slate-800/50 p-4">
                {/* 圖表標題列 */}
                <div className="flex flex-wrap justify-between items-center gap-4 mb-4">
                    <h3 className="text-lg font-semibold text-white">加權指數走勢</h3>
                    <PeriodSelector
                        periods={periods}
                        activePeriod={period}
                        onSelect={setPeriod}
                    />
                </div>

                {/* 指標開關按鈕 */}
                <IndicatorToggle
                    indicators={indicators}
                    activeIndicators={activeIndicators}
                    onToggle={handleToggleIndicator}
                />

                {/* K線圖 */}
                <CandlestickChart height={350} activeIndicators={activeIndicators} />

                {/* 成交量圖 */}
                <div className="mt-4">
                    <VolumeChart height={80} />
                </div>

                {/* 副圖指標 */}
                <div className="mt-4 pt-4 border-t border-slate-700">
                    <SubChartSelector
                        indicators={subIndicators}
                        activeIndicator={activeSubIndicator}
                        onSelect={setActiveSubIndicator}
                    />
                    <div className="mt-2 h-24 bg-slate-900/50 rounded flex items-center justify-center text-slate-500">
                        {activeSubIndicator} 圖表區域
                    </div>
                </div>
            </div>

            {/* 自選股表格 */}
            <WatchlistTable />
        </div>
    );
}
