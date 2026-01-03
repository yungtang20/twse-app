import { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useMobileView } from "@/context/MobileViewContext";
import TechnicalChart from '@/components/TechnicalChart';
import { supabase } from '@/lib/supabaseClient';

export function Dashboard() {
    const navigate = useNavigate();
    const location = useLocation();
    const { isMobileView } = useMobileView();
    const [stockInfo, setStockInfo] = useState({ name: '加權指數', code: '0000' });
    const [searchTerm, setSearchTerm] = useState('');
    const [stockList, setStockList] = useState([]);
    const [filteredStocks, setFilteredStocks] = useState([]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const [hoverData, setHoverData] = useState(null);
    const [prevData, setPrevData] = useState(null);

    // Handle navigation state (e.g. from Rankings)
    useEffect(() => {
        if (location.state?.code && stockList.length > 0) {
            const found = stockList.find(s => s.code === location.state.code);
            if (found) {
                setStockInfo(found);
                // Clear state to avoid re-triggering on refresh
                window.history.replaceState({}, document.title);
            }
        }
    }, [location.state, stockList]);

    // Fetch Stock List from Supabase
    useEffect(() => {
        const fetchStocks = async () => {
            try {
                const { data, error } = await supabase
                    .from('stock_meta')
                    .select('code, name')
                    .limit(3000);
                if (!error && data) setStockList(data);
            } catch (e) { console.error('Stock list fetch error:', e); }
        };
        fetchStocks();
    }, []);

    // Filter Stocks
    useEffect(() => {
        if (!searchTerm) { setFilteredStocks([]); return; }
        const lower = searchTerm.toLowerCase();
        const filtered = stockList.filter(s => s.code.includes(lower) || s.name.includes(lower)).slice(0, 10);
        setFilteredStocks(filtered);
    }, [searchTerm, stockList]);

    const handleHoverUpdate = (current, prev) => {
        setHoverData(current);
        setPrevData(prev);
    };

    const priceChange = hoverData && prevData ? (hoverData.close - prevData.close).toFixed(2) : '0';
    const priceChangePercent = hoverData && prevData ? ((hoverData.close - prevData.close) / prevData.close * 100).toFixed(2) : '0';
    const volChange = hoverData && prevData ? (hoverData.value - prevData.value) : 0;

    return (
        <div className={`bg-slate-900 h-screen w-screen max-h-screen max-w-screen overflow-hidden flex flex-col p-3 ${isMobileView ? 'justify-start' : ''}`}>
            <div className={`w-full h-full flex flex-col transition-all duration-300 ${isMobileView ? 'max-w-[430px] mx-auto' : ''}`}>

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
                            <span className="text-slate-400 text-xs ml-2">{hoverData?.time || '-'}</span>
                        </div>

                        <div className={`flex ${isMobileView ? 'justify-between text-xs' : 'gap-3'}`}>
                            <span>收 <b className="text-white">{hoverData?.close?.toFixed(2) || '-'}</b></span>
                            <span className={Number(priceChange) > 0 ? 'text-red-400' : Number(priceChange) < 0 ? 'text-green-400' : 'text-slate-400'}>
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
                                <span>收 <b className="text-white">{hoverData?.close?.toFixed(2) || '-'}</b><span className={Number(priceChange) > 0 ? 'text-red-400' : Number(priceChange) < 0 ? 'text-green-400' : 'text-slate-400'}>({Number(priceChange) > 0 ? '+' : ''}{priceChange})</span></span>
                                <span>量 <b className="text-yellow-400">{hoverData ? (hoverData.value / 1000).toFixed(0) : '-'}</b><span className={Number(volChange) > 0 ? 'text-red-400' : Number(volChange) < 0 ? 'text-green-400' : 'text-slate-400'}>({Number(volChange) > 0 ? '+' : ''}{hoverData && prevData ? (volChange / 1000).toFixed(0) : '0'})</span></span>
                                {hoverData?.amount > 0 && <span>額 <b className="text-cyan-400">{(hoverData.amount / 100000000).toFixed(2)}億</b></span>}
                            </>
                        )}
                    </div>
                </div>

                <div className="flex-1 min-h-0">
                    <TechnicalChart code={stockInfo.code} name={stockInfo.name} onHoverData={handleHoverUpdate} />
                </div>
            </div>
        </div>
    );
}

export default Dashboard;
