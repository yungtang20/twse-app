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
        <div className="bg-slate-900 h-screen w-screen overflow-hidden flex flex-col pb-10">
            {/* Full screen chart - no header */}
            <div className="flex-1 min-h-0 w-full">
                <TechnicalChart
                    code={stockInfo.code}
                    name={stockInfo.name}
                    onHoverData={handleHoverUpdate}
                    stockList={stockList}
                    onStockChange={(s) => setStockInfo(s)}
                />
            </div>
        </div>
    );
}

export default Dashboard;
