import { Routes, Route } from "react-router-dom";
import { Layout } from "@/components/layout/Layout";
import { Dashboard } from "@/pages/Dashboard";

// Placeholders for other pages
const MarketScan = () => <div className="text-2xl font-bold">市場掃描 (建置中)</div>;
const Rankings = () => <div className="text-2xl font-bold">法人排行 (建置中)</div>;
const StockAnalysis = () => <div className="text-2xl font-bold">個股分析 (建置中)</div>;
const Settings = () => <div className="text-2xl font-bold">系統設定 (建置中)</div>;

function App() {
    return (
        <Routes>
            <Route element={<Layout />}>
                <Route path="/" element={<Dashboard />} />
                <Route path="/scan" element={<MarketScan />} />
                <Route path="/rankings" element={<Rankings />} />
                <Route path="/analysis" element={<StockAnalysis />} />
                <Route path="/settings" element={<Settings />} />
            </Route>
        </Routes>
    );
}

export default App;
