import { Routes, Route } from "react-router-dom";
import { Layout } from "@/components/layout/Layout";
import { Dashboard } from "@/pages/Dashboard";
import { Scan } from "@/pages/Scan";
import { MobileViewProvider } from "@/context/MobileViewContext";
import StockDetail from "@/pages/StockDetail";

// Placeholders for other pages
import { Rankings } from "@/pages/Rankings";
import { Settings } from "@/pages/Settings";

function App() {
    return (
        <MobileViewProvider>
            <Routes>
                <Route element={<Layout />}>
                    <Route path="/" element={<Dashboard />} />
                    <Route path="/scan" element={<Scan />} />
                    <Route path="/stock/:code" element={<StockDetail />} />
                    <Route path="/rankings" element={<Rankings />} />
                    <Route path="/settings" element={<Settings />} />
                </Route>
            </Routes>
        </MobileViewProvider>
    );
}

export default App;
