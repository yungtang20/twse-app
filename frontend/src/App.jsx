import { Routes, Route } from "react-router-dom";
import { Layout } from "@/components/layout/Layout";
import { Dashboard } from "@/pages/Dashboard";
import { Scan } from "@/pages/Scan";
import { MobileViewProvider } from "@/context/MobileViewContext";

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
                    <Route path="/rankings" element={<Rankings />} />
                    <Route path="/settings" element={<Settings />} />
                </Route>
            </Routes>
        </MobileViewProvider>
    );
}

export default App;
