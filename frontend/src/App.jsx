import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Home from './pages/Home'
import Scan from './pages/Scan'
import Ranking from './pages/Ranking'
import StockDetail from './pages/StockDetail'
import Settings from './pages/Settings'

function App() {
    return (
        <Routes>
            <Route path="/" element={<Layout />}>
                <Route index element={<Home />} />
                <Route path="scan" element={<Scan />} />
                <Route path="scan/:type" element={<Scan />} />
                <Route path="ranking" element={<Ranking />} />
                <Route path="ranking/:entity/:direction" element={<Ranking />} />
                <Route path="stock/:code" element={<StockDetail />} />
                <Route path="settings" element={<Settings />} />
            </Route>
        </Routes>
    )
}

export default App
