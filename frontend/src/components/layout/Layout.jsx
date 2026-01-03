import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { Header } from "./Header";
import { BottomNav } from "./BottomNav";
import { useMobileView } from "@/context/MobileViewContext";

export function Layout() {
    const { isMobileView } = useMobileView();

    return (
        <div className="flex min-h-screen bg-slate-950 text-slate-50 font-sans">
            {!isMobileView && <Sidebar />}
            <div className="flex-1 flex flex-col min-w-0">
                {!isMobileView && <Header />}
                <main className={`flex-1 p-0 overflow-hidden ${isMobileView ? 'pb-10' : ''}`}>
                    <Outlet />
                </main>
                {isMobileView && <BottomNav />}
            </div>
        </div>
    );
}
