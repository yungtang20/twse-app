import { Bell, User, Smartphone } from "lucide-react";
import { useMobileView } from "@/context/MobileViewContext";

export function Header() {
    const { isMobileView, setIsMobileView } = useMobileView();

    return (
        <header className="h-16 border-b border-slate-800 bg-slate-900/50 backdrop-blur flex items-center justify-between px-6 sticky top-0 z-10">
            <div className="flex items-center gap-4">
                {/* Breadcrumbs or Page Title could go here */}
                <span className="text-slate-400">歡迎回來，投資者</span>
            </div>

            <div className="flex items-center gap-4">
                <button
                    onClick={() => setIsMobileView(!isMobileView)}
                    className={`px-3 py-1.5 rounded-full text-xs font-bold transition-all flex items-center gap-2 ${isMobileView
                        ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/30'
                        : 'bg-slate-800 text-slate-400 hover:bg-slate-700'
                        }`}
                >
                    <Smartphone className="w-4 h-4" />
                    {isMobileView ? '桌面版' : '手機版'}
                </button>

                <button className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-full transition-colors relative">
                    <Bell className="w-5 h-5" />
                    <span className="absolute top-2 right-2 w-2 h-2 bg-red-500 rounded-full"></span>
                </button>
                <button className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-full transition-colors">
                    <User className="w-5 h-5" />
                </button>
            </div>
        </header>
    );
}
