import { Link, useLocation } from "react-router-dom";
import { LayoutDashboard, BarChart2, TrendingUp, LineChart, Settings } from "lucide-react";
import { cn } from "@/lib/utils";

const sidebarItems = [
    { icon: LayoutDashboard, label: "儀表板", path: "/" },
    { icon: BarChart2, label: "市場掃描", path: "/scan" },
    { icon: TrendingUp, label: "法人排行", path: "/rankings" },
    { icon: LineChart, label: "個股分析", path: "/analysis" },
    { icon: Settings, label: "系統設定", path: "/settings" },
];

export function Sidebar() {
    const location = useLocation();

    return (
        <div className="flex flex-col w-64 h-screen bg-slate-900 border-r border-slate-800 text-slate-300">
            <div className="p-6">
                <h1 className="text-xl font-bold text-white flex items-center gap-2">
                    <LineChart className="w-6 h-6 text-red-500" />
                    TWSE Stock
                </h1>
            </div>

            <nav className="flex-1 px-4 space-y-2">
                {sidebarItems.map((item) => {
                    const Icon = item.icon;
                    const isActive = location.pathname === item.path;

                    return (
                        <Link
                            key={item.path}
                            to={item.path}
                            className={cn(
                                "flex items-center gap-3 px-4 py-3 rounded-lg transition-colors",
                                isActive
                                    ? "bg-slate-800 text-white font-medium"
                                    : "hover:bg-slate-800/50 hover:text-white"
                            )}
                        >
                            <Icon className="w-5 h-5" />
                            {item.label}
                        </Link>
                    );
                })}
            </nav>

            <div className="p-4 border-t border-slate-800">
                <div className="text-xs text-slate-500 text-center">
                    v4.0 Enhanced
                </div>
            </div>
        </div>
    );
}
