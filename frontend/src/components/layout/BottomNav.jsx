import { Link, useLocation } from "react-router-dom";
import { LayoutDashboard, BarChart2, TrendingUp, Settings } from "lucide-react";
import { cn } from "@/lib/utils";
import { useMobileView } from "@/context/MobileViewContext";

export function BottomNav() {
    const location = useLocation();
    const { isMobileView, setIsMobileView } = useMobileView();

    const navItems = [
        { icon: LayoutDashboard, label: "儀表板", path: "/" },
        { icon: BarChart2, label: "市場掃描", path: "/scan" },
        { icon: TrendingUp, label: "法人排行", path: "/rankings" },
        { icon: Settings, label: "系統設定", path: "/settings" },
    ];

    return (
        <div className="fixed bottom-0 left-0 right-0 h-16 bg-black border-t border-zinc-800 grid grid-cols-4 z-50">
            {navItems.map((item) => {
                const Icon = item.icon;
                const isActive = location.pathname === item.path;

                return (
                    <Link
                        key={item.path}
                        to={item.path}
                        className={cn(
                            "flex flex-col items-center justify-center gap-1 transition-colors h-full",
                            isActive
                                ? "bg-zinc-800 text-white"
                                : "text-zinc-500 hover:text-zinc-300 hover:bg-zinc-900"
                        )}
                    >
                        <Icon className="w-6 h-6" />
                        <span className="text-[10px] font-medium">{item.label}</span>
                    </Link>
                );
            })}
        </div>
    );
}
