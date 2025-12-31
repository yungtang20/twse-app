import { Link, useLocation } from "react-router-dom";
import { LayoutDashboard, BarChart2, TrendingUp, LineChart, Settings, Smartphone } from "lucide-react";
import { cn } from "@/lib/utils";
import { useState, useEffect, useRef } from "react";
import { useMobileView } from "@/context/MobileViewContext";

const sidebarItems = [
    { icon: LayoutDashboard, label: "儀表板", path: "/" },
    { icon: BarChart2, label: "市場掃描", path: "/scan" },
    { icon: TrendingUp, label: "法人排行", path: "/rankings" },
    { icon: Settings, label: "系統設定", path: "/settings" },
];

export function Sidebar() {
    const location = useLocation();
    const [width, setWidth] = useState(256);
    const [isResizing, setIsResizing] = useState(false);
    const sidebarRef = useRef(null);
    const { isMobileView, setIsMobileView } = useMobileView();

    const startResizing = (mouseDownEvent) => {
        setIsResizing(true);
    };

    const stopResizing = () => {
        setIsResizing(false);
    };

    const resize = (mouseMoveEvent) => {
        if (isResizing) {
            const newWidth = mouseMoveEvent.clientX - sidebarRef.current.getBoundingClientRect().left;
            if (newWidth > 200 && newWidth < 480) {
                setWidth(newWidth);
            }
        }
    };

    useEffect(() => {
        window.addEventListener("mousemove", resize);
        window.addEventListener("mouseup", stopResizing);
        return () => {
            window.removeEventListener("mousemove", resize);
            window.removeEventListener("mouseup", stopResizing);
        };
    }, [isResizing]);

    return (
        <div
            ref={sidebarRef}
            className="flex flex-col h-screen bg-slate-900 border-r border-slate-800 text-slate-300 relative shrink-0"
            style={{ width: `${width}px` }}
        >
            <div className="p-6">
                <h1 className="text-xl font-bold text-white flex items-center gap-2 overflow-hidden whitespace-nowrap">
                    <LineChart className="w-6 h-6 text-red-500 shrink-0" />
                    TWSE Stock
                </h1>
            </div>

            <nav className="flex-1 px-4 space-y-2 overflow-hidden">
                {sidebarItems.map((item) => {
                    const Icon = item.icon;
                    const isActive = location.pathname === item.path;

                    return (
                        <div key={item.path}>
                            <Link
                                to={item.path}
                                className={cn(
                                    "flex items-center gap-3 px-4 py-3 rounded-lg transition-colors whitespace-nowrap",
                                    isActive
                                        ? "bg-slate-800 text-white font-medium"
                                        : "hover:bg-slate-800/50 hover:text-white"
                                )}
                            >
                                <Icon className="w-5 h-5 shrink-0" />
                                {item.label}
                            </Link>
                        </div>
                    );
                })}
            </nav>

            <div className="p-4 border-t border-slate-800">
                <div className="text-xs text-slate-500 text-center whitespace-nowrap overflow-hidden">
                    v4.0 Enhanced
                </div>
            </div>

            {/* Resize Handle */}
            <div
                className="absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-blue-500 transition-colors z-50"
                onMouseDown={startResizing}
            />
        </div>
    );
}
