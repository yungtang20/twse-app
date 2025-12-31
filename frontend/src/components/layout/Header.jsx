export function Header() {
    return (
        <header className="h-16 border-b border-slate-800 bg-slate-900/50 backdrop-blur flex items-center justify-between px-6 sticky top-0 z-10">
            <div className="flex items-center gap-4">
                {/* Breadcrumbs or Page Title could go here */}
                <span className="text-slate-400">歡迎回來，投資者</span>
            </div>
        </header>
    );
}
