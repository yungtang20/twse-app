import React, { useState, useEffect } from 'react';
import { Database, RefreshCw, Server, AlertCircle, CheckCircle, Clock, Cloud, CloudOff, Wifi, Activity, Download, Smartphone } from 'lucide-react';
import { useMobileView } from "@/context/MobileViewContext";

export const Settings = () => {
    const { isMobileView, setIsMobileView } = useMobileView();
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(false);
    const [activeTask, setActiveTask] = useState(null);
    const [taskProgress, setTaskProgress] = useState(null);
    const [finmindToken, setFinmindToken] = useState('');

    const [errorMsg, setErrorMsg] = useState(null);
    const [syncMode, setSyncMode] = useState('hybrid');
    const [readSource, setReadSource] = useState('local');
    const [updateTarget, setUpdateTarget] = useState('local');
    const [lastSyncTime, setLastSyncTime] = useState(null);
    const [supabaseConnected, setSupabaseConnected] = useState(false);
    const [syncStatus, setSyncStatus] = useState(null);
    const [dbPath, setDbPath] = useState('');
    const [dbPathExists, setDbPathExists] = useState(true);
    const [dbPathSaving, setDbPathSaving] = useState(false);

    const fetchStatus = async () => {
        setLoading(true);
        setErrorMsg(null);

        // Check if we're in production (APK) mode - no local backend
        const isProduction = !import.meta.env.DEV;
        if (isProduction) {
            // In production, skip backend API calls
            setLoading(false);
            return;
        }

        try {
            const res = await fetch('/api/admin/status');
            if (!res.ok) throw new Error(res.statusText);
            const data = await res.json();
            if (data.success) {
                setStatus(data.data);
            }

            // Fetch Config
            const configRes = await fetch('/api/admin/config');
            if (configRes.ok) {
                const configData = await configRes.json();
                if (configData.success) {
                    setFinmindToken(configData.data.finmind_token || '');
                }
            }
        } catch (error) {
            console.warn('Status API unavailable:', error);
            // Set mock status for dev
            if (import.meta.env.DEV) {
                setStatus({
                    db_path: 'taiwan_stock.db',
                    latest_date: 20260102,
                    total_stocks: 1935,
                    sync_status: 'idle'
                });
            }
        } finally {
            setLoading(false);
        }
    };

    const saveConfig = async () => {
        try {
            const res = await fetch('/api/admin/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ finmind_token: finmindToken })
            });
            const data = await res.json();
            if (data.success) {
                alert('設定已儲存');
            }
        } catch (error) {
            console.error('Failed to save config:', error);
            alert('儲存失敗');
        }
    };

    const fetchDbPath = async () => {
        try {
            const res = await fetch('/api/admin/db-path');
            if (res.ok) {
                const data = await res.json();
                if (data.success) {
                    setDbPath(data.data.db_path || '');
                    setDbPathExists(data.data.exists);
                }
            }
        } catch (error) {
            console.warn('Failed to fetch db path:', error);
        }
    };

    const saveDbPath = async () => {
        if (!dbPath.trim()) return;
        setDbPathSaving(true);
        try {
            const res = await fetch('/api/admin/db-path', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ db_path: dbPath })
            });
            const data = await res.json();
            if (data.success) {
                setDbPathExists(true);
                alert('資料庫路徑已更新');
            } else {
                setDbPathExists(false);
                alert(data.message || '路徑無效');
            }
        } catch (error) {
            console.error('Failed to save db path:', error);
            alert('儲存失敗');
        } finally {
            setDbPathSaving(false);
        }
    };

    const handleFileSelect = (event) => {
        const file = event.target.files[0];
        if (file) {
            // Note: Browser security prevents getting full path.
            // We can only get the name. User might need to manually adjust if not in default location.
            setDbPath(file.name);
            alert('注意：由於瀏覽器安全限制，無法自動取得完整路徑。\n如果檔案不在程式預設目錄下，請手動補全路徑。');
        }
    };

    useEffect(() => {
        fetchStatus();
        fetchDbPath();
    }, []);

    // Poll for task progress if there is an active task
    useEffect(() => {
        let interval;
        if (activeTask) {
            interval = setInterval(async () => {
                try {
                    const res = await fetch(`/api/admin/task/${activeTask}`);
                    const data = await res.json();
                    if (data.success) {
                        setTaskProgress(data.data);
                        if (data.data.status === 'completed' || data.data.status === 'failed') {
                            clearInterval(interval);
                            // Refresh status after completion
                            if (data.data.status === 'completed') {
                                setTimeout(() => {
                                    fetchStatus();
                                    fetchSyncStatus();
                                }, 1000);
                            }
                        }
                    }
                } catch (error) {
                    console.error('Failed to poll task:', error);
                }
            }, 1000);
        }
        return () => clearInterval(interval);
    }, [activeTask]);

    const handleUpdate = async (type, targetOverride = null) => {
        if (activeTask && taskProgress?.status === 'running') return;

        if (targetOverride && targetOverride !== updateTarget) {
            await handleSettingChange('update', targetOverride);
        }

        try {
            const endpoint = type === 'daily' ? '/api/admin/update/daily' : '/api/admin/update/streaks';
            const res = await fetch(`${endpoint}`, { method: 'POST' });
            const data = await res.json();
            if (data.success) {
                setActiveTask(data.data.task_id);
                setTaskProgress(data.data);
            }
        } catch (error) {
            console.error('Failed to trigger update:', error);
        }
    };

    const handleSettingChange = async (type, source) => {
        // type: 'read' or 'update'
        // source: 'local' or 'cloud'

        // Update local state immediately
        if (type === 'read') {
            setReadSource(source);
        } else {
            setUpdateTarget(source);
        }

        try {
            const res = await fetch('/api/admin/sync-mode', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    read_source: type === 'read' ? source : readSource,
                    update_target: type === 'update' ? source : updateTarget
                })
            });
            const data = await res.json();
            if (!data.success) {
                console.error('Failed to save sync settings');
            }
        } catch (error) {
            console.error('Failed to set sync setting:', error);
        }
    };

    const handleSync = async (type) => {
        if (activeTask && taskProgress?.status === 'running') return;

        try {
            const endpoint = type === 'push' ? '/api/admin/sync/push' : '/api/admin/sync/pull';
            const res = await fetch(endpoint, { method: 'POST' });
            const data = await res.json();
            if (data.success) {
                setActiveTask(data.data.task_id);
                setTaskProgress(data.data);
            }
        } catch (error) {
            console.error('Failed to trigger sync:', error);
        }
    };

    const fetchSyncMode = async () => {
        // Skip in production mode
        if (!import.meta.env.DEV) return;
        try {
            const res = await fetch('/api/admin/sync-mode');
            if (res.ok) {
                const data = await res.json();
                if (data.success) {
                    setSyncMode(data.data.sync_mode || 'hybrid');
                    setReadSource(data.data.read_source || 'local');
                    setUpdateTarget(data.data.update_target || 'local');
                    setLastSyncTime(data.data.last_sync_time);
                    setSupabaseConnected(data.data.supabase_connected);
                }
            }
        } catch (error) {
            console.warn('Failed to fetch sync mode:', error);
        }
    };

    const fetchSyncStatus = async () => {
        // Skip in production mode
        if (!import.meta.env.DEV) return;
        try {
            const res = await fetch('/api/admin/sync/status');
            if (res.ok) {
                const data = await res.json();
                if (data.success) {
                    setSyncStatus(data.data);
                }
            }
        } catch (error) {
            console.warn('Failed to fetch sync status:', error);
        }
    };

    useEffect(() => {
        fetchSyncMode();
        fetchSyncStatus();
    }, []);

    const formatDate = (dateInt) => {
        if (!dateInt) return '無資料';
        const str = dateInt.toString();
        return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
    };

    return (
        <div className="h-screen w-screen overflow-hidden flex flex-col pb-10 bg-slate-900 text-slate-300">
            {/* Header - Compact */}
            <div className="shrink-0 px-3 py-2 border-b border-slate-800 flex items-center gap-2">
                <Server className="w-5 h-5 text-blue-500" />
                <h1 className="text-lg font-bold text-white">系統設定</h1>
            </div>

            {/* Scrollable Content */}
            <div className="flex-1 overflow-y-auto p-2 space-y-2">
                {errorMsg && (
                    <div className="bg-red-500/10 border border-red-500/50 text-red-500 p-2 rounded text-xs flex items-center gap-2">
                        <AlertCircle className="w-4 h-4" />
                        <span>{errorMsg}</span>
                    </div>
                )}

                {/* Grid for Settings Cards */}
                <div className="grid grid-cols-1 gap-2">
                    {/* API Settings */}
                    <div className="bg-slate-800 p-2 rounded border border-slate-700 flex flex-col gap-1">
                        <div className="flex items-center gap-2">
                            <Database className="w-4 h-4 text-purple-500" />
                            <span className="font-semibold text-white text-sm">API 設定</span>
                            <span className="text-[10px] text-slate-500">FinMind Token</span>
                        </div>
                        <div className="flex gap-2">
                            <input
                                type="text"
                                value={finmindToken}
                                onChange={(e) => setFinmindToken(e.target.value)}
                                placeholder="Token"
                                className="flex-1 bg-slate-900 border border-slate-700 rounded px-2 py-1 text-xs text-white focus:outline-none focus:border-blue-500"
                            />
                            <button onClick={saveConfig} className="bg-purple-600 hover:bg-purple-500 text-white px-3 py-1 rounded text-xs">儲存</button>
                        </div>
                    </div>

                    {/* Display Settings */}
                    <div className="bg-slate-800 p-2 rounded border border-slate-700 flex items-center justify-between">
                        <div className="flex items-center gap-2">
                            <Smartphone className="w-4 h-4 text-green-500" />
                            <span className="font-semibold text-white text-sm">顯示設定</span>
                        </div>
                        <button
                            onClick={() => setIsMobileView(!isMobileView)}
                            className={`px-3 py-1 rounded text-xs transition-colors ${isMobileView ? 'bg-slate-700 text-slate-300' : 'bg-green-600 text-white'}`}
                        >
                            {isMobileView ? '切換至桌面版' : '切換至手機版'}
                        </button>
                    </div>

                    {/* Database Path */}
                    <div className="bg-slate-800 p-2 rounded border border-slate-700 flex flex-col gap-1">
                        <div className="flex items-center gap-2">
                            <Database className="w-4 h-4 text-orange-500" />
                            <span className="font-semibold text-white text-sm">資料庫路徑</span>
                            {dbPathExists ? <CheckCircle className="w-3 h-3 text-green-500" /> : <AlertCircle className="w-3 h-3 text-red-500" />}
                        </div>
                        <div className="flex gap-2">
                            <input
                                type="text"
                                value={dbPath}
                                onChange={(e) => setDbPath(e.target.value)}
                                placeholder="Path"
                                className={`flex-1 bg-slate-900 border rounded px-2 py-1 text-xs text-white focus:outline-none ${dbPathExists ? 'border-slate-700' : 'border-red-500'}`}
                            />
                            <label className="bg-slate-700 hover:bg-slate-600 px-2 py-1 rounded text-xs text-white cursor-pointer border border-slate-500">
                                瀏覽
                                <input type="file" accept=".db,.sqlite" onChange={handleFileSelect} className="hidden" />
                            </label>
                            <button onClick={saveDbPath} disabled={dbPathSaving} className="bg-orange-600 hover:bg-orange-500 disabled:opacity-50 text-white px-3 py-1 rounded text-xs">
                                {dbPathSaving ? '...' : '儲存'}
                            </button>
                        </div>
                    </div>
                </div>

                {/* Sync Settings */}
                <div className="bg-slate-800 rounded border border-slate-700 overflow-hidden flex-1 min-h-0 flex flex-col">
                    <div className="p-2 border-b border-slate-700 flex items-center justify-between bg-slate-800">
                        <div className="flex items-center gap-2">
                            <Server className="w-4 h-4 text-cyan-500" />
                            <h3 className="text-sm font-semibold text-white">系統控制</h3>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="flex bg-slate-900/50 p-0.5 rounded border border-slate-700/50">
                                <button onClick={() => handleSettingChange('read', 'local')} className={`px-2 py-0.5 rounded text-[10px] ${readSource === 'local' ? 'bg-slate-700 text-white' : 'text-slate-400'}`}>本地</button>
                                <button onClick={() => handleSettingChange('read', 'cloud')} className={`px-2 py-0.5 rounded text-[10px] ${readSource === 'cloud' ? 'bg-blue-600 text-white' : 'text-slate-400'}`}>雲端</button>
                            </div>
                            <div onClick={async () => { setSupabaseConnected(false); try { await fetch('/api/admin/connect-cloud', { method: 'POST' }); } catch (e) { } fetchSyncMode(); fetchSyncStatus(); }} className="flex items-center gap-1 bg-slate-900/50 px-2 py-1 rounded-full cursor-pointer border border-transparent hover:border-slate-700">
                                <div className={`w-1.5 h-1.5 rounded-full ${supabaseConnected ? "bg-green-500" : "bg-red-500"}`}></div>
                                <span className={`text-[10px] ${supabaseConnected ? "text-green-400" : "text-red-400"}`}>{supabaseConnected ? "已連線" : "未連線"}</span>
                            </div>
                        </div>
                    </div>

                    <div className="p-2 overflow-y-auto">
                        {syncStatus ? (
                            <div className="space-y-2">
                                {/* Local Row */}
                                <div onClick={() => handleUpdate('daily', 'local')} className={`group relative flex items-center gap-2 p-2 bg-slate-800/50 rounded border border-slate-700/50 ${activeTask && taskProgress?.status === 'running' ? 'opacity-50' : 'cursor-pointer hover:bg-slate-800'}`}>
                                    <Database className="w-3 h-3 text-slate-400" />
                                    <span className="text-xs font-medium text-slate-300 w-12">本地</span>
                                    <div className="flex-1 grid grid-cols-4 gap-1 text-[10px]">
                                        <div className="flex flex-col"><span className="text-slate-500">日期</span><span className="text-white">{formatDate(syncStatus.local?.latest_date)}</span></div>
                                        <div className="flex flex-col"><span className="text-slate-500">法人</span><span className="text-white">{formatDate(status?.institutional_date)}</span></div>
                                        <div className="flex flex-col"><span className="text-slate-500">數量</span><span className="text-white">{syncStatus.local?.stock_count}</span></div>
                                        <div className="flex flex-col"><span className="text-slate-500">大小</span><span className="text-white">{status?.db_size_mb}MB</span></div>
                                    </div>
                                </div>

                                {/* Cloud Row */}
                                <div onClick={() => handleUpdate('daily', 'cloud')} className={`group relative flex items-center gap-2 p-2 bg-blue-900/10 rounded border border-blue-500/10 ${activeTask && taskProgress?.status === 'running' ? 'opacity-50' : 'cursor-pointer hover:bg-blue-900/20'}`}>
                                    <Cloud className="w-3 h-3 text-blue-400" />
                                    <span className="text-xs font-medium text-blue-400 w-12">雲端</span>
                                    <div className="flex-1 grid grid-cols-4 gap-1 text-[10px]">
                                        <div className="flex flex-col"><span className="text-slate-500">日期</span><span className={`font-mono ${syncStatus.cloud?.latest_date === syncStatus.local?.latest_date ? "text-green-400" : "text-yellow-400"}`}>{formatDate(syncStatus.cloud?.latest_date)}</span></div>
                                        <div className="flex flex-col"><span className="text-slate-500">數量</span><span className={`font-mono ${syncStatus.cloud?.stock_count === syncStatus.local?.stock_count ? "text-green-400" : "text-yellow-400"}`}>{syncStatus.cloud?.stock_count}</span></div>
                                    </div>
                                    <button onClick={(e) => { e.stopPropagation(); handleSync('pull'); }} disabled={activeTask && taskProgress?.status === 'running'} className="px-2 py-1 bg-slate-800 text-slate-300 text-[10px] rounded border border-slate-700 flex items-center gap-1">
                                        <Download className="w-3 h-3" /> 拉取
                                    </button>
                                </div>
                            </div>
                        ) : (
                            <div className="p-4 text-center text-xs text-slate-500">載入中...</div>
                        )}
                    </div>
                </div>

                {/* Task Progress */}
                {activeTask && taskProgress && (
                    <div className="bg-slate-800 p-2 rounded border border-slate-700">
                        <div className="flex items-center justify-between mb-1">
                            <h3 className="font-semibold text-white text-xs flex items-center gap-1">
                                {taskProgress.status === 'running' && <RefreshCw className="w-3 h-3 animate-spin text-blue-500" />}
                                {taskProgress.status === 'completed' && <CheckCircle className="w-3 h-3 text-green-500" />}
                                {taskProgress.status === 'failed' && <AlertCircle className="w-3 h-3 text-red-500" />}
                                {activeTask}
                            </h3>
                            <span className="text-xs text-slate-400">{taskProgress.progress}%</span>
                        </div>
                        <div className="w-full bg-slate-700 rounded-full h-1.5 mb-1">
                            <div className={`h-1.5 rounded-full transition-all duration-500 ${taskProgress.status === 'failed' ? 'bg-red-500' : taskProgress.status === 'completed' ? 'bg-green-500' : 'bg-blue-500'}`} style={{ width: `${taskProgress.progress}%` }}></div>
                        </div>
                        <p className="text-[10px] text-slate-400 truncate">{taskProgress.message}</p>
                    </div>
                )}
            </div>
        </div>
    );
};
