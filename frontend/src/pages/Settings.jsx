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
        try {
            const res = await fetch('/api/admin/status');
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            const data = await res.json();
            if (data.success) {
                setStatus(data.data);
            }

            // Fetch Config
            const configRes = await fetch('/api/admin/config');
            const configData = await configRes.json();
            if (configData.success) {
                setFinmindToken(configData.data.finmind_token || '');
            }
        } catch (error) {
            console.error('Failed to fetch status:', error);
            setErrorMsg(error.message);
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
            const data = await res.json();
            if (data.success) {
                setDbPath(data.data.db_path || '');
                setDbPathExists(data.data.exists);
            }
        } catch (error) {
            console.error('Failed to fetch db path:', error);
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
        try {
            const res = await fetch('/api/admin/sync-mode');
            const data = await res.json();
            if (data.success) {
                setSyncMode(data.data.sync_mode || 'hybrid');
                setReadSource(data.data.read_source || 'local');
                setUpdateTarget(data.data.update_target || 'local');
                setLastSyncTime(data.data.last_sync_time);
                setSupabaseConnected(data.data.supabase_connected);
            }
        } catch (error) {
            console.error('Failed to fetch sync mode:', error);
        }
    };

    const fetchSyncStatus = async () => {
        try {
            const res = await fetch('/api/admin/sync/status');
            const data = await res.json();
            if (data.success) {
                setSyncStatus(data.data);
            }
        } catch (error) {
            console.error('Failed to fetch sync status:', error);
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
        <div className="p-6 bg-slate-900 min-h-screen text-slate-300">
            <h1 className="text-2xl font-bold text-white mb-6 flex items-center gap-2">
                <Server className="w-8 h-8 text-blue-500" />
                系統設定
            </h1>

            {errorMsg && (
                <div className="bg-red-500/10 border border-red-500/50 text-red-500 p-4 rounded-lg mb-6 flex items-center gap-2">
                    <AlertCircle className="w-5 h-5" />
                    <span>{errorMsg}</span>
                </div>
            )}

            {/* API Settings */}
            <div className="bg-slate-800 p-4 rounded-lg border border-slate-700 mb-6 flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div className="flex items-center gap-2">
                    <Database className="w-5 h-5 text-purple-500" />
                    <span className="font-semibold text-white">API 設定</span>
                    <span className="text-xs text-slate-500 ml-2">FinMind Token</span>
                </div>
                <div className="flex gap-2 flex-1 max-w-3xl">
                    <input
                        type="text"
                        value={finmindToken}
                        onChange={(e) => setFinmindToken(e.target.value)}
                        placeholder="請輸入 FinMind Token"
                        className="flex-1 bg-slate-900 border border-slate-700 rounded px-3 py-1.5 text-sm text-white focus:outline-none focus:border-blue-500"
                    />
                    <button
                        onClick={saveConfig}
                        className="bg-purple-600 hover:bg-purple-500 text-white px-4 py-1.5 rounded text-sm transition-colors"
                    >
                        儲存
                    </button>
                </div>
            </div>

            {/* Display Settings */}
            <div className="bg-slate-800 p-4 rounded-lg border border-slate-700 mb-6 flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div className="flex items-center gap-2">
                    <Smartphone className="w-5 h-5 text-green-500" />
                    <span className="font-semibold text-white">顯示設定</span>
                    <span className="text-xs text-slate-500 ml-2">Display Mode</span>
                </div>
                <div className="flex items-center gap-4">
                    <span className="text-sm text-slate-400">
                        {isMobileView ? '目前為手機版介面' : '目前為桌面版介面'}
                    </span>
                    <button
                        onClick={() => setIsMobileView(!isMobileView)}
                        className={`px-4 py-1.5 rounded text-sm transition-colors ${isMobileView
                            ? 'bg-slate-700 text-slate-300 hover:bg-slate-600'
                            : 'bg-green-600 text-white hover:bg-green-500'
                            }`}
                    >
                        {isMobileView ? '切換至桌面版' : '切換至手機版'}
                    </button>
                </div>
            </div>

            {/* Database Path Settings */}
            <div className="bg-slate-800 p-4 rounded-lg border border-slate-700 mb-6 flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div className="flex items-center gap-2">
                    <Database className="w-5 h-5 text-orange-500" />
                    <span className="font-semibold text-white">資料庫路徑</span>
                    <span className="text-xs text-slate-500 ml-2">Database Path</span>
                    {dbPathExists ? (
                        <CheckCircle className="w-4 h-4 text-green-500" />
                    ) : (
                        <AlertCircle className="w-4 h-4 text-red-500" />
                    )}
                </div>
                <div className="flex gap-2 flex-1 max-w-3xl">
                    <input
                        type="text"
                        value={dbPath}
                        onChange={(e) => setDbPath(e.target.value)}
                        placeholder="例如: D:\twse\taiwan_stock.db"
                        className={`flex-1 bg-slate-900 border rounded px-3 py-1.5 text-sm text-white focus:outline-none ${dbPathExists ? 'border-slate-700 focus:border-orange-500' : 'border-red-500'
                            }`}
                    />
                    <label className="bg-slate-700 hover:bg-slate-600 px-3 py-1.5 rounded text-sm text-white cursor-pointer border border-slate-500 transition-colors flex items-center">
                        瀏覽
                        <input
                            type="file"
                            accept=".db,.sqlite"
                            onChange={handleFileSelect}
                            className="hidden"
                        />
                    </label>
                    <button
                        onClick={saveDbPath}
                        disabled={dbPathSaving}
                        className="bg-orange-600 hover:bg-orange-500 disabled:opacity-50 text-white px-4 py-1.5 rounded text-sm transition-colors"
                    >
                        {dbPathSaving ? '儲存中...' : '儲存'}
                    </button>
                </div>
            </div>

            {/* Sync Settings */}
            <div className="bg-slate-800 rounded-lg border border-slate-700 mb-8 overflow-hidden">
                {/* Header & Status */}
                <div className="p-6 border-b border-slate-700 flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div>
                        <div className="flex items-center gap-4 mb-1">
                            <h3 className="text-lg font-semibold text-white flex items-center gap-2">
                                <Server className="w-5 h-5 text-cyan-500" />
                                系統與資料控制
                            </h3>
                            {/* Read Source Toggle */}
                            <div className="flex items-center bg-slate-900/50 p-1 rounded-lg border border-slate-700/50">
                                <button
                                    onClick={() => handleSettingChange('read', 'local')}
                                    className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${readSource === 'local'
                                        ? 'bg-slate-700 text-white shadow-sm'
                                        : 'text-slate-400 hover:text-slate-300'
                                        }`}
                                >
                                    本地讀取
                                </button>
                                <button
                                    onClick={() => handleSettingChange('read', 'cloud')}
                                    className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${readSource === 'cloud'
                                        ? 'bg-blue-600 text-white shadow-sm'
                                        : 'text-slate-400 hover:text-slate-300'
                                        }`}
                                >
                                    雲端讀取
                                </button>
                            </div>
                        </div>
                        <p className="text-sm text-slate-400">
                            最後同步: <span className="font-mono text-slate-300">{lastSyncTime || "從未同步"}</span>
                        </p>
                    </div>
                    <div
                        onClick={async () => {
                            setSupabaseConnected(false); // Reset to trigger visual feedback
                            try {
                                await fetch('/api/admin/connect-cloud', { method: 'POST' });
                            } catch (e) {
                                console.error(e);
                            }
                            fetchSyncMode();
                            fetchSyncStatus();
                        }}
                        className="flex items-center gap-3 bg-slate-900/50 px-4 py-2 rounded-full cursor-pointer hover:bg-slate-800 transition-colors border border-transparent hover:border-slate-700"
                        title="點擊重新連線"
                    >
                        <div className={`w-2 h-2 rounded-full ${supabaseConnected ? "bg-green-500 shadow-[0_0_8px_rgba(34,197,94,0.5)]" : "bg-red-500 animate-pulse"}`}></div>
                        <span className={`text-sm font-medium ${supabaseConnected ? "text-green-400" : "text-red-400"}`}>
                            {supabaseConnected ? "雲端已連線" : "雲端未連線 (點擊重試)"}
                        </span>
                    </div>
                </div>



                <div className="p-6">
                    <div className="bg-slate-900/50 rounded-xl p-4">
                        {syncStatus ? (
                            <div className="space-y-3">
                                {/* Local Row - Click to Update Local */}
                                <div
                                    onClick={() => handleUpdate('daily', 'local')}
                                    className={`group relative flex items-center gap-4 p-3 bg-slate-800/50 rounded-lg border border-slate-700/50 transition-all ${activeTask && taskProgress?.status === 'running' ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:border-cyan-500/50 hover:bg-slate-800'
                                        }`}
                                >
                                    <div className="flex items-center gap-2 min-w-[100px]">
                                        <Database className="w-4 h-4 text-slate-400" />
                                        <span className="text-sm font-medium text-slate-300">本地資料庫</span>
                                    </div>
                                    <div className="flex-1 grid grid-cols-2 md:grid-cols-4 gap-4">
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500">最新日期</span>
                                            <span className="font-mono text-white">
                                                {formatDate(syncStatus.local?.latest_date)}
                                                {syncStatus.local?.last_modified && (
                                                    <span className="text-slate-400 ml-1 text-xs">
                                                        {syncStatus.local.last_modified.split(' ')[1].slice(0, 5)}
                                                    </span>
                                                )}
                                            </span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500">法人買賣</span>
                                            <span className="font-mono text-white">{formatDate(status?.institutional_date)}</span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500">股票數量</span>
                                            <span className="font-mono text-white">{syncStatus.local?.stock_count} 檔</span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500">檔案大小</span>
                                            <span className="font-mono text-white">{status?.db_size_mb} MB</span>
                                        </div>
                                    </div>
                                    {/* Hover Overlay */}
                                    {(!activeTask || taskProgress?.status !== 'running') && (
                                        <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 opacity-0 group-hover:opacity-100 transition-opacity rounded-lg backdrop-blur-[1px]">
                                            <span className="text-cyan-400 font-semibold flex items-center gap-2">
                                                <RefreshCw className="w-4 h-4" /> 點擊執行本地更新
                                            </span>
                                        </div>
                                    )}
                                </div>

                                {/* Cloud Row - Click to Update Cloud */}
                                <div
                                    onClick={() => handleUpdate('daily', 'cloud')}
                                    className={`group relative flex items-center gap-4 p-3 bg-blue-900/10 rounded-lg border border-blue-500/10 transition-all ${activeTask && taskProgress?.status === 'running' ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:border-blue-500/50 hover:bg-blue-900/20'
                                        }`}
                                >
                                    <div className="flex items-center gap-2 min-w-[100px]">
                                        <Cloud className="w-4 h-4 text-blue-400" />
                                        <span className="text-sm font-medium text-blue-400">雲端資料庫</span>
                                    </div>
                                    <div className="flex-1 grid grid-cols-2 md:grid-cols-4 gap-4">
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500">最新日期</span>
                                            <span className={`font-mono ${syncStatus.cloud?.latest_date === syncStatus.local?.latest_date ? "text-green-400" : "text-yellow-400"}`}>
                                                {formatDate(syncStatus.cloud?.latest_date)}
                                            </span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500">股票數量</span>
                                            <span className={`font-mono ${syncStatus.cloud?.stock_count === syncStatus.local?.stock_count ? "text-green-400" : "text-yellow-400"}`}>
                                                {syncStatus.cloud?.stock_count !== undefined ? syncStatus.cloud?.stock_count : "查詢中..."} 檔
                                            </span>
                                        </div>
                                    </div>

                                    {/* Pull Button */}
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            handleSync('pull');
                                        }}
                                        disabled={activeTask && taskProgress?.status === 'running'}
                                        className="ml-auto px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs rounded border border-slate-700 transition-colors flex items-center gap-1 z-10 relative"
                                        title="從雲端拉取"
                                    >
                                        <Download className="w-3 h-3" /> 拉取
                                    </button>

                                    {/* Hover Overlay */}
                                    {(!activeTask || taskProgress?.status !== 'running') && (
                                        <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 opacity-0 group-hover:opacity-100 transition-opacity rounded-lg backdrop-blur-[1px] pointer-events-none">
                                            <span className="text-blue-400 font-semibold flex items-center gap-2">
                                                <RefreshCw className="w-4 h-4" /> 點擊執行雲端更新 (含上傳)
                                            </span>
                                        </div>
                                    )}
                                </div>
                            </div>
                        ) : (
                            <div className="p-8 text-center text-slate-500">
                                載入同步狀態中...
                            </div>
                        )}
                    </div>
                </div>      {/* Action Buttons */}

            </div>

            {/* Task Progress */}
            {
                activeTask && taskProgress && (
                    <div className="bg-slate-800 p-6 rounded-lg border border-slate-700 mb-8 animate-in fade-in slide-in-from-top-4">
                        <div className="flex items-center justify-between mb-2">
                            <h3 className="font-semibold text-white flex items-center gap-2">
                                {taskProgress.status === 'running' && <RefreshCw className="w-4 h-4 animate-spin text-blue-500" />}
                                {taskProgress.status === 'completed' && <CheckCircle className="w-4 h-4 text-green-500" />}
                                {taskProgress.status === 'failed' && <AlertCircle className="w-4 h-4 text-red-500" />}
                                任務執行中: {activeTask}
                            </h3>
                            <span className="text-sm text-slate-400">{taskProgress.progress}%</span>
                        </div>
                        <div className="w-full bg-slate-700 rounded-full h-2.5 mb-2">
                            <div
                                className={`h-2.5 rounded-full transition-all duration-500 ${taskProgress.status === 'failed' ? 'bg-red-500' :
                                    taskProgress.status === 'completed' ? 'bg-green-500' : 'bg-blue-500'
                                    }`}
                                style={{ width: `${taskProgress.progress}%` }}
                            ></div>
                        </div>
                        <p className="text-sm text-slate-400">{taskProgress.message}</p>
                    </div>
                )
            }
        </div >
    );
};
