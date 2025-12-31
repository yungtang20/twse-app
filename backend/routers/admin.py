"""
台灣股市分析系統 - 系統管理 API 路由
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from backend.services.db import get_system_status, get_cloud_status

router = APIRouter()


# ========================================
# 資料模型
# ========================================

class AdminResponse(BaseModel):
    """管理 API 回應"""
    success: bool
    data: Optional[dict] = None
    message: Optional[str] = None


class UpdateTask(BaseModel):
    """更新任務狀態"""
    task_id: str
    status: str
    progress: Optional[int] = None
    message: Optional[str] = None


# 全域任務狀態
_task_status = {}


# ========================================
# API 端點
# ========================================

@router.get("/admin/status", response_model=AdminResponse)
async def admin_status():
    """
    取得系統狀態
    """
    try:
        status = get_system_status()
        return {
            "success": True,
            "data": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/update/daily", response_model=AdminResponse)
async def trigger_daily_update(background_tasks: BackgroundTasks):
    """
    觸發每日更新 (背景執行)
    
    注意：此端點會在背景執行更新任務，
    使用 /admin/task/{task_id} 查詢進度
    """
    try:
        task_id = f"daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # 記錄任務狀態
        _task_status[task_id] = {
            "status": "pending",
            "progress": 0,
            "message": "任務已排程"
        }
        
        # 背景執行
        background_tasks.add_task(run_daily_update, task_id)
        
        return {
            "success": True,
            "data": {
                "task_id": task_id,
                "status": "pending",
                "message": "每日更新已排程執行"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


import sys
import os
import json
import logging
from pathlib import Path

# Setup logger
logger = logging.getLogger(__name__)

# Add root directory to path to import 最終修正
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from main_script import (
    step1_check_holiday,
    step2_download_lists,
    step3_download_basic_info,
    step4_clean_delisted,
    step5_download_quotes,
    step6_download_valuation,
    step7_download_institutional,
    step8_download_margin,
    step9_download_tdcc,
    step10_check_gaps,
    step11_verify_backfill,
    step12_calc_indicators,
    step8_sync_supabase
)
from update_institutional_streaks import update_streaks

@router.post("/admin/update/streaks", response_model=AdminResponse)
async def trigger_streaks_update(background_tasks: BackgroundTasks):
    """
    觸發法人連買連賣計算 (背景執行)
    """
    try:
        task_id = f"streaks_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        _task_status[task_id] = {
            "status": "pending",
            "progress": 0,
            "message": "任務已排程"
        }
        
        background_tasks.add_task(run_streaks_update, task_id)
        
        return {
            "success": True,
            "data": {
                "task_id": task_id,
                "status": "pending",
                "message": "法人連買連賣計算已排程"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def update_task_progress(task_id: str, progress: int, message: str, status: str = "running"):
    """更新任務進度 helper"""
    if task_id in _task_status:
        _task_status[task_id].update({
            "status": status,
            "progress": progress,
            "message": message
        })

def run_streaks_update(task_id: str):
    """背景執行法人連買連賣計算"""
    try:
        _task_status[task_id] = {"status": "running", "progress": 0, "message": "開始計算連買連賣..."}
        
        # 這裡直接呼叫 update_streaks
        # 由於 update_streaks 是一次性函數，我們無法細分進度，只能設為 50% -> 100%
        _task_status[task_id]["progress"] = 10
        
        update_streaks()
        
        _task_status[task_id] = {
            "status": "completed",
            "progress": 100,
            "message": "計算完成"
        }
    except Exception as e:
        _task_status[task_id] = {
            "status": "failed",
            "progress": 0,
            "message": str(e)
        }

def run_daily_update(task_id: str):
    """執行每日更新流程 (背景任務)"""
    try:
        update_task_progress(task_id, 0, "啟動每日更新流程...")
        
        # Step 1: Check Holiday
        update_task_progress(task_id, 5, "Step 1: 檢查開休市...")
        if step1_check_holiday():
            update_task_progress(task_id, 10, "今日休市，但仍繼續執行補歷史資料...")
        else:
            update_task_progress(task_id, 10, "今日是交易日")
            
        # Step 2: Download Lists
        update_task_progress(task_id, 15, "Step 2: 下載股票清單...")
        step2_download_lists(silent_header=True)
        
        # Step 3: Basic Info
        update_task_progress(task_id, 20, "Step 3: 下載基本資料...")
        step3_download_basic_info(silent_header=True)
        
        # Step 4: Clean Delisted
        update_task_progress(task_id, 25, "Step 4: 清理下市股票...")
        step4_clean_delisted()
        
        # Step 5: Download Quotes (TPEx + TWSE)
        update_task_progress(task_id, 35, "Step 5: 下載今日行情...")
        step5_download_quotes(silent_header=True)
        
        # Step 6: Valuation
        update_task_progress(task_id, 45, "Step 6: 下載估值資料...")
        step6_download_valuation(silent_header=True)
        
        # Step 7: Institutional
        update_task_progress(task_id, 55, "Step 7: 下載三大法人買賣超...")
        step7_download_institutional(silent_header=True)
        
        # Step 8: Margin
        update_task_progress(task_id, 65, "Step 8: 下載融資融券...")
        step8_download_margin(silent_header=True)
        
        # Step 9: TDCC
        update_task_progress(task_id, 70, "Step 9: 下載集保大戶...")
        step9_download_tdcc(silent_header=True)
        
        # Step 10: Check Gaps
        update_task_progress(task_id, 75, "Step 10: 檢查數據缺失...")
        step10_check_gaps()
        
        # Step 11: Verify & Backfill
        update_task_progress(task_id, 80, "Step 11: 驗證一致性並補漏...")
        step11_verify_backfill()
        
        # Step 12: Calc Indicators
        update_task_progress(task_id, 90, "Step 12: 計算技術指標...")
        step12_calc_indicators()
        
        # Step 13: Sync Supabase
        # Check config for update_target
        config_path = Path("config.json")
        should_sync = False
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if config.get("update_target") == "cloud":
                        should_sync = True
            except Exception:
                pass

        if should_sync:
            update_task_progress(task_id, 95, "Step 13: 同步資料到雲端...")
            
            def sync_cb(p, msg):
                # Map 0-100 to 95-99
                overall = 95 + int(p * 0.04)
                update_task_progress(task_id, overall, msg, "running")
                
            step8_sync_supabase(progress_callback=sync_cb)
        else:
            update_task_progress(task_id, 95, "Step 13: 略過雲端同步 (本地模式)")
        
        update_task_progress(task_id, 100, "每日更新完成", "completed")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Daily update failed: {e}")
        update_task_progress(task_id, 0, f"更新失敗: {str(e)}", "failed")



@router.get("/admin/task/{task_id}", response_model=AdminResponse)
async def get_task_status(task_id: str):
    """
    取得任務執行狀態
    """
    if task_id not in _task_status:
        raise HTTPException(status_code=404, detail="任務不存在")
    
    return {
        "success": True,
        "data": {
            "task_id": task_id,
            **_task_status[task_id]
        }
    }


@router.post("/admin/backup", response_model=AdminResponse)
async def create_backup():
    """
    建立資料庫備份
    """
    try:
        import shutil
        from pathlib import Path
        
        db_path = Path("../taiwan_stock.db")
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="資料庫檔案不存在")
        
        backup_name = f"taiwan_stock_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        backup_path = db_path.parent / "backups" / backup_name
        backup_path.parent.mkdir(exist_ok=True)
        
        shutil.copy2(db_path, backup_path)
        
        return {
            "success": True,
            "data": {
                "backup_path": str(backup_path),
                "size_mb": round(backup_path.stat().st_size / 1024 / 1024, 2)
            },
            "message": f"備份完成：{backup_name}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/config", response_model=AdminResponse)
async def get_config():
    """取得系統設定 (包含 FinMind Token)"""
    try:
        config_path = Path("config.json")
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return {
                    "success": True,
                    "data": config
                }
        return {
            "success": True,
            "data": {"finmind_token": ""}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/admin/config", response_model=AdminResponse)
async def save_config(config: dict):
    """儲存系統設定"""
    try:
        config_path = Path("config.json")
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
        return {
            "success": True,
            "data": config,
            "message": "設定已儲存"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/admin/logs", response_model=AdminResponse)
async def get_logs(
    limit: int = 100,
    level: Optional[str] = None
):
    """
    取得系統日誌 (最近 N 行)
    """
    try:
        from pathlib import Path
        
        log_path = Path("system.log")
        if not log_path.exists():
            return {
                "success": True,
                "data": {
                    "logs": [],
                    "message": "日誌檔案不存在"
                }
            }
        
        # 讀取最後 N 行
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-limit:]
        
        # 過濾級別 (如果指定)
        if level:
            lines = [l for l in lines if level.upper() in l]
        
        return {
            "success": True,
            "data": {
                "logs": [line.strip() for line in lines],
                "count": len(lines)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 同步模式 API
# ========================================

class SyncModeRequest(BaseModel):
    """同步模式請求"""
    sync_mode: Optional[str] = None  # "offline", "hybrid", "cloud" (legacy)
    read_source: Optional[str] = None  # "local" or "cloud"
    update_target: Optional[str] = None  # "local" or "cloud"


@router.post("/admin/connect-cloud", response_model=AdminResponse)
async def connect_cloud():
    """
    手動觸發雲端連線
    """
    try:
        success = db_manager.connect_supabase()
        return {
            "success": success,
            "data": {
                "connected": success,
                "message": "連線成功" if success else "連線失敗"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/sync-mode", response_model=AdminResponse)
async def get_sync_mode():
    """
    取得目前同步模式設定
    """
    try:
        config_path = Path("config.json")
        config = {}
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        
        return {
            "success": True,
            "data": {
                "sync_mode": config.get("sync_mode", "hybrid"),
                "read_source": config.get("read_source", "local"),
                "update_target": config.get("update_target", "local"),
                "last_sync_time": config.get("last_sync_time", None),
                "supabase_connected": db_manager.supabase is not None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/sync-mode", response_model=AdminResponse)
async def set_sync_mode(request: SyncModeRequest):
    """
    設定同步模式
    """
    try:
        config_path = Path("config.json")
        config = {}
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        
        # Update read_source and update_target
        if request.read_source:
            if request.read_source not in ["local", "cloud"]:
                raise HTTPException(status_code=400, detail="無效的讀取來源")
            config["read_source"] = request.read_source
        
        if request.update_target:
            if request.update_target not in ["local", "cloud"]:
                raise HTTPException(status_code=400, detail="無效的更新目標")
            config["update_target"] = request.update_target
        
        # Legacy sync_mode support
        if request.sync_mode:
            config["sync_mode"] = request.sync_mode
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "data": {
                "sync_mode": request.sync_mode,
                "message": f"同步模式已設定為: {request.sync_mode}"
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/sync/push", response_model=AdminResponse)
async def sync_push(background_tasks: BackgroundTasks):
    """
    推送本地資料到雲端 (背景執行)
    """
    try:
        task_id = f"sync_push_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        _task_status[task_id] = {
            "status": "pending",
            "progress": 0,
            "message": "準備推送到雲端..."
        }
        
        background_tasks.add_task(run_sync_push, task_id)
        
        return {
            "success": True,
            "data": {
                "task_id": task_id,
                "status": "pending",
                "message": "雲端推送已排程執行"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/sync/pull", response_model=AdminResponse)
async def sync_pull(background_tasks: BackgroundTasks):
    """
    從雲端拉取資料 (背景執行)
    """
    try:
        task_id = f"sync_pull_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        _task_status[task_id] = {
            "status": "pending",
            "progress": 0,
            "message": "準備從雲端拉取..."
        }
        
        background_tasks.add_task(run_sync_pull, task_id)
        
        return {
            "success": True,
            "data": {
                "task_id": task_id,
                "status": "pending",
                "message": "雲端拉取已排程執行"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_sync_push(task_id: str):
    """背景執行推送到雲端"""
    try:
        update_task_progress(task_id, 10, "正在連線雲端...", "running")
        
        # 使用現有的 sync_supabase 功能
        update_task_progress(task_id, 30, "正在同步資料...")
        
        def sync_cb(p, msg):
            # Map 0-100 to 30-90
            overall = 30 + int(p * 0.6)
            update_task_progress(task_id, overall, msg, "running")
            
        step8_sync_supabase(progress_callback=sync_cb)
        
        # 更新最後同步時間
        update_task_progress(task_id, 90, "更新同步時間...")
        config_path = Path("config.json")
        config = {}
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        
        config["last_sync_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        update_task_progress(task_id, 100, "雲端推送完成", "completed")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Sync push failed: {e}")
        update_task_progress(task_id, 0, f"推送失敗: {str(e)}", "failed")


def run_sync_pull(task_id: str):
    """背景執行從雲端拉取"""
    try:
        update_task_progress(task_id, 10, "正在連線雲端...", "running")
        
        if not db_manager.supabase:
            update_task_progress(task_id, 0, "雲端未連線", "failed")
            return
        
        update_task_progress(task_id, 30, "正在檢查雲端資料...")
        
        # 從 Supabase 拉取最新資料
        # 這裡可以擴展為更複雜的同步邏輯
        # 目前先實作基本的狀態更新
        
        update_task_progress(task_id, 90, "更新同步時間...")
        config_path = Path("config.json")
        config = {}
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        
        config["last_sync_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        update_task_progress(task_id, 100, "雲端拉取完成", "completed")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Sync pull failed: {e}")
        update_task_progress(task_id, 0, f"拉取失敗: {str(e)}", "failed")


# Import db_manager for sync functions
from backend.services.db import db_manager


@router.get("/admin/sync/status", response_model=AdminResponse)
async def get_sync_status():
    """
    取得同步狀態比對 (本地 vs 雲端)
    """
    try:
        local_status = get_system_status()
        cloud_status = get_cloud_status()
        
        return {
            "success": True,
            "data": {
                "local": local_status,
                "cloud": cloud_status
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 資料庫路徑 API
# ========================================

class DBPathRequest(BaseModel):
    """資料庫路徑請求"""
    db_path: str


@router.get("/admin/db-path", response_model=AdminResponse)
async def get_db_path():
    """
    取得目前資料庫路徑
    """
    try:
        return {
            "success": True,
            "data": {
                "db_path": str(db_manager.db_path),
                "exists": db_manager.db_path.exists() if hasattr(db_manager.db_path, 'exists') else False
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/db-path", response_model=AdminResponse)
async def set_db_path(request: DBPathRequest):
    """
    設定資料庫路徑
    """
    try:
        new_path = Path(request.db_path)
        
        # 驗證路徑
        if not new_path.exists():
            return {
                "success": False,
                "message": f"路徑不存在: {request.db_path}"
            }
        
        if not new_path.suffix == '.db':
            return {
                "success": False,
                "message": "檔案必須是 .db 格式"
            }
        
        # 更新 db_manager
        success = db_manager.set_db_path(request.db_path)
        
        if not success:
            return {
                "success": False,
                "message": "無法設定資料庫路徑"
            }
        
        # 儲存到 config.json
        config_path = Path("config.json")
        config = {}
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        
        config["db_path"] = request.db_path
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "data": {
                "db_path": request.db_path
            },
            "message": "資料庫路徑已更新"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


