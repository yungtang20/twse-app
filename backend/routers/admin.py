"""
台灣股市分析系統 - 系統管理 API 路由
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from services.db import get_system_status

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


async def run_daily_update(task_id: str):
    """背景執行每日更新"""
    try:
        _task_status[task_id] = {"status": "running", "progress": 0, "message": "開始更新"}
        
        # 模擬更新步驟
        steps = [
            "更新上市櫃清單",
            "下載 TPEx 資料",
            "下載 TWSE 資料",
            "下載法人資料",
            "計算技術指標"
        ]
        
        for i, step in enumerate(steps):
            _task_status[task_id] = {
                "status": "running",
                "progress": int((i + 1) / len(steps) * 100),
                "message": step
            }
            # 實際更新邏輯可在此呼叫原始 Python 函數
            # await asyncio.sleep(1)  # 模擬
        
        _task_status[task_id] = {
            "status": "completed",
            "progress": 100,
            "message": "更新完成"
        }
    except Exception as e:
        _task_status[task_id] = {
            "status": "failed",
            "progress": 0,
            "message": str(e)
        }


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
        
        log_path = Path("../system.log")
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
