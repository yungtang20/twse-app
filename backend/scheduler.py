from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging
import sys
import os

# Ensure backend modules can be imported
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

def run_sync_push_job():
    """Wrapper to run sync push with a generated task ID"""
    try:
        # Import here to avoid circular imports during initialization
        from backend.routers.admin import run_sync_push, _task_status
        
        task_id = f"auto_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"⏰ 觸發排程同步任務: {task_id}")
        
        # Initialize task status
        _task_status[task_id] = {
            "status": "pending",
            "progress": 0,
            "message": "排程自動同步啟動..."
        }
        
        # Run the sync
        run_sync_push(task_id)
        
    except Exception as e:
        logger.error(f"排程同步失敗: {e}")

def start_scheduler():
    """Start the scheduler with defined jobs"""
    # 15:30 Daily
    scheduler.add_job(
        run_sync_push_job,
        CronTrigger(hour=15, minute=30),
        id="sync_push_1530",
        replace_existing=True
    )
    
    # 21:30 Daily
    scheduler.add_job(
        run_sync_push_job,
        CronTrigger(hour=21, minute=30),
        id="sync_push_2130",
        replace_existing=True
    )
    
    scheduler.start()
    print("📅 排程器已啟動: 每日 15:30, 21:30 自動同步到雲端")
