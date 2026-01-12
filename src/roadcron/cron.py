"""
RoadCron - Cron Job Scheduling for BlackRoad
Schedule and run periodic tasks with cron expressions.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import asyncio
import logging
import re
import threading
import time

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DISABLED = "disabled"


@dataclass
class JobExecution:
    id: str
    job_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    status: JobStatus = JobStatus.RUNNING
    result: Any = None
    error: Optional[str] = None
    duration_ms: float = 0


@dataclass
class CronJob:
    id: str
    name: str
    schedule: str
    fn: Callable
    enabled: bool = True
    timeout: int = 300
    retries: int = 0
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    executions: List[JobExecution] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CronExpression:
    def __init__(self, expression: str):
        self.expression = expression
        self.minute, self.hour, self.day, self.month, self.weekday = self._parse(expression)

    def _parse(self, expr: str) -> tuple:
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expr}")
        return (
            self._parse_field(parts[0], 0, 59),
            self._parse_field(parts[1], 0, 23),
            self._parse_field(parts[2], 1, 31),
            self._parse_field(parts[3], 1, 12),
            self._parse_field(parts[4], 0, 6)
        )

    def _parse_field(self, field: str, min_val: int, max_val: int) -> set:
        values = set()
        
        if field == "*":
            return set(range(min_val, max_val + 1))
        
        for part in field.split(","):
            if "/" in part:
                base, step = part.split("/")
                step = int(step)
                if base == "*":
                    start = min_val
                else:
                    start = int(base)
                values.update(range(start, max_val + 1, step))
            elif "-" in part:
                start, end = part.split("-")
                values.update(range(int(start), int(end) + 1))
            else:
                values.add(int(part))
        
        return values

    def matches(self, dt: datetime) -> bool:
        return (
            dt.minute in self.minute and
            dt.hour in self.hour and
            dt.day in self.day and
            dt.month in self.month and
            dt.weekday() in self.weekday
        )

    def next_run(self, after: datetime = None) -> datetime:
        dt = after or datetime.now()
        dt = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        for _ in range(525600):  # Max 1 year of minutes
            if self.matches(dt):
                return dt
            dt += timedelta(minutes=1)
        
        return dt


class CronSchedule:
    EVERY_MINUTE = "* * * * *"
    EVERY_5_MINUTES = "*/5 * * * *"
    EVERY_15_MINUTES = "*/15 * * * *"
    EVERY_30_MINUTES = "*/30 * * * *"
    EVERY_HOUR = "0 * * * *"
    EVERY_DAY = "0 0 * * *"
    EVERY_WEEK = "0 0 * * 0"
    EVERY_MONTH = "0 0 1 * *"
    
    @staticmethod
    def every_minutes(n: int) -> str:
        return f"*/{n} * * * *"
    
    @staticmethod
    def every_hours(n: int) -> str:
        return f"0 */{n} * * *"
    
    @staticmethod
    def at(hour: int, minute: int = 0) -> str:
        return f"{minute} {hour} * * *"
    
    @staticmethod
    def weekdays_at(hour: int, minute: int = 0) -> str:
        return f"{minute} {hour} * * 1-5"
    
    @staticmethod
    def weekends_at(hour: int, minute: int = 0) -> str:
        return f"{minute} {hour} * * 0,6"


class Scheduler:
    def __init__(self):
        self.jobs: Dict[str, CronJob] = {}
        self.expressions: Dict[str, CronExpression] = {}
        self._running = False
        self._lock = threading.Lock()
        self.hooks: Dict[str, List[Callable]] = {
            "job_start": [], "job_complete": [], "job_fail": []
        }

    def add_hook(self, event: str, handler: Callable) -> None:
        if event in self.hooks:
            self.hooks[event].append(handler)

    def _emit(self, event: str, data: Any = None) -> None:
        for handler in self.hooks.get(event, []):
            try:
                handler(data)
            except Exception as e:
                logger.error(f"Hook error: {e}")

    def add(self, job_id: str, name: str, schedule: str, fn: Callable, **kwargs) -> CronJob:
        expr = CronExpression(schedule)
        job = CronJob(
            id=job_id,
            name=name,
            schedule=schedule,
            fn=fn,
            next_run=expr.next_run(),
            **kwargs
        )
        
        with self._lock:
            self.jobs[job_id] = job
            self.expressions[job_id] = expr
        
        return job

    def remove(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self.jobs:
                del self.jobs[job_id]
                del self.expressions[job_id]
                return True
        return False

    def enable(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if job:
            job.enabled = True
            return True
        return False

    def disable(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if job:
            job.enabled = False
            return True
        return False

    def run_job(self, job_id: str) -> Optional[JobExecution]:
        job = self.jobs.get(job_id)
        if not job:
            return None
        
        execution = JobExecution(
            id=f"{job_id}_{int(time.time())}",
            job_id=job_id,
            started_at=datetime.now()
        )
        
        self._emit("job_start", execution)
        start = time.time()
        
        try:
            result = job.fn()
            if asyncio.iscoroutine(result):
                result = asyncio.get_event_loop().run_until_complete(result)
            
            execution.result = result
            execution.status = JobStatus.COMPLETED
            self._emit("job_complete", execution)
        except Exception as e:
            execution.error = str(e)
            execution.status = JobStatus.FAILED
            self._emit("job_fail", execution)
            logger.error(f"Job {job_id} failed: {e}")
        
        execution.ended_at = datetime.now()
        execution.duration_ms = (time.time() - start) * 1000
        job.last_run = execution.started_at
        job.executions.append(execution)
        
        if len(job.executions) > 100:
            job.executions = job.executions[-100:]
        
        return execution

    def tick(self) -> List[JobExecution]:
        now = datetime.now().replace(second=0, microsecond=0)
        executions = []
        
        for job_id, job in self.jobs.items():
            if not job.enabled:
                continue
            
            expr = self.expressions.get(job_id)
            if expr and expr.matches(now):
                if job.last_run is None or job.last_run.replace(second=0, microsecond=0) < now:
                    execution = self.run_job(job_id)
                    if execution:
                        executions.append(execution)
                    job.next_run = expr.next_run(now)
        
        return executions

    async def start(self) -> None:
        self._running = True
        logger.info("Scheduler started")
        
        while self._running:
            self.tick()
            await asyncio.sleep(1)

    def stop(self) -> None:
        self._running = False
        logger.info("Scheduler stopped")

    def list_jobs(self) -> List[Dict[str, Any]]:
        return [
            {
                "id": job.id,
                "name": job.name,
                "schedule": job.schedule,
                "enabled": job.enabled,
                "last_run": job.last_run.isoformat() if job.last_run else None,
                "next_run": job.next_run.isoformat() if job.next_run else None,
                "executions": len(job.executions)
            }
            for job in self.jobs.values()
        ]

    def get_job_history(self, job_id: str, limit: int = 10) -> List[JobExecution]:
        job = self.jobs.get(job_id)
        if job:
            return list(reversed(job.executions[-limit:]))
        return []


def job(scheduler: Scheduler, schedule: str, name: str = None, **kwargs) -> Callable:
    def decorator(fn: Callable) -> Callable:
        job_name = name or fn.__name__
        job_id = f"job_{job_name}"
        scheduler.add(job_id, job_name, schedule, fn, **kwargs)
        return fn
    return decorator


def example_usage():
    scheduler = Scheduler()
    
    def cleanup_temp_files():
        print(f"[{datetime.now()}] Cleaning up temp files...")
        return {"files_deleted": 42}
    
    def send_daily_report():
        print(f"[{datetime.now()}] Sending daily report...")
        return {"sent": True}
    
    def health_check():
        print(f"[{datetime.now()}] Running health check...")
        return {"status": "healthy"}
    
    scheduler.add("cleanup", "Cleanup Temp Files", CronSchedule.EVERY_HOUR, cleanup_temp_files)
    scheduler.add("report", "Daily Report", CronSchedule.at(9, 0), send_daily_report)
    scheduler.add("health", "Health Check", CronSchedule.every_minutes(5), health_check)
    
    print("Scheduled jobs:")
    for job in scheduler.list_jobs():
        print(f"  {job['name']}: {job['schedule']} (next: {job['next_run']})")
    
    print("\nManually running cleanup job...")
    execution = scheduler.run_job("cleanup")
    print(f"Result: {execution.result}")
    print(f"Duration: {execution.duration_ms:.2f}ms")
    
    print("\nJob history for cleanup:")
    for exec in scheduler.get_job_history("cleanup"):
        print(f"  {exec.started_at}: {exec.status.value}")

