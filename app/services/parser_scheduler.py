from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import Select, or_, select, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.models.enums import JobRunStatus
from app.models.job_run import JobRun
from app.models.scheduled_job import ScheduledJob
from app.services.autonomy_jobs import JOB_REGISTRY

settings = get_settings()
INSTANCE_ID = uuid.uuid4().hex[:12]


def _tz(name: str) -> timezone:
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone(timedelta(hours=3))


def _compute_next_daily(job: ScheduledJob, now: datetime) -> datetime:
    tz = _tz(job.timezone or "Europe/Moscow")
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_local = now.astimezone(tz)
    hour = job.schedule_hour or 0
    minute = job.schedule_minute or 0
    target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target_local <= now_local:
        target_local = target_local + timedelta(days=1)
    return target_local.astimezone(timezone.utc).replace(tzinfo=None)


def _compute_next_interval(job: ScheduledJob, now: datetime) -> datetime:
    minutes = int(job.interval_minutes or 60)
    next_time = now + timedelta(minutes=max(1, minutes))
    if next_time.tzinfo is not None:
        return next_time.astimezone(timezone.utc).replace(tzinfo=None)
    return next_time


def _compute_next_run(job: ScheduledJob, now: datetime) -> datetime | None:
    if job.schedule_type == "daily":
        return _compute_next_daily(job, now)
    if job.schedule_type == "interval":
        return _compute_next_interval(job, now)
    return None


def _acquire_job_lock(db: Session, job: ScheduledJob, now: datetime) -> bool:
    lock_until = now + timedelta(minutes=30)
    stmt = (
        update(ScheduledJob)
        .where(
            ScheduledJob.id == job.id,
            or_(ScheduledJob.lock_expires_at.is_(None), ScheduledJob.lock_expires_at < now),
        )
        .values(locked_by=INSTANCE_ID, lock_expires_at=lock_until)
    )
    result = db.execute(stmt)
    return result.rowcount == 1


def _release_job_lock(job: ScheduledJob) -> None:
    job.locked_by = None
    job.lock_expires_at = None


def _run_job(db: Session, job: ScheduledJob, now: datetime) -> None:
    if job.job_key not in JOB_REGISTRY:
        return
    if not _acquire_job_lock(db, job, now):
        return

    job_run = JobRun(
        job_id=job.id,
        job_key=job.job_key,
        status=JobRunStatus.running,
        scheduled_for=job.next_run_at,
        started_at=now,
    )
    db.add(job_run)
    db.commit()

    handler = JOB_REGISTRY[job.job_key]
    try:
        payload = handler(db, job_run)
        job_run.status = JobRunStatus.success
        job_run.finished_at = datetime.utcnow()
        job_run.run_payload = payload if isinstance(payload, dict) else None
        job.last_run_at = job_run.finished_at
        job.next_run_at = _compute_next_run(job, job_run.finished_at or now)
        if job.metadata_json:
            job.metadata_json["failure_count"] = 0
        else:
            job.metadata_json = {"failure_count": 0}
    except Exception as exc:
        job_run.status = JobRunStatus.failed
        job_run.finished_at = datetime.utcnow()
        job_run.error_message = str(exc)[:1500]
        job.last_run_at = job_run.finished_at
        metadata = job.metadata_json or {}
        failure_count = int(metadata.get("failure_count", 0)) + 1
        metadata["failure_count"] = failure_count
        job.metadata_json = metadata
        backoff_minutes = min(360, 5 * (2 ** min(failure_count, 5)))
        job.next_run_at = datetime.utcnow() + timedelta(minutes=backoff_minutes)
    finally:
        _release_job_lock(job)
        db.commit()


def _ensure_next_run(db: Session) -> None:
    now = datetime.utcnow()
    jobs = db.execute(select(ScheduledJob)).scalars().all()
    for job in jobs:
        if not job.enabled:
            continue
        if job.next_run_at is None:
            job.next_run_at = _compute_next_run(job, now)
    db.commit()


def run_due_jobs() -> None:
    now = datetime.utcnow()
    with SessionLocal() as db:
        _ensure_next_run(db)
        jobs = (
            db.execute(
                select(ScheduledJob)
                .where(
                    ScheduledJob.enabled.is_(True),
                    ScheduledJob.next_run_at.is_not(None),
                    ScheduledJob.next_run_at <= now,
                )
                .order_by(ScheduledJob.next_run_at.asc())
            )
            .scalars()
            .all()
        )
        for job in jobs:
            _run_job(db, job, now)


async def parser_scheduler_loop(stop_event: asyncio.Event) -> None:
    if not settings.parser_scheduler_enabled:
        await stop_event.wait()
        return
    while not stop_event.is_set():
        await asyncio.to_thread(run_due_jobs)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30)
        except TimeoutError:
            continue
