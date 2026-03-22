from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.models.job_run import JobRun
from app.models.parser_source import ParserSource
from app.models.scheduled_job import ScheduledJob
from app.services.parser_orchestrator import run_parser_for_all_agencies
from app.services.source_discovery import auto_activate_sources, run_source_discovery
from app.services.source_health import run_source_health_checks


def trigger_job(db: Session, job_key: str, run_at: datetime | None = None) -> None:
    stmt: Select[tuple[ScheduledJob]] = select(ScheduledJob).where(ScheduledJob.job_key == job_key)
    job = db.execute(stmt).scalar_one_or_none()
    if not job:
        return
    job.next_run_at = run_at or datetime.utcnow()


def run_daily_discovery_job(db: Session, job_run: JobRun) -> dict[str, Any]:
    discovery_run = run_source_discovery(db=db, auto_mode=True)
    activated = auto_activate_sources(db=db)
    discovery_run.activated_count = activated
    trigger_job(db, "daily_active_source_parse_job", run_at=datetime.utcnow() + timedelta(minutes=1))
    return {
        "discovery_run_id": discovery_run.id,
        "activated_sources": activated,
    }


def run_daily_parse_job(db: Session, job_run: JobRun) -> dict[str, Any]:
    runs = run_parser_for_all_agencies(db=db, trigger="scheduled_daily", job_run_id=job_run.id)
    total_sources = sum(run.source_count for run in runs)
    total_inserted = sum(run.inserted_count for run in runs)
    total_errors = sum(run.error_count for run in runs)
    return {
        "parser_runs": [run.id for run in runs],
        "sources": total_sources,
        "inserted": total_inserted,
        "errors": total_errors,
    }


def run_high_priority_refresh(db: Session, job_run: JobRun) -> dict[str, Any]:
    runs = run_parser_for_all_agencies(db=db, trigger="scheduled_high_priority", job_run_id=job_run.id)
    total_sources = sum(run.source_count for run in runs)
    return {"parser_runs": [run.id for run in runs], "sources": total_sources}


def run_periodic_health_check(db: Session, job_run: JobRun) -> dict[str, Any]:
    checked, recovered = run_source_health_checks(db=db)
    return {"checked": checked, "recovered": recovered}


def run_retry_failed_jobs(db: Session, job_run: JobRun) -> dict[str, Any]:
    now = datetime.utcnow()
    jobs = (
        db.execute(select(ScheduledJob).where(ScheduledJob.enabled.is_(True))).scalars().all()
    )
    retried = 0
    for job in jobs:
        if not job.metadata_json:
            continue
        failure_count = int(job.metadata_json.get("failure_count", 0))
        if failure_count <= 0:
            continue
        if job.next_run_at and job.next_run_at > now:
            continue
        backoff_minutes = min(360, 5 * (2 ** min(failure_count, 5)))
        job.next_run_at = now + timedelta(minutes=backoff_minutes)
        retried += 1
    return {"retried": retried}


JOB_REGISTRY = {
    "daily_discovery_job": run_daily_discovery_job,
    "daily_active_source_parse_job": run_daily_parse_job,
    "periodic_source_health_check": run_periodic_health_check,
    "retry_failed_jobs": run_retry_failed_jobs,
    "high_priority_source_refresh": run_high_priority_refresh,
}
