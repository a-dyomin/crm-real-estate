from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.models.enums import JobRunStatus


class ScheduledJobRead(BaseModel):
    id: int
    job_key: str
    name: str
    schedule_type: str
    schedule_hour: int | None
    schedule_minute: int | None
    interval_minutes: int | None
    timezone: str
    next_run_at: datetime | None
    last_run_at: datetime | None
    enabled: bool
    metadata_json: dict[str, Any] | None

    model_config = {"from_attributes": True}


class JobRunRead(BaseModel):
    id: int
    job_id: int | None
    job_key: str
    status: JobRunStatus
    scheduled_for: datetime | None
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None
    run_payload: dict[str, Any] | None

    model_config = {"from_attributes": True}


class AutonomySummaryRead(BaseModel):
    now_msk: datetime
    next_discovery_at: datetime | None
    next_parse_at: datetime | None
    last_discovery_run_at: datetime | None
    last_parse_run_at: datetime | None
    active_sources: int
    auto_activated_sources: int
    failed_sources: int
