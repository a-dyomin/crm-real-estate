from __future__ import annotations

from datetime import datetime

import requests
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.enums import SourceHealthStatus, SourceState
from app.models.parser_source import ParserSource
from app.models.source_state_history import SourceStateHistory

settings = get_settings()


def _record_state_change(db: Session, source: ParserSource, new_state: SourceState, reason: str) -> None:
    if source.source_state == new_state:
        return
    db.add(
        SourceStateHistory(
            parser_source_id=source.id,
            from_state=source.source_state,
            to_state=new_state,
            reason=reason,
        )
    )
    source.source_state = new_state


def _check_url(url: str) -> SourceHealthStatus:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=min(10, settings.parser_request_timeout_sec),
        )
        if response.status_code in {401, 403, 429}:
            return SourceHealthStatus.blocked
        if 200 <= response.status_code < 400:
            return SourceHealthStatus.healthy
        return SourceHealthStatus.failed
    except Exception:
        return SourceHealthStatus.failed


def run_source_health_checks(db: Session, *, limit: int = 50) -> tuple[int, int]:
    stmt: Select[tuple[ParserSource]] = (
        select(ParserSource)
        .where(ParserSource.is_active.is_(True) | (ParserSource.source_state == SourceState.paused))
        .order_by(ParserSource.parse_priority.desc(), ParserSource.id.desc())
        .limit(limit)
    )
    sources = db.execute(stmt).scalars().all()
    checked = 0
    recovered = 0
    for source in sources:
        status = _check_url(source.source_url)
        checked += 1
        if status == SourceHealthStatus.healthy:
            source.health_status = status
            source.consecutive_success_count = int(source.consecutive_success_count or 0) + 1
            if source.source_state in {SourceState.paused, SourceState.error} and source.consecutive_success_count >= 2:
                source.is_active = True
                _record_state_change(db, source, SourceState.active, "Auto-resume after health check.")
                recovered += 1
        else:
            source.health_status = status
            source.consecutive_success_count = 0
        source.last_discovery_at = source.last_discovery_at or datetime.utcnow()
    return checked, recovered
