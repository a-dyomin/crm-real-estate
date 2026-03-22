import copy
from datetime import datetime, timedelta

from sqlalchemy import Select, distinct, select
from sqlalchemy.orm import Session

from app.models.enums import (
    ParserResultStatus,
    ParserRunStatus,
    SourceHealthStatus,
    SourceParseStatus,
    SourceState,
)
from app.models.parser_run import ParserRun
from app.models.parser_source import ParserSource
from app.models.source_parse_run import SourceParseRun
from app.models.source_state_history import SourceStateHistory
from app.schemas.parser import ParserIngestItem
from app.services.parser_collectors import collect_items_for_source
from app.services.parser_ingest import ingest_parser_item
from app.services.owner_intelligence import refresh_owner_intelligence
from app.services.market_benchmarks import recompute_market_benchmarks
from app.services.parser_scoring import update_parser_scores


def _truncate_error(error: Exception) -> str:
    message = f"{type(error).__name__}: {error}"
    return message[:1500]


def _snapshot_source(source: ParserSource) -> ParserSource:
    return ParserSource(
        id=source.id,
        agency_id=source.agency_id,
        name=source.name,
        source_channel=source.source_channel,
        source_url=source.source_url,
        city=source.city,
        region_code=source.region_code,
        is_active=source.is_active,
        source_state=source.source_state,
        activation_mode=source.activation_mode,
        auto_discovered=source.auto_discovered,
        poll_minutes=source.poll_minutes,
        parse_frequency_minutes=source.parse_frequency_minutes,
        parse_priority=source.parse_priority,
        max_items_per_run=source.max_items_per_run,
        extra_config=copy.deepcopy(source.extra_config) if source.extra_config else None,
        last_discovery_at=source.last_discovery_at,
        last_parsed_at=source.last_parsed_at,
        next_scheduled_parse_at=source.next_scheduled_parse_at,
        health_status=source.health_status,
        failure_count=source.failure_count,
        consecutive_success_count=source.consecutive_success_count,
        scheduler_lock_key=source.scheduler_lock_key,
        auto_activation_reason=source.auto_activation_reason,
        last_run_at=source.last_run_at,
        last_success_at=source.last_success_at,
        last_error=source.last_error,
        created_at=source.created_at,
        updated_at=source.updated_at,
    )


def _record_state_change(db: Session, source: ParserSource, new_state: SourceState, reason: str | None = None) -> None:
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


def _next_parse_at(source: ParserSource, now: datetime, *, failure_count: int | None = None) -> datetime:
    base_minutes = int(source.parse_frequency_minutes or source.poll_minutes or 1440)
    if failure_count is None:
        failure_count = source.failure_count or 0
    backoff_multiplier = 2 ** min(failure_count, 4)
    delay_minutes = max(15, base_minutes * backoff_multiplier)
    return now + timedelta(minutes=delay_minutes)


def run_parser_for_agency(
    db: Session,
    agency_id: int,
    trigger: str = "scheduled",
    job_run_id: int | None = None,
) -> ParserRun:
    source_stmt: Select[tuple[ParserSource]] = (
        select(ParserSource)
        .where(
            ParserSource.agency_id == agency_id,
            ParserSource.is_active.is_(True),
            ParserSource.source_state == SourceState.active,
        )
        .order_by(ParserSource.id.asc())
    )
    sources_all = db.execute(source_stmt).scalars().all()
    now = datetime.utcnow()
    if trigger.startswith("scheduled"):
        sources: list[ParserSource] = []
        for source in sources_all:
            if trigger == "scheduled_high_priority" and (source.parse_priority or 0) < 80:
                continue
            due_at = source.next_scheduled_parse_at
            if due_at and now < due_at:
                continue
            sources.append(source)
    else:
        sources = sources_all

    run = ParserRun(
        agency_id=agency_id,
        status=ParserRunStatus.running,
        trigger=trigger,
        source_count=len(sources),
    )
    db.add(run)
    db.flush()
    run_id = run.id
    db.commit()

    if not sources:
        run = db.get(ParserRun, run_id)
        if run:
            run.status = ParserRunStatus.completed
            run.finished_at = datetime.utcnow()
            db.commit()
            db.refresh(run)
            return run
        return ParserRun(
            agency_id=agency_id,
            status=ParserRunStatus.completed,
            trigger=trigger,
            source_count=0,
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
        )

    counters = {
        "fetched": 0,
        "inserted": 0,
        "duplicate": 0,
        "possible_duplicate": 0,
        "errors": 0,
        "contacts_extracted": 0,
        "contacts_rejected": 0,
        "leads_published": 0,
        "listings_parsed": 0,
    }

    for source in sources:
        source_id = source.id
        snapshot = _snapshot_source(source)
        source_row = db.get(ParserSource, source_id)
        if source_row:
            source_row.last_run_at = datetime.utcnow()
            db.commit()
        parse_run: SourceParseRun | None = None
        source_counts = {
            "fetched": 0,
            "inserted": 0,
            "duplicate": 0,
            "possible_duplicate": 0,
            "errors": 0,
            "contacts_extracted": 0,
            "contacts_rejected": 0,
            "leads_published": 0,
            "listings_parsed": 0,
        }
        try:
            parse_run = SourceParseRun(
                parser_source_id=source_id,
                job_run_id=job_run_id,
                status=SourceParseStatus.running,
            )
            db.add(parse_run)
            db.commit()
            items: list[ParserIngestItem] = collect_items_for_source(snapshot)
            counters["fetched"] += len(items)
            source_counts["fetched"] = len(items)
            counters["listings_parsed"] += len(items)
            source_counts["listings_parsed"] = len(items)
            for item in items:
                candidate_count = len(item.contact_candidates or [])
                rejected_count = len(item.rejected_contacts or [])
                counters["contacts_extracted"] += candidate_count
                source_counts["contacts_extracted"] += candidate_count
                counters["contacts_rejected"] += rejected_count
                source_counts["contacts_rejected"] += rejected_count
                result = ingest_parser_item(db=db, agency_id=agency_id, payload=item)
                if result.status == ParserResultStatus.duplicate:
                    counters["duplicate"] += 1
                    source_counts["duplicate"] += 1
                elif result.status == ParserResultStatus.possible_duplicate:
                    counters["possible_duplicate"] += 1
                    counters["inserted"] += 1
                    source_counts["possible_duplicate"] += 1
                    source_counts["inserted"] += 1
                    counters["leads_published"] += 1
                    source_counts["leads_published"] += 1
                else:
                    counters["inserted"] += 1
                    source_counts["inserted"] += 1
                    counters["leads_published"] += 1
                    source_counts["leads_published"] += 1
            if source_row:
                source_row.last_fetch_at = datetime.utcnow()
                source_row.last_success_at = datetime.utcnow()
                source_row.last_error = None
                source_row.last_error_at = None
                source_row.last_parsed_at = datetime.utcnow()
                source_row.health_status = SourceHealthStatus.healthy
                source_row.failure_count = 0
                source_row.consecutive_success_count = (source_row.consecutive_success_count or 0) + 1
                source_row.next_scheduled_parse_at = _next_parse_at(source_row, datetime.utcnow(), failure_count=0)
                source_row.listings_parsed_last_run = source_counts["listings_parsed"]
                source_row.contacts_extracted_last_run = source_counts["contacts_extracted"]
                source_row.contacts_rejected_last_run = source_counts["contacts_rejected"]
                source_row.leads_published_last_run = source_counts["leads_published"]
            if parse_run:
                parse_run.fetched_count = source_counts["fetched"]
                parse_run.listings_parsed = source_counts["listings_parsed"]
                parse_run.inserted_count = source_counts["inserted"]
                parse_run.duplicate_count = source_counts["duplicate"]
                parse_run.possible_duplicate_count = source_counts["possible_duplicate"]
                parse_run.contacts_extracted = source_counts["contacts_extracted"]
                parse_run.contacts_rejected = source_counts["contacts_rejected"]
                parse_run.leads_published = source_counts["leads_published"]
                parse_run.status = SourceParseStatus.completed
                parse_run.finished_at = datetime.utcnow()
        except Exception as exc:
            counters["errors"] += 1
            source_counts["errors"] += 1
            if source_row:
                source_row.last_error = _truncate_error(exc)
                source_row.last_error_at = datetime.utcnow()
                source_row.last_parsed_at = datetime.utcnow()
                source_row.failure_count = int(source_row.failure_count or 0) + 1
                source_row.consecutive_success_count = 0
                source_row.health_status = SourceHealthStatus.degraded
                source_row.next_scheduled_parse_at = _next_parse_at(source_row, datetime.utcnow())
                if source_row.failure_count >= 3:
                    source_row.is_active = False
                    _record_state_change(
                        db, source_row, SourceState.paused, reason="Auto-paused after repeated failures."
                    )
            if parse_run:
                parse_run.status = SourceParseStatus.failed
                parse_run.error_message = _truncate_error(exc)
                parse_run.finished_at = datetime.utcnow()

        run_row = db.get(ParserRun, run_id)
        if run_row:
            run_row.fetched_count = counters["fetched"]
            run_row.inserted_count = counters["inserted"]
            run_row.duplicate_count = counters["duplicate"]
            run_row.possible_duplicate_count = counters["possible_duplicate"]
            run_row.error_count = counters["errors"]
        db.commit()

    run_row = db.get(ParserRun, run_id)
    if not run_row:
        return run
    run_row.finished_at = datetime.utcnow()
    if counters["errors"] == 0:
        run_row.status = ParserRunStatus.completed
    elif counters["errors"] < run_row.source_count:
        run_row.status = ParserRunStatus.completed_with_errors
    else:
        run_row.status = ParserRunStatus.failed
        run_row.error_message = "All sources failed during parser run."

    db.commit()
    try:
        refresh_owner_intelligence(db, agency_id)
        recompute_market_benchmarks(db, agency_id)
        update_parser_scores(db, agency_id)
    except Exception as exc:
        if run_row:
            run_row.status = ParserRunStatus.completed_with_errors
            if not run_row.error_message:
                run_row.error_message = f"Post-processing failed: {type(exc).__name__}"
            db.commit()
    db.refresh(run_row)
    return run_row


def run_parser_for_all_agencies(
    db: Session,
    trigger: str = "scheduled",
    job_run_id: int | None = None,
) -> list[ParserRun]:
    agency_stmt = select(distinct(ParserSource.agency_id)).where(ParserSource.is_active.is_(True))
    agency_ids = [int(item[0]) for item in db.execute(agency_stmt).all()]
    runs: list[ParserRun] = []
    for agency_id in agency_ids:
        runs.append(run_parser_for_agency(db=db, agency_id=agency_id, trigger=trigger, job_run_id=job_run_id))
    return runs
