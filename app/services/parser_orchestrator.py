import copy
from datetime import datetime

from sqlalchemy import Select, distinct, select
from sqlalchemy.orm import Session

from app.models.enums import ParserResultStatus, ParserRunStatus
from app.models.parser_run import ParserRun
from app.models.parser_source import ParserSource
from app.schemas.parser import ParserIngestItem
from app.services.parser_collectors import collect_items_for_source
from app.services.parser_ingest import ingest_parser_item


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
        poll_minutes=source.poll_minutes,
        max_items_per_run=source.max_items_per_run,
        extra_config=copy.deepcopy(source.extra_config) if source.extra_config else None,
        last_run_at=source.last_run_at,
        last_success_at=source.last_success_at,
        last_error=source.last_error,
        created_at=source.created_at,
        updated_at=source.updated_at,
    )


def run_parser_for_agency(db: Session, agency_id: int, trigger: str = "scheduled") -> ParserRun:
    source_stmt: Select[tuple[ParserSource]] = (
        select(ParserSource)
        .where(ParserSource.agency_id == agency_id, ParserSource.is_active.is_(True))
        .order_by(ParserSource.id.asc())
    )
    sources_all = db.execute(source_stmt).scalars().all()
    now = datetime.utcnow()
    if trigger == "scheduled":
        sources = [
            source
            for source in sources_all
            if not source.last_run_at
            or (now - source.last_run_at).total_seconds() >= max(300, source.poll_minutes * 60)
        ]
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
    }

    for source in sources:
        source_id = source.id
        snapshot = _snapshot_source(source)
        source_row = db.get(ParserSource, source_id)
        if source_row:
            source_row.last_run_at = datetime.utcnow()
            db.commit()
        try:
            items: list[ParserIngestItem] = collect_items_for_source(snapshot)
            counters["fetched"] += len(items)
            for item in items:
                result = ingest_parser_item(db=db, agency_id=agency_id, payload=item)
                if result.status == ParserResultStatus.duplicate:
                    counters["duplicate"] += 1
                elif result.status == ParserResultStatus.possible_duplicate:
                    counters["possible_duplicate"] += 1
                    counters["inserted"] += 1
                else:
                    counters["inserted"] += 1
            if source_row:
                source_row.last_success_at = datetime.utcnow()
                source_row.last_error = None
        except Exception as exc:
            counters["errors"] += 1
            if source_row:
                source_row.last_error = _truncate_error(exc)

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
    db.refresh(run_row)
    return run_row


def run_parser_for_all_agencies(db: Session, trigger: str = "scheduled") -> list[ParserRun]:
    agency_stmt = select(distinct(ParserSource.agency_id)).where(ParserSource.is_active.is_(True))
    agency_ids = [int(item[0]) for item in db.execute(agency_stmt).all()]
    runs: list[ParserRun] = []
    for agency_id in agency_ids:
        runs.append(run_parser_for_agency(db=db, agency_id=agency_id, trigger=trigger))
    return runs
