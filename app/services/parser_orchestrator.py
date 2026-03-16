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

    if not sources:
        run.status = ParserRunStatus.completed
        run.finished_at = datetime.utcnow()
        db.flush()
        return run

    for source in sources:
        source.last_run_at = datetime.utcnow()
        try:
            items: list[ParserIngestItem] = collect_items_for_source(source)
            run.fetched_count += len(items)
            for item in items:
                result = ingest_parser_item(db=db, agency_id=agency_id, payload=item)
                if result.status == ParserResultStatus.duplicate:
                    run.duplicate_count += 1
                elif result.status == ParserResultStatus.possible_duplicate:
                    run.possible_duplicate_count += 1
                    run.inserted_count += 1
                else:
                    run.inserted_count += 1
            source.last_success_at = datetime.utcnow()
            source.last_error = None
        except Exception as exc:
            run.error_count += 1
            source.last_error = _truncate_error(exc)

    run.finished_at = datetime.utcnow()
    if run.error_count == 0:
        run.status = ParserRunStatus.completed
    elif run.error_count < run.source_count:
        run.status = ParserRunStatus.completed_with_errors
    else:
        run.status = ParserRunStatus.failed
        run.error_message = "All sources failed during parser run."

    db.flush()
    return run


def run_parser_for_all_agencies(db: Session, trigger: str = "scheduled") -> list[ParserRun]:
    agency_stmt = select(distinct(ParserSource.agency_id)).where(ParserSource.is_active.is_(True))
    agency_ids = [int(item[0]) for item in db.execute(agency_stmt).all()]
    runs: list[ParserRun] = []
    for agency_id in agency_ids:
        runs.append(run_parser_for_agency(db=db, agency_id=agency_id, trigger=trigger))
    return runs
