import math

from collections import Counter
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Select, func, or_, select, desc, nullslast
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.enums import ParserResultStatus, SourceChannel, SourceState, UserRole
from app.models.discovered_source import DiscoveredSource
from app.models.contact_identity import ContactIdentity
from app.models.contact_identity_link import ContactIdentityLink
from app.models.job_run import JobRun
from app.models.parser_result import ParserResult
from app.models.parser_run import ParserRun
from app.models.parser_source import ParserSource
from app.models.parser_template import ParserTemplate
from app.models.scheduled_job import ScheduledJob
from app.models.source_discovery_run import SourceDiscoveryRun
from app.models.source_health_check import SourceHealthCheck
from app.models.source_parse_run import SourceParseRun
from app.models.source_seed import SourceSeed
from app.models.user import User
from app.schemas.autonomy import AutonomySummaryRead, JobRunRead, ScheduledJobRead
from app.schemas.discovery import (
    DiscoveredSourceRead,
    SourceDiscoveryRunRead,
    SourceSeedCreate,
    SourceSeedRead,
    SourceSeedUpdate,
)
from app.schemas.deal import DealRead
from app.schemas.lead import LeadRead
from app.schemas.owner_intel import ContactIdentityDetailRead, ContactIdentityListingRead, ContactIdentityRead
from app.schemas.parser import (
    ParserIngestRequest,
    ParserResultPageRead,
    ParserRunRead,
    ParserResultRead,
    SourceParseRunRead,
    ParserSourceCreate,
    ParserSourceRead,
    ParserSourceUpdate,
    ParserToDealRequest,
    ParserToLeadRequest,
)
from app.services.conversion import parser_result_to_deal, parser_result_to_lead
from app.services.audit import write_audit_log
from app.services.parser_ingest import ingest_parser_item
from app.services.parser_orchestrator import run_parser_for_agency
from app.services.source_discovery import run_source_discovery
from app.services.owner_intelligence import refresh_owner_intelligence

router = APIRouter(prefix="/parser", tags=["parser"])

MSK_TZ = ZoneInfo("Europe/Moscow")


@router.get(
    "/discovery/seeds",
    response_model=list[SourceSeedRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_discovery_seeds(db: Session = Depends(get_db)) -> list[SourceSeed]:
    stmt: Select[tuple[SourceSeed]] = select(SourceSeed).order_by(SourceSeed.priority.desc(), SourceSeed.id.desc())
    return db.execute(stmt).scalars().all()


@router.post(
    "/discovery/seeds",
    response_model=SourceSeedRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def create_discovery_seed(payload: SourceSeedCreate, db: Session = Depends(get_db)) -> SourceSeed:
    seed = SourceSeed(**payload.model_dump())
    db.add(seed)
    db.commit()
    db.refresh(seed)
    return seed


@router.patch(
    "/discovery/seeds/{seed_id}",
    response_model=SourceSeedRead,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def update_discovery_seed(
    seed_id: int,
    payload: SourceSeedUpdate,
    db: Session = Depends(get_db),
) -> SourceSeed:
    seed = db.get(SourceSeed, seed_id)
    if not seed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Seed not found.")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(seed, field, value)
    db.commit()
    db.refresh(seed)
    return seed


@router.get(
    "/discovery/sources",
    response_model=list[DiscoveredSourceRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_discovered_sources(
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> list[DiscoveredSource]:
    stmt: Select[tuple[DiscoveredSource]] = (
        select(DiscoveredSource).order_by(DiscoveredSource.relevance_score.desc()).limit(limit)
    )
    sources = db.execute(stmt).scalars().all()
    if not sources:
        return []
    template_map = {template.id: template for template in db.execute(select(ParserTemplate)).scalars().all()}
    source_ids = [source.id for source in sources]
    checks = (
        db.execute(
            select(SourceHealthCheck)
            .where(SourceHealthCheck.discovered_source_id.in_(source_ids))
            .order_by(SourceHealthCheck.checked_at.desc())
        )
        .scalars()
        .all()
    )
    latest_health: dict[int, SourceHealthCheck] = {}
    for check in checks:
        if check.discovered_source_id not in latest_health:
            latest_health[check.discovered_source_id] = check
    for source in sources:
        template = template_map.get(source.parser_template_id)
        source.parser_template_key = template.key if template else None
        health = latest_health.get(source.id)
        source.health_status = health.status if health else None
    return sources


@router.get(
    "/discovery/runs",
    response_model=list[SourceDiscoveryRunRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_discovery_runs(
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[SourceDiscoveryRun]:
    stmt: Select[tuple[SourceDiscoveryRun]] = (
        select(SourceDiscoveryRun).order_by(SourceDiscoveryRun.started_at.desc()).limit(limit)
    )
    return db.execute(stmt).scalars().all()


@router.post(
    "/discovery/run",
    response_model=SourceDiscoveryRunRead,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def run_discovery_now(db: Session = Depends(get_db)) -> SourceDiscoveryRun:
    run = run_source_discovery(db=db)
    db.commit()
    db.refresh(run)
    return run


@router.get(
    "/autonomy/summary",
    response_model=AutonomySummaryRead,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def get_autonomy_summary(db: Session = Depends(get_db)) -> AutonomySummaryRead:
    now_msk = datetime.now(MSK_TZ)
    jobs = db.execute(select(ScheduledJob)).scalars().all()
    job_map = {job.job_key: job for job in jobs}
    discovery_job = job_map.get("daily_discovery_job")
    parse_job = job_map.get("daily_active_source_parse_job")

    last_discovery = (
        db.execute(
            select(JobRun)
            .where(JobRun.job_key == "daily_discovery_job")
            .order_by(JobRun.started_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )
    last_parse = (
        db.execute(
            select(JobRun)
            .where(JobRun.job_key == "daily_active_source_parse_job")
            .order_by(JobRun.started_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )

    active_sources = (
        db.execute(select(func.count()).select_from(ParserSource).where(ParserSource.source_state == SourceState.active))
        .scalar()
        or 0
    )
    auto_activated = (
        db.execute(
            select(func.count())
            .select_from(ParserSource)
            .where(ParserSource.auto_discovered.is_(True))
        ).scalar()
        or 0
    )
    failed_sources = (
        db.execute(
            select(func.count())
            .select_from(ParserSource)
            .where(ParserSource.source_state.in_([SourceState.paused, SourceState.error]))
        ).scalar()
        or 0
    )
    return AutonomySummaryRead(
        now_msk=now_msk,
        next_discovery_at=discovery_job.next_run_at if discovery_job else None,
        next_parse_at=parse_job.next_run_at if parse_job else None,
        last_discovery_run_at=last_discovery.finished_at if last_discovery else None,
        last_parse_run_at=last_parse.finished_at if last_parse else None,
        active_sources=int(active_sources),
        auto_activated_sources=int(auto_activated),
        failed_sources=int(failed_sources),
    )


@router.get(
    "/autonomy/jobs",
    response_model=list[ScheduledJobRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_scheduled_jobs(db: Session = Depends(get_db)) -> list[ScheduledJob]:
    stmt: Select[tuple[ScheduledJob]] = select(ScheduledJob).order_by(ScheduledJob.job_key.asc())
    return db.execute(stmt).scalars().all()


@router.get(
    "/autonomy/job-runs",
    response_model=list[JobRunRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_job_runs(
    limit: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
) -> list[JobRun]:
    stmt: Select[tuple[JobRun]] = select(JobRun).order_by(JobRun.started_at.desc()).limit(limit)
    return db.execute(stmt).scalars().all()


@router.get(
    "/autonomy/sources",
    response_model=list[ParserSourceRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_autonomy_sources(
    state_filter: SourceState | None = Query(default=None, alias="state"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> list[ParserSource]:
    stmt: Select[tuple[ParserSource]] = select(ParserSource)
    if state_filter:
        stmt = stmt.where(ParserSource.source_state == state_filter)
    stmt = stmt.order_by(ParserSource.parse_priority.desc(), ParserSource.id.desc()).limit(limit)
    return db.execute(stmt).scalars().all()


@router.get(
    "/sources",
    response_model=list[ParserSourceRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_parser_sources(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[ParserSource]:
    stmt: Select[tuple[ParserSource]] = (
        select(ParserSource).where(ParserSource.agency_id == current_user.agency_id).order_by(ParserSource.id.desc())
    )
    return db.execute(stmt).scalars().all()


@router.post(
    "/sources",
    response_model=ParserSourceRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def create_parser_source(
    payload: ParserSourceCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> ParserSource:
    if payload.source_channel == SourceChannel.manual:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Manual channel is not valid for auto source.")
    data = payload.model_dump()
    if data.get("parse_frequency_minutes") is None:
        data["parse_frequency_minutes"] = data.get("poll_minutes") or 1440
    if data.get("parse_priority") is None:
        data["parse_priority"] = 50
    source = ParserSource(agency_id=current_user.agency_id, **data)
    db.add(source)
    db.flush()
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.source_create",
        entity_type="parser_source",
        entity_id=str(source.id),
        details={"name": source.name, "channel": source.source_channel.value},
    )
    db.commit()
    db.refresh(source)
    return source


@router.patch(
    "/sources/{source_id}",
    response_model=ParserSourceRead,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def update_parser_source(
    source_id: int,
    payload: ParserSourceUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ParserSource:
    stmt: Select[tuple[ParserSource]] = select(ParserSource).where(
        ParserSource.id == source_id, ParserSource.agency_id == current_user.agency_id
    )
    source = db.execute(stmt).scalar_one_or_none()
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parser source not found.")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(source, field, value)
    if source.parse_frequency_minutes is None:
        source.parse_frequency_minutes = source.poll_minutes
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.source_update",
        entity_type="parser_source",
        entity_id=str(source.id),
    )
    db.commit()
    db.refresh(source)
    return source


@router.get(
    "/runs",
    response_model=list[ParserRunRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_parser_runs(
    limit: int = Query(default=20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[ParserRun]:
    stmt: Select[tuple[ParserRun]] = (
        select(ParserRun)
        .where(ParserRun.agency_id == current_user.agency_id)
        .order_by(ParserRun.started_at.desc())
        .limit(limit)
    )
    return db.execute(stmt).scalars().all()


@router.get(
    "/source-runs",
    response_model=list[SourceParseRunRead],
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def list_source_runs(
    limit: int = Query(default=40, ge=1, le=200),
    source_id: int | None = Query(default=None, ge=1),
    db: Session = Depends(get_db),
) -> list[SourceParseRun]:
    stmt: Select[tuple[SourceParseRun]] = select(SourceParseRun).order_by(SourceParseRun.started_at.desc())
    if source_id:
        stmt = stmt.where(SourceParseRun.parser_source_id == source_id)
    stmt = stmt.limit(limit)
    return db.execute(stmt).scalars().all()


@router.post(
    "/owner-contacts/refresh",
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def refresh_owner_contacts(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> dict[str, int]:
    created = refresh_owner_intelligence(db, current_user.agency_id)
    return {"identities": created}


@router.get(
    "/owner-contacts",
    response_model=list[ContactIdentityRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager, UserRole.agent))],
)
def list_owner_contacts(
    min_owner_score: float | None = Query(default=None, ge=0, le=100),
    only_owner: bool = Query(default=True),
    only_single_listing: bool = Query(default=False),
    only_new_days: int | None = Query(default=None, ge=1, le=365),
    only_low_competition: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[ContactIdentity]:
    stmt: Select[tuple[ContactIdentity]] = select(ContactIdentity).where(
        ContactIdentity.agency_id == current_user.agency_id
    )
    if only_owner:
        stmt = stmt.where(ContactIdentity.final_class == "owner_candidate")
    if min_owner_score is not None:
        stmt = stmt.where(ContactIdentity.owner_probability >= min_owner_score)
    if only_single_listing:
        stmt = stmt.where(ContactIdentity.total_listings == 1)
    if only_low_competition:
        stmt = stmt.where(ContactIdentity.repost_rate <= 0.2)
    if only_new_days:
        since = datetime.utcnow() - timedelta(days=only_new_days)
        stmt = stmt.where(ContactIdentity.last_seen_at >= since)
    stmt = stmt.order_by(
        ContactIdentity.owner_priority_score.desc().nullslast(),
        ContactIdentity.owner_probability.desc(),
        ContactIdentity.last_seen_at.desc(),
    ).limit(limit)
    return db.execute(stmt).scalars().all()


@router.get(
    "/owner-contacts/{contact_id}",
    response_model=ContactIdentityDetailRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager, UserRole.agent))],
)
def get_owner_contact_detail(
    contact_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> ContactIdentityDetailRead:
    identity = db.get(ContactIdentity, contact_id)
    if not identity or identity.agency_id != current_user.agency_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Owner contact not found.")

    link_stmt: Select[tuple[ContactIdentityLink]] = select(ContactIdentityLink).where(
        ContactIdentityLink.contact_identity_id == identity.id
    )
    links = db.execute(link_stmt).scalars().all()
    result_ids = [link.parser_result_id for link in links]
    listings: list[ContactIdentityListingRead] = []
    objects: dict[str, dict[str, Any]] = {}
    timeline: Counter[str] = Counter()

    if result_ids:
        results = (
            db.execute(
                select(ParserResult).where(ParserResult.id.in_(result_ids)).order_by(ParserResult.updated_at.desc())
            )
            .scalars()
            .all()
        )
        for result in results:
            listings.append(
                ContactIdentityListingRead(
                    id=result.id,
                    title=result.title,
                    raw_url=result.raw_url,
                    source_channel=result.source_channel.value,
                    status=result.status.value,
                    updated_at=result.updated_at,
                    normalized_address=result.normalized_address,
                    price_rub=float(result.price_rub) if result.price_rub is not None else None,
                    area_sqm=float(result.area_sqm) if result.area_sqm is not None else None,
                    listing_type=result.listing_type,
                )
            )
            object_key = result.fingerprint or f"{result.normalized_address}|{result.area_sqm}|{result.listing_type}"
            obj = objects.get(object_key)
            if not obj:
                obj = {
                    "address": result.normalized_address,
                    "area_sqm": float(result.area_sqm) if result.area_sqm is not None else None,
                    "listing_type": result.listing_type,
                    "count": 0,
                    "last_seen_at": result.updated_at,
                }
                objects[object_key] = obj
            obj["count"] += 1
            if result.updated_at and (obj["last_seen_at"] is None or result.updated_at > obj["last_seen_at"]):
                obj["last_seen_at"] = result.updated_at
            if result.updated_at:
                timeline[result.updated_at.strftime("%Y-%m-%d")] += 1

    activity = [{"date": date, "count": count} for date, count in sorted(timeline.items(), reverse=True)[:30]]

    return ContactIdentityDetailRead(
        identity=identity,
        listings=listings,
        objects=list(objects.values()),
        activity_timeline=activity,
        explanation=identity.explanation,
    )


@router.post(
    "/run-now",
    response_model=ParserRunRead,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def run_parser_now(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> ParserRun:
    run = run_parser_for_agency(db=db, agency_id=current_user.agency_id, trigger="manual")
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.run_now",
        entity_type="parser_run",
        entity_id=str(run.id),
        details={
            "status": run.status.value,
            "fetched": run.fetched_count,
            "inserted": run.inserted_count,
            "errors": run.error_count,
        },
    )
    db.commit()
    db.refresh(run)
    return run


@router.post(
    "/ingest",
    response_model=list[ParserResultRead],
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def ingest_parser_batch(
    payload: ParserIngestRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[ParserResult]:
    created: list[ParserResult] = []
    for item in payload.items:
        created.append(ingest_parser_item(db=db, agency_id=current_user.agency_id, payload=item))
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.ingest",
        entity_type="parser_result",
        message="Parser batch ingest completed.",
        details={"count": len(created)},
    )
    db.commit()
    for item in created:
        db.refresh(item)
    return created


@router.get("/results", response_model=ParserResultPageRead)
def list_parser_results(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    status_filter: ParserResultStatus | None = Query(default=None, alias="status"),
    source_filter: SourceChannel | None = Query(default=None, alias="source"),
    deal_type: str | None = Query(default=None, alias="deal_type"),
    region: str | None = Query(default=None, alias="region"),
    updated_from: str | None = Query(default=None, alias="updated_from"),
    updated_to: str | None = Query(default=None, alias="updated_to"),
    duplicates_only: bool = Query(default=False, alias="duplicates_only"),
    q: str | None = Query(default=None, min_length=1, max_length=200),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=20, ge=1, le=100),
) -> ParserResultPageRead:
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(ParserResult.agency_id == current_user.agency_id)

    def _parse_datetime(value: str, *, end_of_day: bool = False) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if end_of_day and len(value) == 10:
            return parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
        return parsed

    if status_filter:
        stmt = stmt.where(ParserResult.status == status_filter)
    if source_filter:
        stmt = stmt.where(ParserResult.source_channel == source_filter)
    if deal_type:
        stmt = stmt.where(ParserResult.listing_type == deal_type)
    if duplicates_only:
        stmt = stmt.where(ParserResult.status.in_([ParserResultStatus.duplicate, ParserResultStatus.possible_duplicate]))
    if region:
        stmt = stmt.where(or_(ParserResult.region_code == region, ParserResult.city.ilike(f"%{region}%")))
    if updated_from:
        parsed_from = _parse_datetime(updated_from)
        if parsed_from:
            stmt = stmt.where(ParserResult.updated_at >= parsed_from)
    if updated_to:
        parsed_to = _parse_datetime(updated_to, end_of_day=True)
        if parsed_to:
            stmt = stmt.where(ParserResult.updated_at <= parsed_to)
    normalized_query = (q or "").strip()
    if normalized_query:
        search_pattern = f"%{normalized_query}%"
        stmt = stmt.where(
            or_(
                ParserResult.title.ilike(search_pattern),
                ParserResult.description.ilike(search_pattern),
                ParserResult.listing_type.ilike(search_pattern),
                ParserResult.normalized_address.ilike(search_pattern),
                ParserResult.address_district.ilike(search_pattern),
                ParserResult.address_street.ilike(search_pattern),
                ParserResult.city.ilike(search_pattern),
                ParserResult.contact_name.ilike(search_pattern),
                ParserResult.contact_phone.ilike(search_pattern),
                ParserResult.contact_email.ilike(search_pattern),
                ParserResult.raw_url.ilike(search_pattern),
                ParserResult.telegram_post_url.ilike(search_pattern),
            )
        )

    total_stmt = select(func.count()).select_from(stmt.subquery())
    total = int(db.execute(total_stmt).scalar() or 0)
    pages = max(1, math.ceil(total / page_size)) if total else 1
    current_page = min(page, pages)
    offset = (current_page - 1) * page_size

    paged_stmt = (
        stmt.order_by(nullslast(desc(ParserResult.lead_score)), ParserResult.updated_at.desc(), ParserResult.id.desc())
        .offset(offset)
        .limit(page_size)
    )
    items = db.execute(paged_stmt).scalars().all()
    return ParserResultPageRead(items=items, total=total, page=current_page, page_size=page_size, pages=pages)


@router.get(
    "/results/{result_id}/duplicates",
    response_model=list[ParserResultRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def list_parser_duplicates(
    result_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> list[ParserResult]:
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
        ParserResult.id == result_id, ParserResult.agency_id == current_user.agency_id
    )
    result = db.execute(stmt).scalar_one_or_none()
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parser result not found.")
    criteria = [ParserResult.duplicate_of_id == result.id]
    if result.duplicate_of_id:
        criteria.append(ParserResult.id == result.duplicate_of_id)
    if result.fingerprint:
        criteria.append(ParserResult.fingerprint == result.fingerprint)
    if not criteria:
        return []
    dup_stmt: Select[tuple[ParserResult]] = (
        select(ParserResult)
        .where(ParserResult.agency_id == current_user.agency_id, ParserResult.id != result.id, or_(*criteria))
        .order_by(ParserResult.updated_at.desc())
        .limit(25)
    )
    return db.execute(dup_stmt).scalars().all()


@router.post(
    "/results/{result_id}/to-lead",
    response_model=LeadRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def parser_to_lead(
    result_id: int,
    payload: ParserToLeadRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
        ParserResult.id == result_id, ParserResult.agency_id == current_user.agency_id
    )
    parser_result = db.execute(stmt).scalar_one_or_none()
    if not parser_result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parser result not found.")
    lead = parser_result_to_lead(
        db=db,
        parser_result=parser_result,
        title=payload.title,
        owner_user_id=payload.owner_user_id,
    )
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.to_lead",
        entity_type="lead",
        entity_id=str(lead.id),
        details={"parser_result_id": parser_result.id},
    )
    db.commit()
    db.refresh(lead)
    return lead


@router.post(
    "/results/{result_id}/to-deal",
    response_model=DealRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.sales, UserRole.manager))],
)
def parser_to_deal(
    result_id: int,
    payload: ParserToDealRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
        ParserResult.id == result_id, ParserResult.agency_id == current_user.agency_id
    )
    parser_result = db.execute(stmt).scalar_one_or_none()
    if not parser_result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parser result not found.")
    deal = parser_result_to_deal(
        db=db,
        parser_result=parser_result,
        title=payload.title,
        owner_user_id=payload.owner_user_id,
        value_rub=payload.value_rub,
    )
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.to_deal",
        entity_type="deal",
        entity_id=str(deal.id),
        details={"parser_result_id": parser_result.id},
    )
    db.commit()
    db.refresh(deal)
    return deal


@router.post(
    "/results/{result_id}/reject",
    response_model=ParserResultRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def reject_parser_result(
    result_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> ParserResult:
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
        ParserResult.id == result_id, ParserResult.agency_id == current_user.agency_id
    )
    parser_result = db.execute(stmt).scalar_one_or_none()
    if not parser_result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Parser result not found.")
    parser_result.status = ParserResultStatus.rejected
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="parser.reject",
        entity_type="parser_result",
        entity_id=str(parser_result.id),
    )
    db.commit()
    db.refresh(parser_result)
    return parser_result
