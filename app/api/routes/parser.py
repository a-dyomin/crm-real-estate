import math

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Select, func, or_, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.enums import ParserResultStatus, SourceChannel, UserRole
from app.models.parser_result import ParserResult
from app.models.parser_run import ParserRun
from app.models.parser_source import ParserSource
from app.models.user import User
from app.schemas.deal import DealRead
from app.schemas.lead import LeadRead
from app.schemas.parser import (
    ParserIngestRequest,
    ParserResultPageRead,
    ParserRunRead,
    ParserResultRead,
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

router = APIRouter(prefix="/parser", tags=["parser"])


@router.get(
    "/sources",
    response_model=list[ParserSourceRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
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
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.manager))],
)
def create_parser_source(
    payload: ParserSourceCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> ParserSource:
    if payload.source_channel == SourceChannel.manual:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Manual channel is not valid for auto source.")
    source = ParserSource(agency_id=current_user.agency_id, **payload.model_dump())
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
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.manager))],
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
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
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


@router.post(
    "/run-now",
    response_model=ParserRunRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
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
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
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
    q: str | None = Query(default=None, min_length=1, max_length=200),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=20, ge=1, le=100),
) -> ParserResultPageRead:
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(ParserResult.agency_id == current_user.agency_id)
    if status_filter:
        stmt = stmt.where(ParserResult.status == status_filter)
    if source_filter:
        stmt = stmt.where(ParserResult.source_channel == source_filter)
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

    paged_stmt = stmt.order_by(ParserResult.id.desc()).offset(offset).limit(page_size)
    items = db.execute(paged_stmt).scalars().all()
    return ParserResultPageRead(items=items, total=total, page=current_page, page_size=page_size, pages=pages)


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
