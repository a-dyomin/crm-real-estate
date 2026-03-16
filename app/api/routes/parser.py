from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.enums import ParserResultStatus, SourceChannel, UserRole
from app.models.parser_result import ParserResult
from app.models.user import User
from app.schemas.deal import DealRead
from app.schemas.lead import LeadRead
from app.schemas.parser import (
    ParserIngestRequest,
    ParserResultRead,
    ParserToDealRequest,
    ParserToLeadRequest,
)
from app.services.conversion import parser_result_to_deal, parser_result_to_lead
from app.services.audit import write_audit_log
from app.services.parser_ingest import ingest_parser_item

router = APIRouter(prefix="/parser", tags=["parser"])


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


@router.get("/results", response_model=list[ParserResultRead])
def list_parser_results(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    status_filter: ParserResultStatus | None = Query(default=None, alias="status"),
    source_filter: SourceChannel | None = Query(default=None, alias="source"),
) -> list[ParserResult]:
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(ParserResult.agency_id == current_user.agency_id)
    if status_filter:
        stmt = stmt.where(ParserResult.status == status_filter)
    if source_filter:
        stmt = stmt.where(ParserResult.source_channel == source_filter)
    stmt = stmt.order_by(ParserResult.id.desc())
    return db.execute(stmt).scalars().all()


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
