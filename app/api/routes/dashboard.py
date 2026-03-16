from fastapi import APIRouter, Depends
from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.deal import Deal
from app.models.enums import DealStatus, LeadStatus, ParserResultStatus, UserRole
from app.models.lead import Lead
from app.models.parser_result import ParserResult
from app.models.user import User
from app.schemas.dashboard import DashboardSummary

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get(
    "/summary",
    response_model=DashboardSummary,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.manager, UserRole.sales))],
)
def summary(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> DashboardSummary:
    agency_id = current_user.agency_id

    parser_total = db.scalar(select(func.count(ParserResult.id)).where(ParserResult.agency_id == agency_id)) or 0
    parser_new = (
        db.scalar(
            select(func.count(ParserResult.id)).where(
                ParserResult.agency_id == agency_id, ParserResult.status == ParserResultStatus.new
            )
        )
        or 0
    )
    parser_possible_duplicate = (
        db.scalar(
            select(func.count(ParserResult.id)).where(
                ParserResult.agency_id == agency_id, ParserResult.status == ParserResultStatus.possible_duplicate
            )
        )
        or 0
    )
    parser_duplicate = (
        db.scalar(
            select(func.count(ParserResult.id)).where(
                ParserResult.agency_id == agency_id, ParserResult.status == ParserResultStatus.duplicate
            )
        )
        or 0
    )

    leads_total = db.scalar(select(func.count(Lead.id)).where(Lead.agency_id == agency_id)) or 0
    deals_total = db.scalar(select(func.count(Deal.id)).where(Deal.agency_id == agency_id)) or 0

    lead_counts_stmt: Select[tuple[LeadStatus, int]] = (
        select(Lead.status, func.count(Lead.id)).where(Lead.agency_id == agency_id).group_by(Lead.status)
    )
    leads_by_status = {status.value: count for status, count in db.execute(lead_counts_stmt).all()}
    for status in LeadStatus:
        leads_by_status.setdefault(status.value, 0)

    deal_counts_stmt: Select[tuple[DealStatus, int]] = (
        select(Deal.status, func.count(Deal.id)).where(Deal.agency_id == agency_id).group_by(Deal.status)
    )
    deals_by_status = {status.value: count for status, count in db.execute(deal_counts_stmt).all()}
    for status in DealStatus:
        deals_by_status.setdefault(status.value, 0)

    conversion = round((deals_total / leads_total) * 100, 2) if leads_total else 0.0
    pipeline_value = (
        db.scalar(
            select(func.coalesce(func.sum(Deal.value_rub), 0)).where(
                Deal.agency_id == agency_id,
                Deal.status.in_([DealStatus.new, DealStatus.negotiation, DealStatus.due_diligence]),
            )
        )
        or 0
    )

    return DashboardSummary(
        parser_total=parser_total,
        parser_new=parser_new,
        parser_possible_duplicate=parser_possible_duplicate,
        parser_duplicate=parser_duplicate,
        leads_total=leads_total,
        deals_total=deals_total,
        leads_by_status=leads_by_status,
        deals_by_status=deals_by_status,
        conversion_lead_to_deal_percent=conversion,
        pipeline_value_rub=float(pipeline_value),
    )

