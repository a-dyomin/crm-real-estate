from fastapi import APIRouter, Depends
from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.models.call_record import CallRecord
from app.db.session import get_db
from app.models.deal import Deal
from app.models.enums import CallStatus, DealStatus, LeadStatus, ParserResultStatus, TranscriptStatus, UserRole
from app.models.lead import Lead
from app.models.parser_result import ParserResult
from app.models.user import User
from app.schemas.dashboard import DashboardSummary

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
PRIMARY_LEAD_STATUSES = [
    LeadStatus.new_lead,
    LeadStatus.qualification,
    LeadStatus.no_answer,
    LeadStatus.call_center_tasks,
    LeadStatus.sent_to_commission,
    LeadStatus.final_no_answer,
    LeadStatus.deferred_demand,
    LeadStatus.poor_quality_lead,
    LeadStatus.high_quality_lead,
]


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

    calls_total = db.scalar(select(func.count(CallRecord.id)).where(CallRecord.agency_id == agency_id)) or 0
    calls_missed = (
        db.scalar(
            select(func.count(CallRecord.id)).where(CallRecord.agency_id == agency_id, CallRecord.status == CallStatus.missed)
        )
        or 0
    )
    calls_transcribed = (
        db.scalar(
            select(func.count(CallRecord.id)).where(
                CallRecord.agency_id == agency_id, CallRecord.transcript_status == TranscriptStatus.completed
            )
        )
        or 0
    )

    leads_total = db.scalar(select(func.count(Lead.id)).where(Lead.agency_id == agency_id)) or 0
    deals_total = db.scalar(select(func.count(Deal.id)).where(Deal.agency_id == agency_id)) or 0

    lead_counts_stmt: Select[tuple[LeadStatus, int]] = (
        select(Lead.status, func.count(Lead.id)).where(Lead.agency_id == agency_id).group_by(Lead.status)
    )
    leads_by_status = {status.value: 0 for status in PRIMARY_LEAD_STATUSES}
    for status, count in db.execute(lead_counts_stmt).all():
        if status in PRIMARY_LEAD_STATUSES:
            leads_by_status[status.value] = count

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
        calls_total=calls_total,
        calls_missed=calls_missed,
        calls_transcribed=calls_transcribed,
        leads_total=leads_total,
        deals_total=deals_total,
        leads_by_status=leads_by_status,
        deals_by_status=deals_by_status,
        conversion_lead_to_deal_percent=conversion,
        pipeline_value_rub=float(pipeline_value),
    )
