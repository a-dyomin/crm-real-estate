from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.enums import UserRole
from app.models.lead import Lead
from app.models.user import User
from app.schemas.lead import LeadCreate, LeadRead, LeadUpdateStatus
from app.services.audit import write_audit_log

router = APIRouter(prefix="/leads", tags=["leads"])


@router.get(
    "",
    response_model=list[LeadRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def list_leads(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[Lead]:
    stmt: Select[tuple[Lead]] = select(Lead).where(Lead.agency_id == current_user.agency_id).order_by(Lead.id.desc())
    return db.execute(stmt).scalars().all()


@router.post(
    "",
    response_model=LeadRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def create_lead(payload: LeadCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Lead:
    lead = Lead(agency_id=current_user.agency_id, **payload.model_dump())
    db.add(lead)
    db.flush()
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="lead.create",
        entity_type="lead",
        entity_id=str(lead.id),
        details={"title": lead.title, "status": lead.status.value},
    )
    db.commit()
    db.refresh(lead)
    return lead


@router.patch(
    "/{lead_id}/status",
    response_model=LeadRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def update_lead_status(
    lead_id: int,
    payload: LeadUpdateStatus,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Lead:
    stmt: Select[tuple[Lead]] = select(Lead).where(Lead.id == lead_id, Lead.agency_id == current_user.agency_id)
    lead = db.execute(stmt).scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found.")
    lead.status = payload.status
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="lead.status_update",
        entity_type="lead",
        entity_id=str(lead.id),
        details={"status": lead.status.value},
    )
    db.commit()
    db.refresh(lead)
    return lead
