from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.deal import Deal
from app.models.enums import UserRole
from app.models.user import User
from app.schemas.deal import DealCreate, DealRead, DealUpdateStatus
from app.services.audit import write_audit_log

router = APIRouter(prefix="/deals", tags=["deals"])


@router.get(
    "",
    response_model=list[DealRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def list_deals(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[Deal]:
    stmt: Select[tuple[Deal]] = select(Deal).where(Deal.agency_id == current_user.agency_id).order_by(Deal.id.desc())
    return db.execute(stmt).scalars().all()


@router.post(
    "",
    response_model=DealRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.sales, UserRole.manager))],
)
def create_deal(payload: DealCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Deal:
    deal = Deal(agency_id=current_user.agency_id, **payload.model_dump())
    db.add(deal)
    db.flush()
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="deal.create",
        entity_type="deal",
        entity_id=str(deal.id),
        details={"title": deal.title, "status": deal.status.value},
    )
    db.commit()
    db.refresh(deal)
    return deal


@router.patch(
    "/{deal_id}/status",
    response_model=DealRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.sales, UserRole.manager))],
)
def update_deal_status(
    deal_id: int,
    payload: DealUpdateStatus,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Deal:
    stmt: Select[tuple[Deal]] = select(Deal).where(Deal.id == deal_id, Deal.agency_id == current_user.agency_id)
    deal = db.execute(stmt).scalar_one_or_none()
    if not deal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deal not found.")
    deal.status = payload.status
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="deal.status_update",
        entity_type="deal",
        entity_id=str(deal.id),
        details={"status": deal.status.value},
    )
    db.commit()
    db.refresh(deal)
    return deal
