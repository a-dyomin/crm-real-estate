from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.enums import UserRole
from app.models.property import Property
from app.models.user import User
from app.schemas.property import PropertyCreate, PropertyRead
from app.services.audit import write_audit_log

router = APIRouter(prefix="/properties", tags=["properties"])


@router.get(
    "",
    response_model=list[PropertyRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def list_properties(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[Property]:
    stmt: Select[tuple[Property]] = (
        select(Property).where(Property.agency_id == current_user.agency_id).order_by(Property.id.desc())
    )
    return db.execute(stmt).scalars().all()


@router.post(
    "",
    response_model=PropertyRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def create_property(payload: PropertyCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Property:
    record = Property(agency_id=current_user.agency_id, **payload.model_dump())
    db.add(record)
    db.flush()
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="property.create",
        entity_type="property",
        entity_id=str(record.id),
        details={"title": record.title},
    )
    db.commit()
    db.refresh(record)
    return record


@router.get("/{property_id}", response_model=PropertyRead)
def get_property(property_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Property:
    stmt: Select[tuple[Property]] = select(Property).where(
        Property.id == property_id, Property.agency_id == current_user.agency_id
    )
    record = db.execute(stmt).scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Property not found.")
    return record
