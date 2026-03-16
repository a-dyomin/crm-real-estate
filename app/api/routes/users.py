from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.core.security import hash_password
from app.db.session import get_db
from app.models.enums import UserRole
from app.models.user import User
from app.schemas.user import UserCreate, UserRead, UserToggleActive
from app.services.audit import write_audit_log

router = APIRouter(prefix="/users", tags=["users"])


@router.get(
    "",
    response_model=list[UserRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.manager))],
)
def list_users(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[User]:
    stmt: Select[tuple[User]] = select(User).where(User.agency_id == current_user.agency_id).order_by(User.id.desc())
    return db.execute(stmt).scalars().all()


@router.post(
    "",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def create_user(payload: UserCreate, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> User:
    target_agency = payload.agency_id or current_user.agency_id
    if target_agency != current_user.agency_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin can create users only in own agency.")

    exists_stmt: Select[tuple[User]] = select(User).where(User.email == payload.email)
    if db.execute(exists_stmt).scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User with this email already exists.")

    user = User(
        agency_id=target_agency,
        email=payload.email,
        full_name=payload.full_name,
        phone=payload.phone,
        role=payload.role,
        password_hash=hash_password(payload.password),
        is_active=True,
    )
    db.add(user)
    db.flush()
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="user.create",
        entity_type="user",
        entity_id=str(user.id),
        message="Admin created user.",
        details={"email": user.email, "role": user.role.value},
    )
    db.commit()
    db.refresh(user)
    return user


@router.patch(
    "/{user_id}/active",
    response_model=UserRead,
    dependencies=[Depends(require_roles(UserRole.admin))],
)
def toggle_user_active(
    user_id: int,
    payload: UserToggleActive,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> User:
    stmt: Select[tuple[User]] = select(User).where(User.id == user_id, User.agency_id == current_user.agency_id)
    user = db.execute(stmt).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")
    if user.id == current_user.id and not payload.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="You cannot deactivate yourself.")
    user.is_active = payload.is_active
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="user.toggle_active",
        entity_type="user",
        entity_id=str(user.id),
        details={"is_active": payload.is_active},
    )
    db.commit()
    db.refresh(user)
    return user

