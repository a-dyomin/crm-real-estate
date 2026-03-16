from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import get_settings
from app.core.security import create_access_token, verify_password
from app.db.session import get_db
from app.models.user import User
from app.schemas.auth import LoginRequest, LoginResponse, MeResponse
from app.schemas.user import UserRead
from app.services.audit import write_audit_log

router = APIRouter(prefix="/auth", tags=["auth"])
settings = get_settings()


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, response: Response, db: Session = Depends(get_db)) -> LoginResponse:
    stmt: Select[tuple[User]] = select(User).where(User.email == payload.email)
    user = db.execute(stmt).scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User is deactivated.")

    access_token = create_access_token(user_id=user.id, agency_id=user.agency_id, role=user.role.value)
    expires_at = datetime.utcnow() + timedelta(minutes=settings.access_token_ttl_minutes)
    user.last_login_at = datetime.utcnow()
    write_audit_log(
        db,
        agency_id=user.agency_id,
        user=user,
        action="auth.login",
        entity_type="user",
        entity_id=str(user.id),
        message="User logged in.",
    )
    db.commit()
    db.refresh(user)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        samesite="lax",
        max_age=settings.access_token_ttl_minutes * 60,
    )
    return LoginResponse(access_token=access_token, expires_at=expires_at, user=UserRead.model_validate(user))


@router.post("/logout")
def logout(response: Response, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> dict[str, str]:
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="auth.logout",
        entity_type="user",
        entity_id=str(current_user.id),
        message="User logged out.",
    )
    db.commit()
    response.delete_cookie("access_token")
    return {"status": "ok"}


@router.get("/me", response_model=MeResponse)
def me(current_user: User = Depends(get_current_user)) -> MeResponse:
    return MeResponse(user=UserRead.model_validate(current_user))

