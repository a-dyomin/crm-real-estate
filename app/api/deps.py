from collections.abc import Callable

from fastapi import Cookie, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.security import decode_access_token
from app.db.session import get_db
from app.models.enums import UserRole
from app.models.user import User

bearer_scheme = HTTPBearer(auto_error=False)


def _extract_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    access_token_cookie: str | None = Cookie(default=None, alias="access_token"),
) -> str:
    if credentials and credentials.scheme.lower() == "bearer":
        return credentials.credentials
    if access_token_cookie:
        return access_token_cookie
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")


def get_current_user(db: Session = Depends(get_db), token: str = Depends(_extract_token)) -> User:
    token_data = decode_access_token(token)
    stmt: Select[tuple[User]] = select(User).where(User.id == token_data.user_id, User.agency_id == token_data.agency_id)
    user = db.execute(stmt).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User from token was not found.")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User is deactivated.")
    return user


def get_agency_id(current_user: User = Depends(get_current_user)) -> int:
    return current_user.agency_id


def require_roles(*roles: UserRole) -> Callable[[User], User]:
    def _checker(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{current_user.role.value}' is not allowed for this operation.",
            )
        return current_user

    return _checker
