from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import hash_password
from app.models.agency import Agency
from app.models.enums import UserRole
from app.models.user import User


def seed_initial_data(db: Session) -> None:
    settings = get_settings()
    agency_stmt: Select[tuple[Agency]] = select(Agency).where(Agency.id == 1)
    agency = db.execute(agency_stmt).scalar_one_or_none()
    if not agency:
        agency = Agency(id=1, name="Regional CRE Agency", region_code="RU-UDM")
        db.add(agency)
        db.flush()

    admin_stmt: Select[tuple[User]] = select(User).where(User.email == settings.default_admin_email)
    admin = db.execute(admin_stmt).scalar_one_or_none()
    if not admin:
        db.add(
            User(
                agency_id=agency.id,
                email=settings.default_admin_email,
                full_name="System Admin",
                role=UserRole.admin,
                phone="+79000000000",
                password_hash=hash_password(settings.default_admin_password),
                is_active=True,
            )
        )
    else:
        if not admin.password_hash:
            admin.password_hash = hash_password(settings.default_admin_password)
        admin.is_active = True
