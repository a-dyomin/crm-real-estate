from typing import Any

from sqlalchemy.orm import Session

from app.models.audit_log import AuditLog
from app.models.user import User


def write_audit_log(
    db: Session,
    *,
    agency_id: int,
    action: str,
    entity_type: str,
    entity_id: str | None = None,
    message: str | None = None,
    details: dict[str, Any] | None = None,
    user: User | None = None,
) -> AuditLog:
    log = AuditLog(
        agency_id=agency_id,
        user_id=user.id if user else None,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        message=message,
        details=details,
    )
    db.add(log)
    db.flush()
    return log

