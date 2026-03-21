from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import Select, desc, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.db.session import get_db
from app.models.enums import UserRole
from app.models.lead import Lead
from app.models.lead_event import LeadEvent
from app.models.user import User
from app.schemas.lead import LeadCommentCreate, LeadCreate, LeadEventRead, LeadRead, LeadUpdate, LeadUpdateStatus
from app.services.audit import write_audit_log

router = APIRouter(prefix="/leads", tags=["leads"])


LEAD_STATUS_LABELS = {
    "new_lead": "Новый лид",
    "qualification": "Квалификация",
    "no_answer": "Недозвон",
    "call_center_tasks": "Задачи КЦ",
    "sent_to_commission": "Отправлен на комиссию",
    "final_no_answer": "Конечный недозвон",
    "deferred_demand": "Отложенный спрос",
    "poor_quality_lead": "Некачественный лид",
    "high_quality_lead": "Качественный лид",
}


def _lead_event(
    db: Session,
    *,
    lead: Lead,
    user: User | None,
    event_type: str,
    message: str,
    payload: dict | None = None,
) -> None:
    db.add(
        LeadEvent(
            agency_id=lead.agency_id,
            lead_id=lead.id,
            user_id=user.id if user else None,
            event_type=event_type,
            message=message,
            payload=payload,
            created_at=datetime.utcnow(),
        )
    )


def _user_label(db: Session, user_id: int | None) -> str:
    if not user_id:
        return "Не назначен"
    stmt: Select[tuple[User]] = select(User).where(User.id == user_id)
    user = db.execute(stmt).scalar_one_or_none()
    return user.full_name if user else f"ID {user_id}"


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    return False


def _stringify(value: object) -> str:
    if _is_empty(value):
        return "не заполнено"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if str(item).strip()) or "не заполнено"
    return str(value)


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
    _lead_event(
        db,
        lead=lead,
        user=current_user,
        event_type="created",
        message=f"Лид создан: {lead.title}",
    )
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


@router.get(
    "/{lead_id}",
    response_model=LeadRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def get_lead(
    lead_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> Lead:
    stmt: Select[tuple[Lead]] = select(Lead).where(Lead.id == lead_id, Lead.agency_id == current_user.agency_id)
    lead = db.execute(stmt).scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found.")
    return lead


@router.patch(
    "/{lead_id}",
    response_model=LeadRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def update_lead(
    lead_id: int,
    payload: LeadUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Lead:
    stmt: Select[tuple[Lead]] = select(Lead).where(Lead.id == lead_id, Lead.agency_id == current_user.agency_id)
    lead = db.execute(stmt).scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found.")

    before = {
        "title": lead.title,
        "contact_phone": lead.contact_phone,
        "lead_source": lead.lead_source,
        "owner_user_id": lead.owner_user_id,
    }
    updates = payload.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(lead, field, value)

    # Track significant changes.
    after = {
        "title": lead.title,
        "contact_phone": lead.contact_phone,
        "lead_source": lead.lead_source,
        "owner_user_id": lead.owner_user_id,
    }
    for field, old_value in before.items():
        new_value = after[field]
        if old_value == new_value:
            continue
        if field == "owner_user_id":
            old_label = _user_label(db, old_value)
            new_label = _user_label(db, new_value)
            _lead_event(
                db,
                lead=lead,
                user=current_user,
                event_type="owner_changed",
                message=f"Изменен ответственный: с «{old_label}» на «{new_label}».",
                payload={"field": field, "before": old_value, "after": new_value},
            )
            continue

        display_name = {
            "title": "Название лида",
            "contact_phone": "Номер телефона",
            "lead_source": "Источник",
        }.get(field, field)
        if _is_empty(old_value) and not _is_empty(new_value):
            message = f"Заполнено поле «{display_name}»: «{_stringify(new_value)}»."
        elif not _is_empty(old_value) and _is_empty(new_value):
            message = f"Очищено поле «{display_name}»: было «{_stringify(old_value)}»."
        else:
            message = (
                f"Изменено поле «{display_name}»: с «{_stringify(old_value)}» "
                f"на «{_stringify(new_value)}»."
            )
        _lead_event(
            db,
            lead=lead,
            user=current_user,
            event_type="field_changed",
            message=message,
            payload={"field": field, "before": old_value, "after": new_value},
        )

    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="lead.update",
        entity_type="lead",
        entity_id=str(lead.id),
        details={"fields": list(updates.keys())},
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
    old_status = lead.status
    lead.status = payload.status
    if old_status != lead.status:
        _lead_event(
            db,
            lead=lead,
            user=current_user,
            event_type="status_changed",
            message=(
                f"Этап изменен: с «{LEAD_STATUS_LABELS.get(old_status.value, old_status.value)}» "
                f"на «{LEAD_STATUS_LABELS.get(lead.status.value, lead.status.value)}»."
            ),
            payload={"before": old_status.value, "after": lead.status.value},
        )
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


@router.get(
    "/{lead_id}/events",
    response_model=list[LeadEventRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def list_lead_events(
    lead_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[LeadEventRead]:
    stmt: Select[tuple[LeadEvent, User | None]] = (
        select(LeadEvent, User)
        .outerjoin(User, User.id == LeadEvent.user_id)
        .where(LeadEvent.lead_id == lead_id, LeadEvent.agency_id == current_user.agency_id)
        .order_by(desc(LeadEvent.created_at))
    )
    rows = db.execute(stmt).all()
    return [
        LeadEventRead(
            id=event.id,
            lead_id=event.lead_id,
            user_id=event.user_id,
            author_name=user.full_name if user else None,
            event_type=event.event_type,
            message=event.message,
            payload=event.payload,
            created_at=event.created_at,
        )
        for event, user in rows
    ]


@router.post(
    "/{lead_id}/comments",
    response_model=LeadEventRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.agent, UserRole.manager))],
)
def add_lead_comment(
    lead_id: int,
    payload: LeadCommentCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> LeadEventRead:
    stmt: Select[tuple[Lead]] = select(Lead).where(Lead.id == lead_id, Lead.agency_id == current_user.agency_id)
    lead = db.execute(stmt).scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lead not found.")

    _lead_event(
        db,
        lead=lead,
        user=current_user,
        event_type="comment",
        message=payload.message.strip(),
    )
    db.commit()
    event_stmt: Select[tuple[LeadEvent]] = (
        select(LeadEvent)
        .where(LeadEvent.lead_id == lead.id, LeadEvent.agency_id == lead.agency_id)
        .order_by(desc(LeadEvent.created_at))
        .limit(1)
    )
    event = db.execute(event_stmt).scalar_one()
    return LeadEventRead(
        id=event.id,
        lead_id=event.lead_id,
        user_id=event.user_id,
        author_name=current_user.full_name,
        event_type=event.event_type,
        message=event.message,
        payload=event.payload,
        created_at=event.created_at,
    )
