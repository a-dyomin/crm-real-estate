from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Header, HTTPException, UploadFile, status
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_roles
from app.core.config import get_settings
from app.db.session import get_db
from app.models.call_record import CallRecord
from app.models.enums import CallStatus, ContactIntent, LeadStatus, SourceChannel, UserRole
from app.models.lead import Lead
from app.models.user import User
from app.schemas.call import CallCreateManual, CallRead, CallTranscriptionResponse, TelephonyWebhookEvent
from app.schemas.lead import LeadRead
from app.services.audit import write_audit_log
from app.services.transcription import transcribe_call

router = APIRouter(prefix="/calls", tags=["calls"])
settings = get_settings()


def _upsert_call_from_webhook(db: Session, payload: TelephonyWebhookEvent) -> CallRecord:
    stmt: Select[tuple[CallRecord]] = select(CallRecord).where(
        CallRecord.agency_id == payload.agency_id,
        CallRecord.external_call_id == payload.external_call_id,
    )
    call = db.execute(stmt).scalar_one_or_none()
    if not call:
        call = CallRecord(
            agency_id=payload.agency_id,
            provider=payload.provider,
            external_call_id=payload.external_call_id,
            direction=payload.direction,
            status=payload.status or CallStatus.ringing,
            started_at=payload.started_at or datetime.utcnow(),
        )
        db.add(call)

    call.provider = payload.provider or call.provider
    call.direction = payload.direction or call.direction
    if payload.status:
        call.status = payload.status
    if payload.from_number:
        call.from_number = payload.from_number
    if payload.to_number:
        call.to_number = payload.to_number
    if payload.started_at:
        call.started_at = payload.started_at
    if payload.ended_at:
        call.ended_at = payload.ended_at
    if payload.duration_sec is not None:
        call.duration_sec = payload.duration_sec
    if payload.recording_url:
        call.recording_url = payload.recording_url
    if payload.notes:
        call.notes = payload.notes
    if payload.lead_id is not None:
        call.lead_id = payload.lead_id
    if payload.deal_id is not None:
        call.deal_id = payload.deal_id
    if payload.assigned_user_id is not None:
        call.assigned_user_id = payload.assigned_user_id
    db.flush()
    return call


@router.post("/webhook", response_model=CallRead)
def telephony_webhook(
    payload: TelephonyWebhookEvent,
    db: Session = Depends(get_db),
    x_telephony_token: str | None = Header(default=None),
) -> CallRecord:
    if settings.telephony_webhook_token and settings.telephony_webhook_token != (x_telephony_token or ""):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid telephony webhook token.")

    call = _upsert_call_from_webhook(db, payload)
    write_audit_log(
        db,
        agency_id=payload.agency_id,
        action="telephony.webhook",
        entity_type="call",
        entity_id=str(call.id),
        details={"event": payload.event, "provider": payload.provider},
    )
    db.commit()
    db.refresh(call)
    return call


@router.get(
    "",
    response_model=list[CallRead],
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager, UserRole.agent))],
)
def list_calls(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> list[CallRecord]:
    stmt: Select[tuple[CallRecord]] = (
        select(CallRecord).where(CallRecord.agency_id == current_user.agency_id).order_by(CallRecord.id.desc())
    )
    return db.execute(stmt).scalars().all()


@router.get(
    "/{call_id}",
    response_model=CallRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager, UserRole.agent))],
)
def get_call(call_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> CallRecord:
    stmt: Select[tuple[CallRecord]] = select(CallRecord).where(
        CallRecord.id == call_id, CallRecord.agency_id == current_user.agency_id
    )
    call = db.execute(stmt).scalar_one_or_none()
    if not call:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Call not found.")
    return call


@router.post(
    "/manual",
    response_model=CallRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.manager))],
)
def create_manual_call(
    payload: CallCreateManual, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> CallRecord:
    call = CallRecord(
        agency_id=current_user.agency_id,
        provider=payload.provider,
        external_call_id=payload.external_call_id,
        direction=payload.direction,
        status=payload.status,
        from_number=payload.from_number,
        to_number=payload.to_number,
        started_at=payload.started_at or datetime.utcnow(),
        ended_at=payload.ended_at,
        duration_sec=payload.duration_sec,
        notes=payload.notes,
    )
    db.add(call)
    db.flush()
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="call.manual_create",
        entity_type="call",
        entity_id=str(call.id),
    )
    db.commit()
    db.refresh(call)
    return call


@router.post(
    "/{call_id}/upload-recording",
    response_model=CallRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.manager))],
)
def upload_recording(
    call_id: int,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CallRecord:
    stmt: Select[tuple[CallRecord]] = select(CallRecord).where(
        CallRecord.id == call_id, CallRecord.agency_id == current_user.agency_id
    )
    call = db.execute(stmt).scalar_one_or_none()
    if not call:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Call not found.")

    safe_name = Path(file.filename or "recording.mp3").name
    relative_dir = Path("recordings") / str(current_user.agency_id)
    target_dir = Path(settings.media_dir) / relative_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_name = f"call_{call.id}_{int(datetime.utcnow().timestamp())}_{safe_name}"
    target_path = target_dir / target_name
    with target_path.open("wb") as out:
        out.write(file.file.read())

    call.recording_local_path = str(target_path.resolve())
    call.recording_url = "/" + str((Path("media") / relative_dir / target_name).as_posix())
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="call.upload_recording",
        entity_type="call",
        entity_id=str(call.id),
    )
    db.commit()
    db.refresh(call)
    return call


@router.post(
    "/{call_id}/transcribe",
    response_model=CallTranscriptionResponse,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.manager))],
)
def transcribe_call_record(
    call_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> CallTranscriptionResponse:
    stmt: Select[tuple[CallRecord]] = select(CallRecord).where(
        CallRecord.id == call_id, CallRecord.agency_id == current_user.agency_id
    )
    call = db.execute(stmt).scalar_one_or_none()
    if not call:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Call not found.")

    call = transcribe_call(db, call)
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="call.transcribe",
        entity_type="call",
        entity_id=str(call.id),
        details={"status": call.transcript_status.value},
    )
    db.commit()
    db.refresh(call)
    message = "Transcription completed." if call.transcript_status.value == "completed" else "Transcription failed."
    return CallTranscriptionResponse(call=CallRead.model_validate(call), message=message)


@router.post(
    "/{call_id}/to-lead",
    response_model=LeadRead,
    dependencies=[Depends(require_roles(UserRole.admin, UserRole.call_center, UserRole.sales, UserRole.manager))],
)
def call_to_lead(call_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)) -> Lead:
    stmt: Select[tuple[CallRecord]] = select(CallRecord).where(
        CallRecord.id == call_id, CallRecord.agency_id == current_user.agency_id
    )
    call = db.execute(stmt).scalar_one_or_none()
    if not call:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Call not found.")

    if call.lead_id:
        lead_stmt: Select[tuple[Lead]] = select(Lead).where(Lead.id == call.lead_id, Lead.agency_id == current_user.agency_id)
        existing = db.execute(lead_stmt).scalar_one_or_none()
        if existing:
            return existing

    lead = Lead(
        agency_id=current_user.agency_id,
        owner_user_id=call.assigned_user_id or current_user.id,
        title=f"Call lead {call.from_number or 'unknown'}",
        contact_name=call.from_number,
        contact_phone=call.from_number,
        intent=ContactIntent.unknown,
        status=LeadStatus.new_lead,
        source_channel=SourceChannel.manual,
        source_record_id=f"call:{call.id}",
        lead_source="Звонок",
    )
    db.add(lead)
    db.flush()
    call.lead_id = lead.id
    write_audit_log(
        db,
        agency_id=current_user.agency_id,
        user=current_user,
        action="call.to_lead",
        entity_type="lead",
        entity_id=str(lead.id),
        details={"call_id": call.id},
    )
    db.commit()
    db.refresh(lead)
    return lead
