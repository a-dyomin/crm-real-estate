from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.enums import CallDirection, CallStatus, TranscriptStatus


class CallCreateManual(BaseModel):
    provider: str = "manual"
    external_call_id: str | None = None
    direction: CallDirection = CallDirection.inbound
    status: CallStatus = CallStatus.in_progress
    from_number: str | None = None
    to_number: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_sec: int | None = None
    notes: str | None = None


class TelephonyWebhookEvent(BaseModel):
    agency_id: int = Field(default=1, ge=1)
    provider: str = "generic"
    external_call_id: str
    event: str = "call_updated"
    direction: CallDirection = CallDirection.inbound
    status: CallStatus | None = None
    from_number: str | None = None
    to_number: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_sec: int | None = None
    recording_url: str | None = None
    notes: str | None = None
    lead_id: int | None = None
    deal_id: int | None = None
    assigned_user_id: int | None = None


class CallRead(BaseModel):
    id: int
    agency_id: int
    assigned_user_id: int | None
    lead_id: int | None
    deal_id: int | None
    provider: str
    external_call_id: str | None
    direction: CallDirection
    status: CallStatus
    from_number: str | None
    to_number: str | None
    started_at: datetime | None
    ended_at: datetime | None
    duration_sec: int | None
    recording_url: str | None
    transcript_status: TranscriptStatus
    transcript_text: str | None
    transcript_error: str | None
    summary_text: str | None
    extracted_entities: dict[str, Any] | None
    notes: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CallTranscriptionResponse(BaseModel):
    call: CallRead
    message: str

