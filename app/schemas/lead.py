from datetime import datetime

from pydantic import BaseModel, Field

from app.models.enums import ContactIntent, LeadStatus, SourceChannel


class LeadCreate(BaseModel):
    title: str = Field(min_length=3, max_length=255)
    property_id: int | None = None
    owner_user_id: int | None = None
    contact_name: str | None = None
    contact_phone: str | None = None
    contact_email: str | None = None
    intent: ContactIntent = ContactIntent.unknown
    status: LeadStatus = LeadStatus.new_lead
    source_channel: SourceChannel = SourceChannel.manual
    source_record_id: str | None = None


class LeadUpdateStatus(BaseModel):
    status: LeadStatus


class LeadRead(BaseModel):
    id: int
    agency_id: int
    property_id: int | None
    owner_user_id: int | None
    title: str
    contact_name: str | None
    contact_phone: str | None
    contact_email: str | None
    intent: ContactIntent
    status: LeadStatus
    source_channel: SourceChannel
    source_record_id: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
