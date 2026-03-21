from datetime import datetime
from typing import Any

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
    lead_source: str | None = None
    need_type: str | None = None
    search_districts: list[str] | None = None
    object_address: str | None = None
    property_type: str | None = None
    area_range: str | None = None
    business_activity: str | None = None
    urgency: str | None = None
    source_details: str | None = None


class LeadUpdateStatus(BaseModel):
    status: LeadStatus


class LeadUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    owner_user_id: int | None = None
    contact_name: str | None = None
    contact_phone: str | None = None
    contact_email: str | None = None
    lead_source: str | None = None
    need_type: str | None = None
    search_districts: list[str] | None = None
    object_address: str | None = None
    property_type: str | None = None
    area_range: str | None = None
    business_activity: str | None = None
    urgency: str | None = None
    source_details: str | None = None


class LeadCommentCreate(BaseModel):
    message: str = Field(min_length=1, max_length=2000)


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
    lead_source: str | None
    need_type: str | None
    search_districts: list[str] | None
    object_address: str | None
    property_type: str | None
    area_range: str | None
    business_activity: str | None
    urgency: str | None
    source_details: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class LeadEventRead(BaseModel):
    id: int
    lead_id: int
    user_id: int | None
    author_name: str | None = None
    event_type: str
    message: str
    payload: dict[str, Any] | None
    created_at: datetime

    model_config = {"from_attributes": True}
