from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.enums import ContactIntent, ParserResultStatus, SourceChannel


class ParserIngestItem(BaseModel):
    source_channel: SourceChannel
    source_external_id: str | None = None
    raw_url: str | None = None
    title: str = Field(min_length=3, max_length=255)
    description: str | None = None
    normalized_address: str | None = None
    city: str | None = None
    region_code: str | None = "RU-UDM"
    latitude: float | None = None
    longitude: float | None = None
    area_sqm: float | None = None
    price_rub: float | None = None
    contact_name: str | None = None
    contact_phone: str | None = None
    contact_email: str | None = None
    intent: ContactIntent = ContactIntent.unknown
    payload: dict[str, Any] | None = None


class ParserIngestRequest(BaseModel):
    items: list[ParserIngestItem]


class ParserResultRead(BaseModel):
    id: int
    agency_id: int
    source_channel: SourceChannel
    source_external_id: str | None
    raw_url: str | None
    title: str
    description: str | None
    normalized_address: str | None
    city: str | None
    region_code: str | None
    area_sqm: float | None
    price_rub: float | None
    contact_name: str | None
    contact_phone: str | None
    contact_email: str | None
    intent: ContactIntent
    status: ParserResultStatus
    duplicate_of_id: int | None
    fingerprint: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ParserToLeadRequest(BaseModel):
    owner_user_id: int | None = None
    title: str | None = None


class ParserToDealRequest(BaseModel):
    owner_user_id: int | None = None
    title: str | None = None
    value_rub: float | None = None

