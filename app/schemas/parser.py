from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.enums import ContactIntent, ParserResultStatus, ParserRunStatus, SourceChannel


class ParserIngestItem(BaseModel):
    parser_source_id: int | None = None
    source_channel: SourceChannel
    source_external_id: str | None = None
    raw_url: str | None = None
    telegram_post_url: str | None = None
    title: str = Field(min_length=3, max_length=255)
    description: str | None = None
    listing_type: str | None = None
    image_url: str | None = None
    normalized_address: str | None = None
    address_district: str | None = None
    address_street: str | None = None
    city: str | None = None
    region_code: str | None = "RU-UDM"
    latitude: float | None = None
    longitude: float | None = None
    area_sqm: float | None = None
    price_rub: float | None = None
    contact_name: str | None = None
    contact_phone: str | None = None
    contact_email: str | None = None
    contact_candidates: list[dict[str, Any]] | None = None
    selected_contact: dict[str, Any] | None = None
    rejected_contacts: list[dict[str, Any]] | None = None
    contact_rejection_reasons: list[str] | None = None
    contact_confidence: float | None = None
    intent: ContactIntent = ContactIntent.unknown
    payload: dict[str, Any] | None = None


class ParserIngestRequest(BaseModel):
    items: list[ParserIngestItem]


class ParserResultRead(BaseModel):
    id: int
    agency_id: int
    parser_source_id: int | None
    source_channel: SourceChannel
    source_external_id: str | None
    raw_url: str | None
    telegram_post_url: str | None
    title: str
    description: str | None
    listing_type: str | None
    image_url: str | None
    normalized_address: str | None
    address_district: str | None
    address_street: str | None
    city: str | None
    region_code: str | None
    area_sqm: float | None
    price_rub: float | None
    contact_name: str | None
    contact_phone: str | None
    contact_email: str | None
    contact_candidates: list[dict[str, Any]] | None = None
    selected_contact: dict[str, Any] | None = None
    rejected_contacts: list[dict[str, Any]] | None = None
    contact_rejection_reasons: list[str] | None = None
    contact_confidence: float | None = None
    lead_score: float | None = None
    owner_probability_score: float | None = None
    owner_priority_score: float | None = None
    owner_confidence: float | None = None
    owner_explanation_summary: str | None = None
    market_median_price: float | None = None
    market_median_price_per_m2: float | None = None
    deviation_from_market_pct: float | None = None
    below_market_flag: bool | None = None
    intent: ContactIntent
    status: ParserResultStatus
    pipeline_status: str | None = None
    published_at: datetime | None = None
    property_id: int | None = None
    duplicate_of_id: int | None
    fingerprint: str | None
    payload: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ParserResultPageRead(BaseModel):
    items: list[ParserResultRead]
    total: int
    page: int
    page_size: int
    pages: int


class ParserToLeadRequest(BaseModel):
    owner_user_id: int | None = None
    title: str | None = None


class ParserToDealRequest(BaseModel):
    owner_user_id: int | None = None
    title: str | None = None
    value_rub: float | None = None


class ParserSourceCreate(BaseModel):
    name: str = Field(min_length=2, max_length=255)
    source_channel: SourceChannel
    source_url: str = Field(min_length=6, max_length=1024)
    city: str | None = None
    region_code: str = "RU-UDM"
    is_active: bool = True
    poll_minutes: int = Field(default=1440, ge=60, le=10080)
    parse_frequency_minutes: int | None = Field(default=None, ge=60, le=10080)
    parse_priority: int | None = Field(default=None, ge=0, le=100)
    max_items_per_run: int = Field(default=10000, ge=1, le=10000)
    extra_config: dict[str, Any] | None = None


class ParserSourceUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=255)
    source_url: str | None = Field(default=None, min_length=6, max_length=1024)
    city: str | None = None
    region_code: str | None = None
    is_active: bool | None = None
    poll_minutes: int | None = Field(default=None, ge=60, le=10080)
    parse_frequency_minutes: int | None = Field(default=None, ge=60, le=10080)
    parse_priority: int | None = Field(default=None, ge=0, le=100)
    max_items_per_run: int | None = Field(default=None, ge=1, le=10000)
    extra_config: dict[str, Any] | None = None


class ParserSourceRead(BaseModel):
    id: int
    agency_id: int
    name: str
    source_channel: SourceChannel
    source_url: str
    city: str | None
    region_code: str
    is_active: bool
    source_state: str | None = None
    activation_mode: str | None = None
    auto_discovered: bool | None = None
    poll_minutes: int
    parse_frequency_minutes: int | None = None
    parse_priority: int | None = None
    max_items_per_run: int
    extra_config: dict[str, Any] | None
    last_discovery_at: datetime | None
    last_parsed_at: datetime | None
    next_scheduled_parse_at: datetime | None
    last_fetch_at: datetime | None
    last_error_at: datetime | None
    health_status: str | None = None
    failure_count: int | None = None
    consecutive_success_count: int | None = None
    scheduler_lock_key: str | None = None
    auto_activation_reason: str | None = None
    last_run_at: datetime | None
    last_success_at: datetime | None
    last_error: str | None
    listings_parsed_last_run: int | None = None
    contacts_extracted_last_run: int | None = None
    contacts_rejected_last_run: int | None = None
    leads_published_last_run: int | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ParserRunRead(BaseModel):
    id: int
    agency_id: int
    status: ParserRunStatus
    trigger: str
    source_count: int
    fetched_count: int
    inserted_count: int
    duplicate_count: int
    possible_duplicate_count: int
    error_count: int
    objects_resolved: int | None = None
    identities_scored: int | None = None
    owners_published: int | None = None
    leads_auto_created: int | None = None
    call_center_created: int | None = None
    rejected_count: int | None = None
    error_message: str | None
    started_at: datetime
    finished_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


class SourceParseRunRead(BaseModel):
    id: int
    parser_source_id: int
    job_run_id: int | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    fetched_count: int
    listings_parsed: int
    inserted_count: int
    duplicate_count: int
    possible_duplicate_count: int
    contacts_extracted: int
    contacts_rejected: int
    leads_published: int
    error_message: str | None
    created_at: datetime

    model_config = {"from_attributes": True}
