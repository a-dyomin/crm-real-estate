from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ContactIdentityRead(BaseModel):
    id: int
    agency_id: int
    key_type: str
    key_value: str
    display_value: str | None
    display_name: str | None
    owner_probability: float
    agent_probability: float
    platform_probability: float
    final_class: str
    confidence: float
    owner_priority: str | None
    owner_priority_score: float | None = None
    explanation: dict[str, Any] | None
    organizations: list[str] | None
    total_listings: int
    active_listings: int
    unique_objects: int
    unique_addresses: int
    source_diversity: int
    repost_rate: float
    region_cluster: str | None
    first_seen_at: datetime | None
    last_seen_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ContactIdentityListingRead(BaseModel):
    id: int
    title: str
    raw_url: str | None
    source_channel: str
    status: str
    updated_at: datetime | None
    normalized_address: str | None
    price_rub: float | None
    area_sqm: float | None
    listing_type: str | None


class ContactIdentityDetailRead(BaseModel):
    identity: ContactIdentityRead
    listings: list[ContactIdentityListingRead]
    objects: list[dict[str, Any]]
    activity_timeline: list[dict[str, Any]]
    explanation: dict[str, Any] | None
