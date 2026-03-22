from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.enums import (
    DiscoveryRunStatus,
    DiscoverySeedType,
    DiscoveryStatus,
    DiscoveredSourceType,
    OnboardingPriority,
    SourceHealthStatus,
)


class SourceSeedCreate(BaseModel):
    seed_type: DiscoverySeedType
    value: str = Field(min_length=2, max_length=1024)
    priority: int = 0
    region: str | None = None
    enabled: bool = True


class SourceSeedUpdate(BaseModel):
    priority: int | None = None
    region: str | None = None
    enabled: bool | None = None


class SourceSeedRead(BaseModel):
    id: int
    seed_type: DiscoverySeedType
    value: str
    priority: int
    region: str | None
    enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class DiscoveredSourceRead(BaseModel):
    id: int
    domain: str
    root_url: str
    source_type: DiscoveredSourceType
    discovery_status: DiscoveryStatus
    relevance_score: float
    listing_density_score: float
    contact_richness_score: float
    update_frequency_score: float
    parser_template_id: int | None
    parser_template_key: str | None = None
    onboarding_priority: OnboardingPriority
    discovery_parent_source_id: int | None
    first_seen_at: datetime
    last_seen_at: datetime
    notes: str | None
    metadata_json: dict[str, Any] | None
    health_status: SourceHealthStatus | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SourceDiscoveryRunRead(BaseModel):
    id: int
    started_at: datetime
    finished_at: datetime | None
    status: DiscoveryRunStatus
    seed_count: int
    candidate_count: int
    matched_count: int
    activated_count: int
    error_count: int
    logs_json: dict[str, Any] | None
    created_at: datetime

    model_config = {"from_attributes": True}
