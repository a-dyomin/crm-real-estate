from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import DiscoveryStatus, DiscoveredSourceType, OnboardingPriority


class DiscoveredSource(Base):
    __tablename__ = "discovered_sources"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    domain: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    root_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    source_type: Mapped[DiscoveredSourceType] = mapped_column(Enum(DiscoveredSourceType), nullable=False, index=True)
    discovery_status: Mapped[DiscoveryStatus] = mapped_column(Enum(DiscoveryStatus), nullable=False, index=True)
    relevance_score: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    listing_density_score: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    contact_richness_score: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    update_frequency_score: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    parser_template_id: Mapped[int | None] = mapped_column(ForeignKey("parser_templates.id"), nullable=True, index=True)
    onboarding_priority: Mapped[OnboardingPriority] = mapped_column(Enum(OnboardingPriority), nullable=False, index=True)
    discovery_parent_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovered_sources.id"), nullable=True, index=True
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
